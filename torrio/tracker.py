import time
import struct
import random
import asyncio
import ipaddress

from http import HTTPStatus
from operator import itemgetter
from abc import ABC, abstractmethod
from functools import cached_property
from typing import Optional, Callable, Type
from urllib.parse import urlparse, urlencode

import aiohttp

from torrio.peer import Peer
from torrio.torrent import Torrent
from torrio.client_info import ClientInfo
from torrio.bencode import loads as bloads
from torrio.download_state import DownloadState
from torrio.utils import parse_ipv4_peer_string, parse_ipv6_peer_string


__all__ = (
    "Tracker",
    "TrackerConnectionError",
    "get_tracker_class",
)


class TrackerResponse(ABC):
    def __init__(self, raw_payload: bytes):
        self._raw_payload = raw_payload

    @property
    @abstractmethod
    def peers(self) -> list[Peer]: ...
    @property
    @abstractmethod
    def interval(self) -> int: ...
    @property
    @abstractmethod
    def failure_reason(self) -> Optional[str]: ...


class HTTPTrackerResponse(TrackerResponse):
    @property
    def failure_reason(self) -> Optional[str]:
        return self._payload.get(b"failure reason")

    @cached_property
    def _payload(self) -> dict:
        return bloads(self._raw_payload)

    def _get_peers_from_list(self, peer_key: bytes) -> list[Peer]:
        return [Peer(ipaddress.ip_address(raw_ip), port)
                for raw_ip, port in map(itemgetter(b"ip", b"port"),
                                        self._payload[peer_key])]

    @property
    def _peers(self) -> list[Peer]:
        if b"peers" not in self._payload:
            return []
        if isinstance(self._payload[b"peers"], list):
            return self._get_peers_from_list(b"peers")
        return parse_ipv4_peer_string(self._payload[b"peers"])

    @property
    def _peers6(self) -> list[Peer]:
        if b"peers6" not in self._payload:
            return []
        if isinstance(self._payload[b"peers6"], list):
            return self._get_peers_from_list(b"peers6")
        return parse_ipv6_peer_string(self._payload[b"peers6"])

    @property
    def peers(self) -> list[Peer]:
        return self._peers + self._peers6

    @property
    def interval(self) -> int:
        return self._payload.get(b"interval", 0)


class UDPTrackerResponse(TrackerResponse):
    def __init__(self, raw_payload: bytes, is_ipv6_peer_string: bool = False):
        super().__init__(raw_payload)
        self._is_ipv6_peer_string = is_ipv6_peer_string

    @property
    def peers(self) -> list[Peer]:
        if self._is_ipv6_peer_string:
            return parse_ipv6_peer_string(self._raw_payload[20:])
        return parse_ipv4_peer_string(self._raw_payload[20:])

    @property
    def interval(self) -> int:
        return struct.unpack(">I", self._raw_payload[8: 12])[0]

    @property
    def failure_reason(self) -> Optional[str]:
        return None


class Tracker(ABC):
    def __init__(
        self,
        announce: str,
        torrent: Torrent,
        download_state: DownloadState,
        client_info: ClientInfo
    ) -> None:
        self._announce = announce
        self._torrent = torrent
        self._download_state = download_state
        self._client_info = client_info

    @classmethod
    @abstractmethod
    def is_compatable_to_tracker_type(cls, announce: str) -> bool: ...
    @abstractmethod
    async def ask(self) -> TrackerResponse: ...
    @abstractmethod
    async def close(self) -> None: ...


class TrackerConnectionError(Exception):
    ...


def get_tracker_class(announce: str) -> Type[Tracker]:
    for tracker_cls in Tracker.__subclasses__():
        if tracker_cls.is_compatable_to_tracker_type(announce):
            return tracker_cls
    raise ValueError(f"Invalid announce: {announce}")


class HTTPTracker(Tracker):
    REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=10)

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._http_session = aiohttp.ClientSession()

    @classmethod
    def is_compatable_to_tracker_type(cls, announce: str) -> bool:
        return urlparse(announce).scheme == "http"

    async def ask(self) -> HTTPTrackerResponse:
        try:
            async with self._http_session.get(
                self._request_url,
                timeout=self.REQUEST_TIMEOUT
            ) as http_resp:
                if http_resp.status == HTTPStatus.OK:
                    return HTTPTrackerResponse(await http_resp.read())
                raise TrackerConnectionError("Invalid response from tracker")
        except (asyncio.TimeoutError, aiohttp.ClientConnectionError):
            raise TrackerConnectionError("Can't connect to tracker")

    async def close(self) -> None:
        await self._http_session.close()

    @property
    def _request_url(self) -> str:
        params = {
            "info_hash": self._torrent.info_hash,
            "peer_id": self._client_info.peer_id,
            "port": self._client_info.port,
            "uploaded": self._download_state.uploaded,
            "downloaded": self._download_state.downloaded,
            "left": self._download_state.left,
            "compact": self._client_info.compact,
            "no_peer_id": self._client_info.no_peer_id,
            "event": self._download_state.event,
        }  # TODO: Add ip, numwant, key, trackerid
        return f"{self._announce}?{urlencode(params)}"


class UDPTrackerProtocol(asyncio.DatagramProtocol):
    def __init__(self) -> None:
        self._transport: asyncio.DatagramTransport \
            = None  # type: ignore[assignment]
        self._exception: Optional[Exception] = None
        self._read_queue: asyncio.Queue[tuple[bytes, tuple[str, int]]] \
            = asyncio.Queue()

        self.last_readed_packet_addr: tuple[str, int] \
            = None  # type: ignore[assignment]

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self._transport = transport  # type: ignore[assignment]

    def datagram_received(
        self,
        tracker_response: bytes,
        addr: tuple[str, int]
    ) -> None:
        self._read_queue.put_nowait((tracker_response, addr))

    def error_received(self, exception: Exception) -> None:
        self._exception = exception

    def connection_lost(self, exception: Optional[Exception]) -> None:
        if exception is None:
            if self._transport.is_closing():
                exception = ConnectionError("Connection was aborted or closed")
            else:
                exception = ConnectionError("EOF is received")
        self._exception = exception

    def write(self, request: bytes) -> None:
        self._transport.sendto(bytes(request))

    async def read(self) -> bytes:
        if self._exception is not None:
            raise self._exception
        response, self.last_readed_packet_addr = await self._read_queue.get()
        return response


class UDPTracker(Tracker):
    _connect_action = 0
    _announce_action = 1

    _max_attempts_count = 8

    _udp_events = {
        "none": 0,
        "completed": 1,
        "started": 2,
        "stopped": 3
    }

    _magic_constant = 0x41727101980

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._transport: asyncio.DatagramTransport \
            = None  # type: ignore[assignment]
        self._protocol: UDPTrackerProtocol = None  # type: ignore[assignment]
        self._last_transaction_id: int = None  # type: ignore[assignment]

    @classmethod
    def is_compatable_to_tracker_type(cls, announce: str) -> bool:
        return urlparse(announce).scheme == "udp"

    async def ask(self) -> UDPTrackerResponse:
        if self._transport is None or self._protocol is None:
            await self._init_transport_and_protocol()

        attempts_count = 0

        connection_id = None
        last_connection_time = None

        while attempts_count <= self._max_attempts_count:
            timeout = 15 * 2 ** attempts_count
            if connection_id is None:
                self._send_connect_request()
                try:
                    connect_response = await asyncio.wait_for(
                        self._wait_for_expected_and_valid_connect_response(),
                        timeout=timeout
                    )
                except asyncio.TimeoutError:
                    attempts_count += 1
                    continue
                last_connection_time = time.time()
                connection_id = self._get_connection_id(connect_response)

            self._send_announce_request(connection_id)
            try:
                announce_response = await asyncio.wait_for(
                    self._wait_for_expected_and_valid_announce_response(),
                    timeout=timeout
                )
            except asyncio.TimeoutError:
                attempts_count += 1
                if self._is_connection_expired(last_connection_time):
                    connection_id = None
                continue
            return UDPTrackerResponse(announce_response,
                                      self._is_ipv6_peer_string)
        raise TrackerConnectionError("Can't connect to tracker")

    async def close(self) -> None:
        if self._transport is not None:
            self._transport.close()

    async def _init_transport_and_protocol(self):
        loop = asyncio.get_running_loop()
        self._transport, self._protocol = await loop.create_datagram_endpoint(
            UDPTrackerProtocol, remote_addr=self._remote_addr)

    def _send_connect_request(self) -> None:
        payload = struct.pack(
            ">QII",
            self._magic_constant,
            self._connect_action,
            self._gen_transaction_id()
        )
        self._protocol.write(payload)

    async def _wait_for_expected_and_valid_connect_response(self) -> bytes:
        return await self._wait_for_expected_and_valid_response(
            self._is_expected_and_valid_connect_response)

    def _is_expected_and_valid_connect_response(self, response: bytes) -> bool:
        (transaction_id, ) = struct.unpack(">I", response[4: 8])
        (action, ) = struct.unpack(">I", response[:4])
        return all([
            len(response) >= 16,
            self._last_transaction_id == transaction_id,
            self._connect_action == action
        ])

    def _send_announce_request(self, connection_id: int) -> None:
        payload = struct.pack(
            ">QII20s20sQQQIIIiH",
            connection_id,
            self._announce_action,
            self._gen_transaction_id(),
            self._torrent.info_hash,
            self._client_info.peer_id,
            self._download_state.downloaded,
            self._download_state.left,
            self._download_state.uploaded,
            self._udp_events[self._download_state.event],
            self._client_info.ip_address,
            self._client_info.key,
            self._client_info.num_want,
            self._client_info.port
        )
        self._protocol.write(payload)

    async def _wait_for_expected_and_valid_announce_response(self) -> bytes:
        return await self._wait_for_expected_and_valid_response(
            self._is_expected_and_valid_announce_response)

    def _is_expected_and_valid_announce_response(
        self,
        response: bytes
    ) -> bool:
        (action, ) = struct.unpack(">I", response[0: 4])
        (transaction_id, ) = struct.unpack(">I", response[4: 8])
        return all([
            len(response) >= 20,
            self._last_transaction_id == transaction_id,
            action == self._announce_action
        ])

    async def _wait_for_expected_and_valid_response(
        self,
        is_expected_and_valid_response: Callable[[bytes], bool]
    ) -> bytes:
        while True:
            response = await self._read_tracker_response()
            if is_expected_and_valid_response(response):
                return response

    async def _read_tracker_response(self) -> bytes:
        return await self._protocol.read()

    @staticmethod
    def _is_connection_expired(last_connection_time: float) -> bool:
        return time.time() - last_connection_time > 60

    @staticmethod
    def _get_connection_id(response: bytes) -> int:
        return struct.unpack(">Q", response[8: 16])[0]

    def _gen_transaction_id(self) -> int:
        self._last_transaction_id = random.randint(0, 0xffffffff)
        return self._last_transaction_id

    @cached_property
    def _remote_addr(self) -> tuple[Optional[str], Optional[int]]:
        parsed_announce = urlparse(self._announce)
        return parsed_announce.hostname, parsed_announce.port

    @property
    def _is_ipv6_peer_string(self) -> bool:
        ip = ipaddress.ip_address(self._protocol.last_readed_packet_addr[0])
        return isinstance(ip, ipaddress.IPv6Address)
