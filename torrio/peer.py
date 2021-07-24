import struct
import asyncio

from functools import cache
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Type, NoReturn

from torrio.download_state import DownloadState

from bitarray import bitarray


class PeerMessage(ABC):
    @classmethod
    @abstractmethod
    def decode(cls, raw_msg: bytes) -> "PeerMessage": ...
    @abstractmethod
    def __bytes__(self) -> bytes: ...


class HandshakeMessage(PeerMessage):
    def __init__(self, info_hash: bytes, peer_id: bytes) -> None:
        self.info_hash = info_hash
        self.peer_id = peer_id

    @classmethod
    def decode(cls, raw_msg: bytes) -> "HandshakeMessage":
        info_hash, peer_id = struct.unpack(">20s20s", raw_msg[28:])
        return cls(info_hash, peer_id)

    def __bytes__(self) -> bytes:
        return struct.pack(">B19sQ20s20s", 19, b"BitTorrent protocol", 0,
                           self.info_hash, self.peer_id)


class KeepAliveMessage(PeerMessage):
    _msg_length = 0

    @classmethod
    def decode(cls, raw_msg: bytes) -> "KeepAliveMessage":
        return cls()

    def __bytes__(self) -> bytes:
        return struct.pack(">I", self._msg_length)


class ChokeMessage(PeerMessage):
    _msg_id = 0
    _msg_length = 1

    @classmethod
    def decode(cls, raw_msg: bytes) -> "ChokeMessage":
        return cls()

    def __bytes__(self) -> bytes:
        return struct.pack(">IB", self._msg_length, self._msg_id)


class UnchokeMessage(PeerMessage):
    _msg_id = 1
    _msg_length = 1

    @classmethod
    def decode(cls, raw_msg: bytes) -> "UnchokeMessage":
        return cls()

    def __bytes__(self) -> bytes:
        return struct.pack(">IB", self._msg_length, self._msg_id)


class InterestedMessage(PeerMessage):
    _msg_id = 2
    _msg_length = 1

    @classmethod
    def decode(cls, raw_msg: bytes) -> "InterestedMessage":
        return cls()

    def __bytes__(self) -> bytes:
        return struct.pack(">IB", self._msg_length, self._msg_id)


class NotInterestedMessage(PeerMessage):
    _msg_id = 3
    _msg_length = 1

    @classmethod
    def decode(cls, raw_msg: bytes) -> "NotInterestedMessage":
        return cls()

    def __bytes__(self) -> bytes:
        return struct.pack(">IB", self._msg_length, self._msg_id)


class HaveMessage(PeerMessage):
    _msg_id = 4
    _msg_length = 5

    def __init__(self, piece_idx: int) -> None:
        self.piece_idx = piece_idx

    @classmethod
    def decode(cls, raw_msg: bytes) -> "HaveMessage":
        piece_idx = struct.unpack(">I", raw_msg[5:])[0]
        return cls(piece_idx)

    def __bytes__(self) -> bytes:
        return struct.pack(
            ">IBI",
            self._msg_length,
            self._msg_id,
            self.piece_idx
        )


class BitfieldMessage(PeerMessage):
    _msg_id = 5

    def __init__(self, bitfield: bitarray) -> None:
        self.bitfield = bitfield

    @classmethod
    def decode(cls, raw_msg: bytes) -> "BitfieldMessage":
        msg_length = struct.unpack(">I", raw_msg[:4])[0]
        bitfield_length = msg_length - 1
        raw_bitfield = raw_msg[5: 5 + bitfield_length]
        parsed_bitfield = bitarray(endian="big")
        parsed_bitfield.frombytes(raw_bitfield)
        return cls(parsed_bitfield)

    def __bytes__(self) -> bytes:
        raw_bitfield = self.bitfield.tobytes()
        bitfield_length = len(raw_bitfield)
        msg_length = 1 + bitfield_length
        return struct.pack(
            f">IB{bitfield_length}s",
            msg_length,
            self._msg_id,
            raw_bitfield
        )


class RequestMessage(PeerMessage):
    _msg_id = 6
    _msg_length = 13

    def __init__(self, index: int, begin: int, length: int) -> None:
        self.index = index
        self.begin = begin
        self.length = length

    @classmethod
    def decode(cls, raw_msg: bytes) -> "RequestMessage":
        index, begin, length = struct.unpack(">III", raw_msg[5:])
        return cls(index, begin, length)

    def __bytes__(self) -> bytes:
        return struct.pack(
            ">IBIII",
            self._msg_length,
            self._msg_id,
            self.index,
            self.begin,
            self.length
        )


class PieceMessage(PeerMessage):
    _msg_id = 7

    def __init__(self, index: int, begin: int, block: bytes) -> None:
        self.index = index
        self.begin = begin
        self.block = block

    @classmethod
    def decode(cls, raw_msg: bytes) -> "PieceMessage":
        length = struct.unpack(">I", raw_msg[:4])[0]
        block_length = length - 9
        index, begin, block = struct.unpack(
            f">II{block_length}s",
            raw_msg[5:]
        )
        return cls(index, begin, block)

    def __bytes__(self) -> bytes:
        return struct.pack(
            f">IBII{len(self.block)}s",
            9 + len(self.block),
            self._msg_id,
            self.index,
            self.begin,
            self.block
        )


class CancelMessage(PeerMessage):
    _msg_id = 8
    _msg_length = 13

    def __init__(self, index: int, begin: int, length: int) -> None:
        self.index = index
        self.begin = begin
        self.length = length

    @classmethod
    def decode(cls, raw_msg: bytes) -> "CancelMessage":
        index, begin, length = struct.unpack(">III", raw_msg[5:])
        return cls(index, begin, length)

    def __bytes__(self) -> bytes:
        return struct.pack(
            ">IBIII",
            self._msg_length,
            self._msg_id,
            self.index,
            self.begin,
            self.length
        )


class PeerCommunicationError(Exception):
    ...


class Peer:
    _MIN_TIME_BETWEEN_SENDING = 0.001

    def __new__(cls, ip: str, port: int) -> "Peer":
        return cls.__get_or_create_peer(ip, port)

    @classmethod
    @cache
    def __get_or_create_peer(cls, ip: str, port: int) -> "Peer":
        return super().__new__(cls)

    def __init__(self, ip: str, port: int) -> None:
        self.ip = ip
        self.port = port

        self._am_choking = True
        self._am_interested = False
        self._peer_choking = True
        self._peer_interested = False

    async def start_communicating(
        self,
        info_hash: bytes,
        peer_id: bytes,
        download_state: DownloadState,
    ) -> None:
        async with open_peer_stream(self) as stream:
            await stream.send_msg(HandshakeMessage(info_hash, peer_id))
            resp_handshake_msg = await stream.read_handshake()
            if resp_handshake_msg.info_hash != info_hash:
                raise PeerCommunicationError("Invalid info_hash")
            sending_task = asyncio.create_task(
                self._start_sending(stream, download_state))
            receiving_task = asyncio.create_task(
                self._start_receiving(stream, download_state))
            try:
                await asyncio.gather(sending_task, receiving_task)
            finally:
                if not sending_task.done():
                    sending_task.cancel()
                if not receiving_task.done():
                    receiving_task.cancel()

    async def _start_sending(
        self,
        stream: "PeerStream",
        state: DownloadState
    ) -> NoReturn:
        while True:
            if (piece_idx := state.get_new_piece_to_notify_peer(self)):
                await stream.send_msg(HaveMessage(piece_idx))
            elif self._am_interested:
                if not state.peer_have_blocks_to_download(self):
                    await stream.send_msg(NotInterestedMessage())
                    self._am_interested = False
                else:
                    if not self._peer_choking:
                        if block := state.get_next_block_to_download(self):
                            await stream.send_msg(
                                RequestMessage(
                                    block.piece_idx,
                                    block.offset,
                                    block.length
                                )
                            )
            elif not self._am_interested:
                if state.peer_have_blocks_to_download(self):
                    await stream.send_msg(InterestedMessage())
                    self._am_interested = True
            await asyncio.sleep(self._MIN_TIME_BETWEEN_SENDING)

    async def _start_receiving(
        self,
        stream: "PeerStream",
        download_state: DownloadState,
    ) -> None:
        async for msg in stream:
            if isinstance(msg, KeepAliveMessage):
                pass
            elif isinstance(msg, ChokeMessage):
                self._peer_choking = True
            elif isinstance(msg, UnchokeMessage):
                self._peer_choking = False
            elif isinstance(msg, InterestedMessage):
                self._peer_interested = True
            elif isinstance(msg, NotInterestedMessage):
                self._peer_interested = False
            elif isinstance(msg, HaveMessage):
                download_state.notify_peer_have_piece(self, msg.piece_idx)
            elif isinstance(msg, BitfieldMessage):
                piece_flags = \
                    map(bool, msg.bitfield[:download_state.pieces_count])
                for piece_idx, peer_have_piece in enumerate(piece_flags):
                    if peer_have_piece:
                        download_state.notify_peer_have_piece(
                            self, piece_idx)
            elif isinstance(msg, RequestMessage):
                pass
            elif isinstance(msg, PieceMessage):
                download_state.notify_peer_send_block(
                    msg.index, msg.begin, msg.block)
            elif isinstance(msg, CancelMessage):
                pass

    def __hash__(self) -> int:
        return hash((self.ip, self.port))


class PeerStreamIterator:
    _msg_id_to_msg_cls_map: dict[int, Type[PeerMessage]] = {
        0: ChokeMessage,
        1: UnchokeMessage,
        2: InterestedMessage,
        3: NotInterestedMessage,
        4: HaveMessage,
        5: BitfieldMessage,
        6: RequestMessage,
        7: PieceMessage,
        8: CancelMessage
    }

    def __init__(self, reader: asyncio.StreamReader) -> None:
        self._reader = reader

    def __aiter__(self) -> "PeerStreamIterator":
        return self

    async def __anext__(self) -> PeerMessage:
        length_prefix = await self._reader.readexactly(4)
        msg_length = struct.unpack(">I", length_prefix)[0]
        if msg_length == 0:
            return KeepAliveMessage.decode(length_prefix)
        msg_payload = await self._reader.readexactly(msg_length)
        msg_id = struct.unpack(">B", msg_payload[:1])[0]
        msg_cls = self._msg_id_to_msg_cls_map[msg_id]
        return msg_cls.decode(b"".join([length_prefix, msg_payload]))


class PeerStream:
    MAX_TIME_FOR_CONNECTION = 30

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter
    ) -> None:
        self._reader = reader
        self._writer = writer

    async def send_msg(self, msg: PeerMessage) -> None:
        self._writer.write(bytes(msg))
        await self._writer.drain()

    async def read_handshake(self) -> HandshakeMessage:
        return HandshakeMessage.decode(
            await self._reader.readexactly(struct.calcsize(">B19sQ20s20s")))

    def __aiter__(self) -> PeerStreamIterator:
        return PeerStreamIterator(self._reader)


@asynccontextmanager
async def open_peer_stream(peer: Peer) -> AsyncGenerator[PeerStream, None]:
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(peer.ip, peer.port),
            timeout=PeerStream.MAX_TIME_FOR_CONNECTION
        )
    except (ConnectionRefusedError, OSError, asyncio.TimeoutError):
        raise PeerCommunicationError("Can't connect to peer")

    try:
        yield PeerStream(reader, writer)
    except (asyncio.IncompleteReadError, ConnectionResetError):
        raise PeerCommunicationError("Error while communicating with peer")
    finally:
        if not writer.is_closing():
            writer.close()
            await writer.wait_closed()
