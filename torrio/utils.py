import struct
import asyncio

from urllib.parse import urlparse
from ipaddress import IPv4Address, IPv6Address

from torrio.peer import Peer


def is_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
        return all([parsed.scheme, parsed.netloc, parsed.path])
    except ValueError:
        return False


def parse_ipv4_peer_string(peer_str: bytes) -> list[Peer]:
    raw_peers = [peer_str[ptr: ptr + 6]
                 for ptr in range(0, len(peer_str), 6)]
    return [Peer(str(IPv4Address(raw_peer[:4])),
                 struct.unpack(">H", raw_peer[4:])[0])
            for raw_peer in raw_peers]


def parse_ipv6_peer_string(peer_str: bytes) -> list[Peer]:
    raw_peers = [peer_str[ptr: ptr + 18]
                 for ptr in range(0, len(peer_str), 18)]
    return [Peer(str(IPv6Address(raw_peer[:16])),
                 struct.unpack(">H", raw_peer[16:])[0])
            for raw_peer in raw_peers]


async def wait_cancelled(task: asyncio.Task) -> None:
    try:
        await task
    except asyncio.CancelledError:
        pass
