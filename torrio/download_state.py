import time
import bisect
import asyncio

from hashlib import sha1
from random import choice
from collections import deque
from dataclasses import dataclass
from functools import cached_property
from typing import Optional, Iterable, TYPE_CHECKING

if TYPE_CHECKING:
    from torrio.peer import Peer
    from torrio.torrent import Torrent
    from torrio.saver import Saver


@dataclass
class Block:
    piece_idx: int
    offset: int
    length: int

    @classmethod
    def from_raw(
        cls,
        piece_idx: int,
        offset: int,
        block_data: bytes
    ) -> "Block":
        return cls(piece_idx, offset, len(block_data))

    def __hash__(self) -> int:
        return hash((self.piece_idx, self.offset, self.length))


class DownloadEvents:
    NONE = "none"
    COMPLETED = "completed"
    STARTED = "started"
    STOPPED = "stopped"


class DownloadState:

    _MAX_BLOCK_LENGTH = 2 ** 14
    _TIME_FOR_BLOCK_DOWNLOAD = 10

    # TODO: Make addicted from max available RAM and torrent size
    _MAX_PIECES_TO_DOWNLOAD_TOGETHER = 150

    def __init__(self, torrent: "Torrent", saver: "Saver") -> None:
        self._torrent = torrent
        self._saver = saver

        self._peers_registry: set["Peer"] = set()

        self._blocks_to_download: list[set[Block]] = self._init_blocks()
        self._pieces_to_download: set[int] = self._init_pieces_to_download()
        self._downloading_pieces: set[int] = set()
        self._downloading_blocks_to_download_start_times: dict[Block, float] \
            = {}
        self._pieces_to_downloading_blocks: dict[int, set[Block]] = {}
        self._pieces_downloaded_blocks: dict[int, list[tuple[int, bytes]]] = {}
        self._start_downloading_first_pieces()

        self._pieces_map: dict["Peer", list[bool]] = {}

        self._new_pieces_to_notify_peer: dict["Peer", deque[int]] = {}

        self._full_download_event = asyncio.Event()

        self.uploaded = 0
        self.downloaded = 0
        self.left = torrent.size
        self.event = DownloadEvents.STARTED

    def is_peer_registered(self, peer: "Peer") -> bool:
        return peer in self._peers_registry

    def register_peer(self, peer: "Peer") -> None:
        self._peers_registry.add(peer)
        self._pieces_map[peer] = self._init_peer_pieces_map()
        self._new_pieces_to_notify_peer[peer] = deque()

    def unregister_peer(self, peer: "Peer") -> None:
        self._peers_registry.remove(peer)
        self._pieces_map.pop(peer)
        self._new_pieces_to_notify_peer.pop(peer)

    @cached_property
    def pieces_count(self) -> int:
        return len(self._torrent.info.pieces)

    def peer_have_blocks_to_download(self, peer: "Peer") -> bool:
        for piece_idx in self._get_peer_pieces(peer):
            if (self._is_piece_downloading(piece_idx)
                    and (self._piece_have_blocks_to_download(piece_idx)
                         or self._piece_have_downloading_blocks(piece_idx))):
                return True
        return False

    def get_next_block_to_download(self, peer: "Peer") -> Optional[Block]:
        if block := self._get_long_downloading_peer_block(peer):
            self._restart_block_download(block)
            return block
        elif block := self._get_next_peer_block_to_download(peer):
            self._start_block_download(block)
            return block
        else:
            return None

    def notify_peer_have_piece(
        self,
        peer: "Peer",
        piece_idx: int
    ) -> None:
        self._pieces_map[peer][piece_idx] = True

    def notify_peer_send_block(
        self,
        piece_idx: int,
        offset: int,
        block_data: bytes
    ) -> None:
        block = Block.from_raw(piece_idx, offset, block_data)
        if self._is_already_downloaded_block(block):
            return
        self._finish_block_download(block)
        self._save_block_data(piece_idx, offset, block_data)
        if self._all_piece_blocks_downloaded(piece_idx):
            if not self._is_piece_hash_valid(piece_idx):  # TODO: Log it
                self._restart_piece_download(piece_idx)
            else:
                piece_data = self._get_downloaded_piece_data(piece_idx)
                self._saver.save_piece_data(piece_idx, piece_data)
                self._finish_piece_download(piece_idx)
                self._notify_peers_about_piece(piece_idx)
                if self._have_pieces_to_download():
                    self._start_piece_download(
                        self._get_next_piece_to_download())
                if self._all_downloaded:
                    self._finish_download()

    def get_new_piece_to_notify_peer(self, peer: "Peer") -> Optional[int]:
        return (self._new_pieces_to_notify_peer[peer].popleft()
                if self._new_pieces_to_notify_peer[peer] else None)

    async def wait_for_full_download(self) -> None:
        await self._full_download_event.wait()
        await self._saver.ensure_all_pieces_saved()

    def _start_piece_download(self, piece_idx: int) -> None:
        self._downloading_pieces.add(piece_idx)
        self._pieces_to_downloading_blocks[piece_idx] = set()
        self._pieces_downloaded_blocks[piece_idx] = []

    def _finish_piece_download(self, piece_idx: int) -> None:
        self._downloading_pieces.remove(piece_idx)
        self._pieces_to_downloading_blocks.pop(piece_idx)
        self._pieces_downloaded_blocks.pop(piece_idx)

    def _restart_piece_download(self, piece_idx: int) -> None:
        for offest, block_data in self._pieces_downloaded_blocks[piece_idx]:
            block = Block.from_raw(piece_idx, offest, block_data)
            self._blocks_to_download[piece_idx].add(block)
            self.left += block.length
            self.downloaded -= block.length

    def _is_piece_downloading(self, piece_idx: int) -> bool:
        return piece_idx in self._downloading_pieces

    def _is_piece_hash_valid(self, piece_idx: int) -> bool:
        piece_data = self._get_downloaded_piece_data(piece_idx)
        return self._torrent.info.pieces[piece_idx] \
            == sha1(piece_data).digest()

    def _get_downloaded_piece_data(self, piece_idx: int) -> bytes:
        return b"".join(
            block_data for _, block_data
            in self._pieces_downloaded_blocks[piece_idx]
        )

    def _have_pieces_to_download(self) -> bool:
        return bool(self._pieces_to_download)

    def _get_next_piece_to_download(self) -> int:
        return self._pieces_to_download.pop()

    def _peer_have_piece(self, peer: "Peer", piece_idx: int) -> bool:
        return self._pieces_map[peer][piece_idx]

    def _get_peer_pieces(self, peer: "Peer") -> Iterable[int]:
        return (piece_idx for piece_idx in range(self.pieces_count)
                if self._peer_have_piece(peer, piece_idx))

    def _piece_have_blocks_to_download(self, piece_idx: int) -> bool:
        return bool(self._blocks_to_download[piece_idx])

    def _piece_have_downloading_blocks(self, piece_idx: int) -> bool:
        return bool(self._pieces_to_downloading_blocks[piece_idx])

    def _notify_peers_about_piece(self, piece_idx: int) -> None:
        for peer in self._peers_registry:
            self._new_pieces_to_notify_peer[peer].append(piece_idx)

    def _all_piece_blocks_downloaded(self, piece_idx: int) -> bool:
        return (not self._piece_have_blocks_to_download(piece_idx)
                and not self._piece_have_downloading_blocks(piece_idx))

    def _start_block_download(self, block: Block) -> None:
        self._blocks_to_download[block.piece_idx].remove(block)
        self._downloading_blocks_to_download_start_times[block] = time.time()
        self._pieces_to_downloading_blocks[block.piece_idx].add(block)

    def _finish_block_download(self, block: Block) -> None:
        self._downloading_blocks_to_download_start_times.pop(block)
        self._pieces_to_downloading_blocks[block.piece_idx].remove(block)
        self.left -= block.length
        self.downloaded += block.length

    def _restart_block_download(self, block: Block) -> None:
        self._downloading_blocks_to_download_start_times[block] = time.time()

    def _save_block_data(
        self,
        piece_idx: int,
        offset: int,
        block_data: bytes
    ) -> None:
        piece_downloaded_blocks = self._pieces_downloaded_blocks[piece_idx]
        piece_downloaded_blocks_offsets = [
            block_offset for block_offset, _ in piece_downloaded_blocks]
        block_idx = bisect.bisect_left(piece_downloaded_blocks_offsets, offset)
        piece_downloaded_blocks.insert(block_idx, (offset, block_data))

    def _get_long_downloading_peer_block(
        self,
        peer: "Peer"
    ) -> Optional[Block]:
        current_time = time.time()
        for block in self._get_downloading_blocks():
            if self._peer_have_piece(peer, block.piece_idx) \
                    and self._is_long_downloading_block(
                                block, current_time=current_time):
                return block
        return None

    def _get_next_peer_block_to_download(
        self,
        peer: "Peer"
    ) -> Optional[Block]:
        pieces_to_download = [
            piece_idx for piece_idx in self._get_peer_pieces(peer)
            if (self._is_piece_downloading(piece_idx)
                and self._piece_have_blocks_to_download(piece_idx))
        ]
        if not pieces_to_download:
            return None
        piece_idx_to_download = choice(pieces_to_download)
        return choice(list(self._blocks_to_download[piece_idx_to_download]))

    def _is_already_downloaded_block(self, block: Block) -> bool:
        return (not self._is_downloading_block(block)
                and not self._is_block_to_download(block))

    def _get_downloading_blocks(self) -> Iterable[Block]:
        return self._downloading_blocks_to_download_start_times.keys()

    def _is_block_to_download(self, block: Block) -> bool:
        return block in self._blocks_to_download[block.piece_idx]

    def _is_downloading_block(self, block: Block) -> bool:
        return block in self._downloading_blocks_to_download_start_times

    def _is_long_downloading_block(
        self,
        block: Block,
        current_time: Optional[float]
    ) -> bool:
        if not self._is_downloading_block(block):
            return False
        if current_time is None:
            current_time = time.time()
        download_start_time = \
            self._downloading_blocks_to_download_start_times[block]
        return ((current_time - download_start_time)
                >= self._TIME_FOR_BLOCK_DOWNLOAD)

    @property
    def _all_downloaded(self) -> bool:
        return self.left == 0

    def _finish_download(self) -> None:
        self.event = DownloadEvents.COMPLETED
        self._full_download_event.set()

    def _init_blocks(self) -> list[set[Block]]:
        data_map: list[set[Block]] \
            = [set() for _ in range(self.pieces_count)]
        bytes_to_allocate = self._torrent.size
        for piece_idx, piece_block_container in enumerate(data_map):
            bytes_to_allocate_in_container = min(
                self._torrent.info.piece_length, bytes_to_allocate)
            bytes_to_allocate -= bytes_to_allocate_in_container
            block_offset = 0
            while bytes_to_allocate_in_container:
                block_length = min(
                    self._MAX_BLOCK_LENGTH, bytes_to_allocate_in_container)
                piece_block_container.add(
                    Block(piece_idx,
                          block_offset,
                          block_length)
                )
                block_offset += block_length
                bytes_to_allocate_in_container -= block_length
        return data_map

    def _init_peer_pieces_map(self) -> list[bool]:
        return [False for _ in range(self.pieces_count)]

    def _init_pieces_to_download(self) -> set[int]:
        return set(piece_idx for piece_idx, _
                   in enumerate(self._blocks_to_download))

    def _start_downloading_first_pieces(self) -> None:
        for _ in range(
                min(self._MAX_PIECES_TO_DOWNLOAD_TOGETHER, self.pieces_count)):
            self._start_piece_download(self._get_next_piece_to_download())
