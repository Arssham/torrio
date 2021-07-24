import os
import bisect
import asyncio

from typing import NoReturn
from functools import cached_property, partial
from concurrent.futures import ThreadPoolExecutor

from torrio.torrent import Torrent
from torrio.utils import wait_cancelled


class Saver:
    _DOWNLOAD_FOLDER = os.path.join(os.getcwd(), "downloads")

    def __init__(self, torrent: Torrent) -> None:
        self._torrent = torrent

        self._saving_task: asyncio.Task = None  # type: ignore[assignment]

        self._pieces_to_save: asyncio.Queue[tuple[int, bytes]] \
            = asyncio.Queue()

        self._pool: ThreadPoolExecutor = None  # type: ignore[assignment]

        self._file_paths_to_descriptors: dict[str, int] = \
            None  # type: ignore[assignment]

    def start(self) -> None:
        self._pool = ThreadPoolExecutor()
        self._prepare_files()
        self._file_paths_to_descriptors = self._init_file_descriptors()
        self._saving_task = asyncio.create_task(self._start_saving())

    async def stop(self) -> None:
        self._saving_task.cancel()
        await wait_cancelled(self._saving_task)
        for file_path in self._file_paths_to_descriptors:
            os.close(self._file_paths_to_descriptors[file_path])
        self._pool.shutdown()

    async def ensure_all_pieces_saved(self) -> None:
        await self._pieces_to_save.join()

    def save_piece_data(
        self,
        piece_idx: int,
        piece_data: bytes
    ) -> None:
        self._pieces_to_save.put_nowait((piece_idx, piece_data))

    async def _start_saving(self) -> NoReturn:
        while True:
            piece_idx, piece_data = await self._pieces_to_save.get()
            await self._write_piece_to_disk(piece_idx, piece_data)
            self._pieces_to_save.task_done()

    def _prepare_files(self) -> None:
        for file_path in self._file_paths:
            file_dir = os.path.dirname(file_path)
            os.makedirs(file_dir, exist_ok=True)
            os.mknod(file_path)

    def _init_file_descriptors(self) -> dict[str, int]:
        return {file_path: os.open(file_path, os.O_RDWR)
                for file_path in self._file_paths}

    async def _write_piece_to_disk(
        self,
        piece_idx: int,
        piece_data: bytes
    ) -> None:
        where_and_what_to_write = \
            self._find_where_and_what_to_write(piece_idx, piece_data)
        for file_path, offset_in_file, data_to_write \
                in where_and_what_to_write:
            descriptor = self._file_paths_to_descriptors[file_path]
            await self._move_descriptor(descriptor, offset_in_file)
            await self._write_to_descriptor(descriptor, data_to_write)

    async def _move_descriptor(
        self,
        descriptor: int,
        offset_in_file: int
    ) -> None:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
                self._pool,
                partial(os.lseek,
                        descriptor,
                        offset_in_file,
                        os.SEEK_SET)
        )

    async def _write_to_descriptor(
        self,
        descriptor: int,
        data_to_write: bytes
    ) -> None:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            self._pool,
            partial(os.write,
                    descriptor,
                    data_to_write)
        )

    def _find_where_and_what_to_write(
        self,
        piece_idx: int,
        piece_data: bytes
    ) -> list[tuple[str, int, bytes]]:
        where_and_what_to_write = []
        piece_global_offset = self._piece_global_offsets[piece_idx]
        piece_length = self._piece_lengths[piece_idx]
        file_paths = self._get_files_which_crosses_by(
            piece_global_offset, piece_global_offset + piece_length)
        for file_path in file_paths:
            file_global_offset = self._file_global_offsets[file_path]
            file_length = self._file_lengths[file_path]
            chunk_begin = max(piece_global_offset, file_global_offset)
            chunk_end = min(piece_global_offset + piece_length,
                            file_global_offset + file_length)
            chunk_length = chunk_end - chunk_begin
            chunk = piece_data[: chunk_length]
            piece_data = piece_data[chunk_length:]
            where_and_what_to_write.append((
                file_path,
                self._transform_global_offset_to_file_offset(
                    chunk_begin, file_path),
                chunk
            ))
        return where_and_what_to_write

    def _get_files_which_crosses_by(
        self,
        from_: int,
        to: int
    ) -> list[str]:
        def find_first_file_idx(offset) -> int:
            if not self._torrent.is_multifile:
                return 0
            file_offsets = list(self._file_global_offsets.values())
            return bisect.bisect_right(file_offsets, offset) - 1

        def is_file_crossing(file_idx: int, from_: int, to: int) -> bool:
            file_path = self._file_paths[file_idx]
            file_global_offset = self._file_global_offsets[file_path]
            file_length = self._file_lengths[file_path]
            file_global_end = file_global_offset + file_length
            return file_global_offset < to and file_global_end > from_

        res_files = []
        files_count = len(self._file_paths)
        current_file_idx = find_first_file_idx(from_)
        while (current_file_idx < files_count
               and is_file_crossing(current_file_idx, from_, to)):
            res_files.append(self._file_paths[current_file_idx])
            current_file_idx += 1
        return res_files

    @cached_property
    def _file_paths(self) -> list[str]:
        if not self._torrent.is_multifile:
            file_path = os.path.join(
                self._DOWNLOAD_FOLDER,
                self._torrent.info.name
            )
            return [file_path]
        return [os.path.join(
                    self._DOWNLOAD_FOLDER,
                    self._torrent.info.name,
                    *file_info["path"]
                )
                for file_info in self._torrent.info.files]

    @cached_property
    def _file_lengths(self) -> dict[str, int]:
        if self._torrent.is_multifile:
            return {file_path: self._torrent.info.files[file_idx]["length"]
                    for file_idx, file_path in enumerate(self._file_paths)}
        return {self._file_paths[0]: self._torrent.info.length}

    @cached_property
    def _piece_lengths(self) -> list[int]:
        piece_lengths = []
        bytes_left = self._torrent.size
        while bytes_left:
            piece_length = min(
                bytes_left, self._torrent.info.piece_length)
            piece_lengths.append(piece_length)
            bytes_left -= piece_length
        return piece_lengths

    @cached_property
    def _file_global_offsets(self) -> dict[str, int]:
        file_global_offsets = {}
        cur_file_offset = 0
        for file_path, file_length in self._file_lengths.items():
            file_global_offsets[file_path] = cur_file_offset
            cur_file_offset += file_length
        return file_global_offsets

    @cached_property
    def _piece_global_offsets(self) -> list[int]:
        piece_global_offsets: list[int] = []
        cur_piece_offset = 0
        for piece_length in self._piece_lengths:
            piece_global_offsets.append(cur_piece_offset)
            cur_piece_offset += piece_length
        return piece_global_offsets

    def _transform_global_offset_to_file_offset(
        self,
        global_offset: int,
        file_path: str
    ) -> int:
        return global_offset - self._file_global_offsets[file_path]
