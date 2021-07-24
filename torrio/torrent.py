import hashlib

from itertools import chain
from functools import cached_property
from operator import itemgetter, methodcaller

from torrio import bencode
from torrio.utils import is_url


class TorrentInfo:
    def __init__(self, raw_info: dict) -> None:
        self._raw_info = raw_info

    @cached_property
    def piece_length(self) -> int:
        return self._raw_info[b"piece length"]

    @cached_property
    def pieces(self) -> list[bytes]:
        raw_pieces = self._raw_info[b"pieces"]
        return [
            raw_pieces[offset: offset + 20]
            for offset in range(0, len(raw_pieces), 20)
        ]

    @cached_property
    def name(self) -> str:
        return self._raw_info[b"name"].decode("utf-8")

    @cached_property
    def length(self) -> int:
        return self._raw_info[b"length"]

    @cached_property
    def files(self) -> list[dict]:
        return [{"path": list(map(methodcaller("decode", "utf-8"),
                                  file_info[b"path"])),
                 "length": file_info[b"length"],
                 "md5sum": file_info.get(b"md5sum")}
                for file_info in self._raw_info[b"files"]]


class Torrent:
    def __init__(self, path: str) -> None:
        self._data = self._read_torrent(path)

    def _read_torrent(self, path: str) -> dict:
        with open(path, "rb") as fin:
            file_data = fin.read()
        # .torrent file always contains dict in bencode format
        return bencode.loads(file_data)

    @cached_property
    def info(self) -> TorrentInfo:
        return TorrentInfo(self._data[b"info"])

    @cached_property
    def announce(self) -> str:
        return self._data[b"announce"].decode("utf-8")

    @cached_property
    def announce_list(self) -> list[list[str]]:
        return [
            list(
                map(methodcaller("decode", "utf-8"),
                    filter(is_url, urls))
            )
            for urls in self._data[b"announce-list"]
        ]

    @cached_property
    def info_hash(self) -> bytes:
        return hashlib.sha1(bencode.dumps(self._data[b"info"])).digest()

    @cached_property
    def is_multifile(self) -> bool:
        return b"files" in self._data[b"info"]

    @cached_property
    def size(self) -> int:
        if self.is_multifile:
            return sum(map(itemgetter("length"), self.info.files))
        return self.info.length

    @cached_property
    def announce_urls(self) -> list[str]:
        if b"announce-list" in self._data:
            return list(chain(*self.announce_list))
        return [self.announce]
