from typing import Union, cast


def loads(data: bytes) -> dict:
    return cast(dict, BencodeDecoder(data).decode())


class BencodeDecoder:
    def __init__(self, data: bytes):
        self._data = data
        self._cur_pos = 0

    def decode(self) -> Union[dict, list, bytes, int]:
        if self._cur_ch == b"i":
            return self._decode_int()
        elif self._cur_ch == b"l":
            return self._decode_list()
        elif self._cur_ch == b"d":
            return self._decode_dict()
        elif self._cur_ch.isdigit():
            return self._decode_string()
        else:
            raise BencodeDecodeError(
                f"Invalid character on position {self._cur_pos}")

    @property
    def _cur_ch(self) -> bytes:
        return self._data[self._cur_pos: self._cur_pos + 1]

    def _decode_int(self) -> int:
        begin = self._cur_pos + 1
        end = self._data.index(b"e", begin)
        self._cur_pos = end + 1
        return int(self._data[begin:end])

    def _decode_string(self) -> bytes:
        separator_idx = self._data.index(b":", self._cur_pos)
        length = int(self._data[self._cur_pos: separator_idx])
        begin = separator_idx + 1
        end = begin + length
        self._cur_pos = end
        return self._data[begin:end]

    def _decode_list(self) -> list[Union[dict, list, bytes, int]]:
        blist = []
        self._cur_pos += 1
        while self._cur_ch != b"e":
            blist.append(self.decode())
        self._cur_pos += 1
        return blist

    def _decode_dict(self) -> dict[bytes, Union[dict, list, bytes, int]]:
        bdict = {}
        self._cur_pos += 1
        while self._cur_ch != b"e":
            key = self._decode_string()
            value = self.decode()
            bdict[key] = value
        self._cur_pos += 1
        return bdict


class BencodeDecodeError(ValueError):
    pass


def dumps(data: dict) -> bytes:
    return BencodeEncoder(data).encode()


class BencodeEncoder:
    def __init__(self, data: Union[dict, list, bytes, int]) -> None:
        self._data = data

    def _encode(self, data: Union[dict, list, bytes, int]) -> bytes:
        if isinstance(data, dict):
            payload = b"".join(
                b"".join(map(self._encode, pair)) for pair in data.items()
            )
            return b"d" + payload + b"e"
        elif isinstance(data, list):
            payload = b"".join(map(self._encode, data))
            return b"l" + payload + b"e"
        elif isinstance(data, bytes):
            return str(len(data)).encode("utf-8") + b":" + data
        elif isinstance(data, int):
            return b"i" + str(data).encode("utf-8") + b"e"
        else:
            err_msg = (
                f"Object of type {type(data).__name__} "
                f"is not Bencode serializable"
            )
            raise BencodeEncodeError(err_msg)

    def encode(self) -> bytes:
        return self._encode(self._data)


class BencodeEncodeError(TypeError):
    pass
