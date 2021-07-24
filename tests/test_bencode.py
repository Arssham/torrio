from unittest.mock import patch, PropertyMock

import pytest

from torrio.bencode import (BencodeDecoder,
                            BencodeDecodeError,
                            BencodeEncoder,
                            BencodeEncodeError)


DEC_PATH = "torrio.bencode.BencodeDecoder"
ENC_PATH = "torrio.bencode.BencodeEncoder"


class TestBencodeDecoder:
    @pytest.mark.parametrize("cur_ch, expected_res", [
        (b"i", "int"),
        (b"l", "list"),
        (b"d", "dict"),
        (b"3", "string")
    ])
    @patch(f"{DEC_PATH}._decode_int", lambda o: "int")
    @patch(f"{DEC_PATH}._decode_list", lambda o: "list")
    @patch(f"{DEC_PATH}._decode_dict", lambda o: "dict")
    @patch(f"{DEC_PATH}._decode_string", lambda o: "string")
    def test_decode(self, cur_ch, expected_res):
        with patch(f"{DEC_PATH}._cur_ch", new_callable=PropertyMock) as cur_ch_mock:
            cur_ch_mock.return_value = cur_ch
            assert BencodeDecoder(None).decode() == expected_res

    @pytest.mark.parametrize("cur_ch", [
        b"-", b"a", b"$", b"'", b"e", b"+"
    ])
    def test_decode_with_err(self, cur_ch):
        with patch(f"{DEC_PATH}._cur_ch", new_callable=PropertyMock) as cur_ch_mock:
            cur_ch_mock.return_value = cur_ch
            with pytest.raises(BencodeDecodeError) as excinfo:
                BencodeDecoder(None).decode()
            assert str(excinfo.value) == "Invalid character on position 0"

    @pytest.mark.parametrize("data, cur_pos, expected_ch", [
        (b"i34e", 0, b"i"),
        (b"i43e", 3, b"e"),
        (b"4:test", 1, b":")
    ])
    def test_cur_ch(self, data, cur_pos, expected_ch):
        decoder = BencodeDecoder(data)
        decoder._cur_pos = cur_pos
        assert decoder._cur_ch == expected_ch

    @pytest.mark.parametrize("data, cur_pos, expected_res, expected_cur_pos", [
        (b"i34e", 0, 34, 4),
        (b"i143e", 0, 143, 5),
        (b"d3:keyi34ee", 6, 34, 10)
    ])
    def test_decode_int(self, data, cur_pos, expected_res, expected_cur_pos):
        decoder = BencodeDecoder(data)
        decoder._cur_pos = cur_pos
        assert decoder._decode_int() == expected_res
        assert decoder._cur_pos == expected_cur_pos

    @pytest.mark.parametrize("data, cur_pos, expected_res, expected_cur_pos", [
        (b"4:test", 0, b"test", 6),
        (b"l5:test15:test2e", 1, b"test1", 8),
        (b"l5:test15:test2e", 8, b"test2", 15)
    ])
    def test_decode_string(self, data, cur_pos, expected_res, expected_cur_pos):
        decoder = BencodeDecoder(data)
        decoder._cur_pos = cur_pos
        assert decoder._decode_string() == expected_res
        assert decoder._cur_pos == expected_cur_pos

    @pytest.mark.parametrize("data, cur_pos, expected_res, expected_cur_pos", [
        (b"li34ei43ee", 0, [34, 43], 10),
        (b"d3:keyli34eee", 6, [34], 12)
    ])
    def test_decode_list(self, data, cur_pos, expected_res, expected_cur_pos):
        decoder = BencodeDecoder(data)
        decoder._cur_pos = cur_pos
        assert decoder._decode_list() == expected_res
        assert decoder._cur_pos == expected_cur_pos

    @pytest.mark.parametrize("data, cur_pos, expected_res, expected_cur_pos", [
        (b"d4:key14:val14:key24:val2e", 0, {b"key1": b"val1", b"key2": b"val2"}, 26),
        (b"d3:keyd5:inneri34eee", 6, {b"inner": 34}, 19)
    ])
    def test_decode_dict(self, data, cur_pos, expected_res, expected_cur_pos):
        decoder = BencodeDecoder(data)
        decoder._cur_pos = cur_pos
        assert decoder._decode_dict() == expected_res
        assert decoder._cur_pos == expected_cur_pos


class TestBencodeEncoder:
    @pytest.mark.parametrize("_input, expected_res", [
        (1, b"i1e"),
        (b"test", b"4:test"),
        ([34, b"test"], b"li34e4:teste"),
        ({b"test": b"test"}, b"d4:test4:teste"),
        ({b"test": [{b"1": 1, b"34": 34, b"43": {b"2": b"2"}}]},
         b"d4:testld1:1i1e2:34i34e2:43d1:21:2eeee")
    ])
    def test_encode(self, _input, expected_res):
        assert BencodeEncoder(None)._encode(_input) == expected_res

    def test_encode_with_err(self):
        with pytest.raises(BencodeEncodeError) as err:
            BencodeEncoder(None)._encode(object)
        assert str(err.value) == "Object of type type is not Bencode serializable"