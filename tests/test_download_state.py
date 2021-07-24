from asyncio import Event
from collections import deque
from unittest.mock import patch, PropertyMock, Mock, AsyncMock

import pytest

from torrio.saver import Saver
from torrio.download_state import Block, DownloadState


class TestDownloadState:
    @pytest.mark.parametrize("peers_registry, peer_to_check, is_in", [
        ({1, 2, 3}, 1, True),
        ({2, 3}, 3, True),
        ({4, 5}, 1, False),
        (set(), 6, False)
    ])
    def test_is_peer_registered(self, peers_registry, peer_to_check, is_in):
        def fake_init(obj):
            obj._peers_registry = peers_registry

        with patch.object(DownloadState, "__init__", fake_init):
            state = DownloadState()
            assert (state.is_peer_registered(peer_to_check)
                    if is_in
                    else not state.is_peer_registered(peer_to_check))

    @patch.object(DownloadState, "_init_peer_pieces_map")
    def test_register_peer(self, init_peer_pieces_map_map):
        def fake_init(obj):
            obj._peers_registry = set()
            obj._pieces_map = {}
            obj._new_pieces_to_notify_peer = {}
    
        fake_peer_pieces_map = [False]
        init_peer_pieces_map_map.return_value = fake_peer_pieces_map

        with patch.object(DownloadState, "__init__", fake_init):
            state = DownloadState()
            fake_peer = Mock()
            state.register_peer(fake_peer)
            assert state._peers_registry == {fake_peer}
            assert state._pieces_map[fake_peer] == fake_peer_pieces_map
            assert state._new_pieces_to_notify_peer[fake_peer] == deque()

    def test_unregister_peer(self):
        fake_peer = Mock()
        fake_peer_pieces_map = [False]
    
        def fake_init(obj):
            obj._peers_registry = {fake_peer}
            obj._pieces_map = {fake_peer: fake_peer_pieces_map}
            obj._new_pieces_to_notify_peer = {fake_peer: deque()}

        with patch.object(DownloadState, "__init__", fake_init):
            state = DownloadState()
            state.unregister_peer(fake_peer)
            assert state._peers_registry == set()
            assert state._pieces_map == {}
            assert state._new_pieces_to_notify_peer == {}

    def test_pieces_count(self):
        expected_pieces_count = 5

        def fake_init(obj):
            obj._torrent = Mock(
                info=Mock(
                    pieces=[Mock() for _ in range(expected_pieces_count)]
                )
            )

        with patch.object(DownloadState, "__init__", fake_init):
            state = DownloadState()
            assert state.pieces_count == expected_pieces_count


    @pytest.mark.parametrize(
        "peer, peer_pieces, downloading_pieces, " \
        "piece_have_blocks_to_download, piece_have_downloading_blocks, expected",
        [
            ("peer1", [], {}, {}, {}, False),
            ("peer1", [1, 2, 3],
             {1: False, 2: False, 3: False},
             {1: True, 2: True, 3: True},
             {1: False, 2: False, 3: False}, False),
            ("peer1", [1, 2, 3],
             {1: True, 2: True, 3: False},
             {1: False, 2: False, 3: True},
             {1: False, 2: False, 3: False}, False),
            ("peer1", [1, 2, 3],
             {1: True, 2: False, 3: False},
             {1: True, 2: True, 3: True},
             {1: False, 2: False, 3: False}, True),
            ("peer1", [1, 2, 3],
             {1: True, 2: False, 3: False},
             {1: False, 2: True, 3: True},
             {1: True, 2: False, 3: False}, True),
        ]
    )
    @patch.object(DownloadState, "__init__")
    @patch.object(DownloadState, "_get_peer_pieces")
    @patch.object(DownloadState, "_is_piece_downloading")
    @patch.object(DownloadState, "_piece_have_blocks_to_download")
    @patch.object(DownloadState, "_piece_have_downloading_blocks")
    def test_peer_have_blocks_to_download(
        self,
        piece_have_downloading_blocks_mock,
        piece_have_blocks_to_download_mock,
        is_piece_downloading_mock,
        get_peer_pieces_mock,
        init_mock,

        peer,
        peer_pieces,
        downloading_pieces,
        piece_have_blocks_to_download,
        piece_have_downloading_blocks,
        expected
    ):
        init_mock.return_value = None
        get_peer_pieces_mock.return_value = peer_pieces
        is_piece_downloading_mock.side_effect = \
            lambda piece_idx: downloading_pieces[piece_idx]
        piece_have_blocks_to_download_mock.side_effect = \
            lambda piece_idx: piece_have_blocks_to_download[piece_idx]
        piece_have_downloading_blocks_mock.side_effect = \
            lambda piece_idx: piece_have_downloading_blocks[piece_idx]
        
        state = DownloadState()
        assert state.peer_have_blocks_to_download(peer) is expected

    @pytest.mark.parametrize(
        "peer, long_downloading_peer_block, " \
        "is_restart_block_download_called, " \
        "next_peer_block_to_download, " \
        "is_start_block_download_called, expected",
        [
            ("peer1", None, False, None, False, None),
            ("peer1", "block1", True, None, False, "block1"),
            ("peer1", None, False, "block1", True, "block1"),
            ("peer1", "block1", True, "block2", False, "block1")
        ]
    )
    @patch.object(DownloadState, "__init__")
    @patch.object(DownloadState, "_get_long_downloading_peer_block")
    @patch.object(DownloadState, "_restart_block_download")
    @patch.object(DownloadState, "_get_next_peer_block_to_download")
    @patch.object(DownloadState, "_start_block_download")
    def test_get_next_block_to_download(
        self,
        start_block_download_mock,
        get_next_peer_block_to_download_mock,
        restart_block_download_mock,
        get_long_downloading_peer_block_mock,
        init_mock,

        peer,
        long_downloading_peer_block,
        is_restart_block_download_called,
        next_peer_block_to_download,
        is_start_block_download_called,
        expected
    ):
        init_mock.return_value = None
        get_long_downloading_peer_block_mock.return_value = \
            long_downloading_peer_block
        get_next_peer_block_to_download_mock.return_value = \
            next_peer_block_to_download

        state = DownloadState()
        assert state.get_next_block_to_download(peer) is expected
        if is_restart_block_download_called:
            restart_block_download_mock.assert_called_once()
        if is_start_block_download_called:
            start_block_download_mock.assert_called_once()

    def test_notify_peer_have_piece(self):
        peer = "peer1"
        piece_idx = 0

        def fake_init(obj):
            obj._pieces_map = {peer: [False]}

        with patch.object(DownloadState, "__init__", fake_init):
            state = DownloadState()
            state.notify_peer_have_piece(peer, piece_idx)
            assert state._pieces_map[peer][piece_idx]

    @pytest.mark.parametrize(
        "block, is_already_downloaded_block, " \
        "is_all_piece_blocks_downloaded, is_piece_hash_valid, " \
        "piece_data, have_pieces_to_download, next_piece_to_download, "\
        "all_downloaded, " \
        "is_finish_block_download_called, is_save_block_data_called, " \
        "is_restart_piece_download_called, is_save_piece_data_called, " \
        "is_finish_piece_download_called, is_notify_peers_about_piece_called, " \
        "is_start_piece_download_called, is_finish_download_called",
        [
            # Already downloaded block
            ("block1", True, Mock(), Mock(), Mock(), Mock(), Mock(), Mock(),
             False, False, False, False, False, False, False, False),
            # Not all piece blocks downloaded
            ("block1", False, False, Mock(), Mock(), Mock(), Mock(), Mock(),
             True, True, False, False, False, False, False, False),
            # Restart block download
            ("block1", False, True, False, Mock(), Mock(), Mock(), Mock(),
             True, True, True, False, False, False, False, False),
            # Have pieces to download
            ("block1", False, True, True, "piece_data", True, "new_piece", False,
             True, True, False, True, True, True, True, False),
            # All downloaded
            ("block1", False, True, True, "piece_data", False, Mock(), True,
             True, True, False, True, True, True, False, True),
        ]
    )
    @patch.object(Block, "from_raw")
    @patch.object(DownloadState, "_is_already_downloaded_block")
    @patch.object(DownloadState, "_finish_block_download")
    @patch.object(DownloadState, "_save_block_data")
    @patch.object(DownloadState, "_all_piece_blocks_downloaded")
    @patch.object(DownloadState, "_is_piece_hash_valid")
    @patch.object(DownloadState, "_restart_piece_download")
    @patch.object(DownloadState, "_get_downloaded_piece_data")
    @patch.object(Saver, "save_piece_data")
    @patch.object(DownloadState, "_finish_piece_download")
    @patch.object(DownloadState, "_notify_peers_about_piece")
    @patch.object(DownloadState, "_have_pieces_to_download")
    @patch.object(DownloadState, "_start_piece_download")
    @patch.object(DownloadState, "_get_next_piece_to_download")
    @patch.object(DownloadState, "_all_downloaded", new_callable=PropertyMock)
    @patch.object(DownloadState, "_finish_download")
    def test_notify_peer_send_block(
        self,
        finish_download_mock,
        all_downloaded_mock,
        get_next_piece_to_download_mock,
        start_piece_download_mock,
        have_pieces_to_download_mock,
        notify_peers_about_piece_mock,
        finish_piece_download_mock,
        save_piece_data_mock,
        get_downloaded_piece_data_mock,
        restart_piece_download_mock,
        is_piece_hash_valid_mock,
        all_piece_blocks_downloaded_mock,
        save_block_data_mock,
        finish_block_download_mock,
        is_already_downloaded_block_mock,
        from_raw_mock,

        block,
        is_already_downloaded_block,
        is_all_piece_blocks_downloaded,
        is_piece_hash_valid,
        piece_data,
        have_pieces_to_download,
        next_piece_to_download,
        all_downloaded,
        is_finish_block_download_called,
        is_save_block_data_called,
        is_restart_piece_download_called,
        is_save_piece_data_called,
        is_finish_piece_download_called,
        is_notify_peers_about_piece_called,
        is_start_piece_download_called,
        is_finish_download_called
    ):
        def fake_state_init(obj):
            obj._saver = Saver()
    
        piece_idx, offset, block_data = Mock(), Mock(), Mock()
        from_raw_mock.return_value = block
        is_already_downloaded_block_mock.return_value = \
            is_already_downloaded_block
        all_piece_blocks_downloaded_mock.return_value = \
            is_all_piece_blocks_downloaded
        is_piece_hash_valid_mock.return_value = is_piece_hash_valid
        get_downloaded_piece_data_mock.return_value = piece_data
        have_pieces_to_download_mock.return_value = have_pieces_to_download
        get_next_piece_to_download_mock.return_value = next_piece_to_download
        all_downloaded_mock.return_value = all_downloaded

        with patch.object(Saver, "__init__", Mock(return_value=None)), \
                patch.object(DownloadState, "__init__", fake_state_init):
            state = DownloadState()
            state.notify_peer_send_block(piece_idx, offset, block_data)
            if is_finish_block_download_called:
                finish_block_download_mock.assert_called_once_with(block)
            if is_save_block_data_called:
                save_block_data_mock.assert_called_once_with(piece_idx, offset, block_data)
            if is_restart_piece_download_called:
                restart_piece_download_mock.assert_called_once_with(piece_idx)
            if is_save_piece_data_called:
                save_piece_data_mock.assert_called_once_with(piece_idx, piece_data)
            if is_finish_piece_download_called:
                finish_piece_download_mock.assert_called_once_with(piece_idx)
            if is_notify_peers_about_piece_called:
                notify_peers_about_piece_mock.assert_called_once_with(piece_idx)
            if is_start_piece_download_called:
                start_piece_download_mock.assert_called_once_with(next_piece_to_download)
            if is_finish_download_called:
                finish_download_mock.assert_called_once()

    @pytest.mark.parametrize("peer, new_pieces_for_peer, expected", [
        ("peer", deque(), None),
        ("peer", deque(["piece1"]), "piece1")
    ])
    def test_get_new_piece_to_notify_peer(
        self,
        peer,
        new_pieces_for_peer,
        expected
    ):
        def fake_init(obj):
            obj._new_pieces_to_notify_peer = {peer: new_pieces_for_peer}

        with patch.object(DownloadState, "__init__", fake_init):
            state = DownloadState()
            assert state.get_new_piece_to_notify_peer(peer) is expected

    @pytest.mark.asyncio
    @patch.object(Saver, "__init__")
    @patch.object(Saver, "ensure_all_pieces_saved", new_callable=AsyncMock)
    async def test_wait_for_full_download(self, ensure_all_pieces_saved_mock, saver_init_mock):
        def fake_init(obj):
            obj._full_download_event = Event()
            obj._full_download_event.set()
            obj._saver = Saver()
        
        saver_init_mock.return_value = None

        with patch.object(DownloadState, "__init__", fake_init):
            state = DownloadState()
            await state.wait_for_full_download()
            ensure_all_pieces_saved_mock.assert_called_once()

    def test_start_piece_download(self):
        def fake_init(obj):
            obj._downloading_pieces = set()
            obj._pieces_to_downloading_blocks = {}
            obj._pieces_downloaded_blocks = {}
        
        piece_idx = 1

        with patch.object(DownloadState, "__init__", fake_init):
            state = DownloadState()
            state._start_piece_download(piece_idx)
            assert state._downloading_pieces == {piece_idx}
            assert state._pieces_to_downloading_blocks[piece_idx] == set()
            assert state._pieces_downloaded_blocks[piece_idx] == []

    def test_finish_piece_download(self):
        piece_idx = 1

        def fake_init(obj):
            obj._downloading_pieces = {piece_idx}
            obj._pieces_to_downloading_blocks = {piece_idx: {Mock(), Mock()}}
            obj._pieces_downloaded_blocks = {piece_idx: [Mock(), Mock()]}

        with patch.object(DownloadState, "__init__", fake_init):
            state = DownloadState()
            state._finish_piece_download(piece_idx)
            assert state._downloading_pieces == set()
            assert piece_idx not in state._pieces_to_downloading_blocks
            assert piece_idx not in state._pieces_downloaded_blocks

    @patch.object(Block, "from_raw")
    def test_restart_piece_download(self, from_raw_mock):
        piece_idx = 1
        block_offsets = [0, 5, 10]
        blocks = [Mock(length=5), Mock(length=5), Mock(length=3)]

        def fake_from_raw(piece_idx, offest, block_data):
            return dict(zip(block_offsets, blocks))[offest]

        from_raw_mock.side_effect = fake_from_raw

        def fake_init(obj):
            obj._blocks_to_download = {piece_idx: set()}
            obj._pieces_downloaded_blocks = \
                {piece_idx: [(offset, Mock()) for offset in block_offsets]}
            obj.left = 0
            obj.downloaded = sum(block.length for block in blocks)

        with patch.object(DownloadState, "__init__", fake_init):
            state = DownloadState()
            state._restart_piece_download(piece_idx)
            assert state._blocks_to_download == {piece_idx: set(blocks)}
            assert state.left == sum(block.length for block in blocks)
            assert state.downloaded == 0

    def is_is_piece_downloading(self):
        piece_idx = 0

        def fake_init(obj):
            obj._downloading_pieces = {piece_idx}

        with patch.object(DownloadState, "__init__", fake_init):
            state =DownloadState()
            assert state._is_piece_downloading(piece_idx)

    @pytest.mark.parametrize(
        "pieces, piece_idx, digest, expected",
    [
        ([b"hash1", b"hash2"], 0, b"invalid_hash", False),
        ([b"hash1", b"hash2"], 1, b"hash2", True),
    ])
    @patch("torrio.download_state.sha1")
    @patch.object(DownloadState, "_get_downloaded_piece_data")
    def test_is_piece_hash_valid(
        self,
        get_downloaded_piece_data_mock,
        sha1_mock,

        pieces,
        piece_idx,
        digest,
        expected
    ):
        def fake_init(obj):
            obj._torrent = Mock(
                info=Mock(
                    pieces=pieces
                )
            )

        sha1_mock.return_value.digest.return_value = digest

        with patch.object(DownloadState, "__init__", fake_init):
            state = DownloadState()
            assert state._is_piece_hash_valid(piece_idx) is expected

    def test_get_downloaded_piece_data(self):
        piece_idx = 0
        downloaded_blocks = [
            (Mock(), b"block_1_data"),
            (Mock(), b"block_2_data"),
            (Mock(), b"block_3_data")
        ]
        piece_data = b"block_1_datablock_2_datablock_3_data"
    
        def fake_init(obj):
            obj._pieces_downloaded_blocks = {piece_idx: downloaded_blocks}

        with patch.object(DownloadState, "__init__", fake_init):
            state = DownloadState()
            assert state._get_downloaded_piece_data(piece_idx) == piece_data

    @pytest.mark.parametrize("pieces_to_download, expected", [
        ({1, 2, 3}, True),
        (set(), False)
    ])
    def test_have_pieces_to_download(self, pieces_to_download, expected):
        def fake_init(obj):
            obj._pieces_to_download = pieces_to_download

        with patch.object(DownloadState, "__init__", fake_init):
            state = DownloadState()
            assert state._have_pieces_to_download() is expected

    def test_get_next_piece_to_download(self):
        piece_idx = 1
        pieces_to_download = {piece_idx}
    
        def fake_init(obj):
            obj._pieces_to_download = pieces_to_download

        with patch.object(DownloadState, "__init__", fake_init):
            state = DownloadState()
            assert state._get_next_piece_to_download() == piece_idx

    @pytest.mark.parametrize("piece_idx, peer_piece_map, expected", [
        (1, [False, True, False], True),
        (0, [False, True, True], False)
    ])
    def test_peer_have_piece(self, piece_idx, peer_piece_map, expected):
        peer = Mock()
    
        def fake_init(obj):
            obj._pieces_map = {peer: peer_piece_map}

        with patch.object(DownloadState, "__init__", fake_init):
            state = DownloadState()
            assert state._peer_have_piece(peer, piece_idx) is expected

    @pytest.mark.parametrize("pieces_count, piece_map, expected", [
        (0, [], []),
        (1, [True], [0]),
        (5, [True, False, True, False, True], [0, 2, 4])
    ])
    @patch.object(DownloadState, "_peer_have_piece")
    @patch.object(DownloadState, "pieces_count", new_callable=PropertyMock)
    def test_get_peer_pieces(
        self,
        pieces_count_mock,
        peer_have_piece_mock,

        pieces_count,
        piece_map,
        expected
    ):
        def fake_peer_have_piece(peer, piece_idx):
            return piece_map[piece_idx]

        peer = Mock()
        pieces_count_mock.return_value = pieces_count
        peer_have_piece_mock.side_effect = fake_peer_have_piece

        with patch.object(DownloadState, "__init__", Mock(return_value=None)):
            state = DownloadState()
            assert list(state._get_peer_pieces(peer)) == expected

    @pytest.mark.parametrize("piece_blocks_to_download, expected", [
        (set(), False),
        ({1, 2}, True)
    ])
    def test_piece_have_blocks_to_download(self, piece_blocks_to_download, expected):
        piece_idx = 1

        def fake_init(obj):
            obj._blocks_to_download = {piece_idx: piece_blocks_to_download}

        with patch.object(DownloadState, "__init__", fake_init):
            state = DownloadState()
            assert state._piece_have_blocks_to_download(piece_idx) is expected

    @pytest.mark.parametrize("to_downloading_blocks, expected", [
        (set(), False),
        ({1, 2}, True)
    ])
    def test_piece_have_downloading_blocks(self, to_downloading_blocks, expected):
        piece_idx = 1

        def fake_init(obj):
            obj._pieces_to_downloading_blocks = {piece_idx: to_downloading_blocks}

        with patch.object(DownloadState, "__init__", fake_init):
            state = DownloadState()
            assert state._piece_have_downloading_blocks(piece_idx) is expected

    def test_notify_peers_about_piece(self):
        peer1 = Mock()
        peer2 = Mock()
        piece_idx = 1
        peers_registry = {peer1, peer2}
        new_pieces_to_notify_peer = {
            peer1: deque(),
            peer2: deque()
        }
        expected_new_pieces_to_notify_peer = {
            peer1: deque([piece_idx]),
            peer2: deque([piece_idx])
        }

        def fake_init(obj):
            obj._peers_registry = peers_registry
            obj._new_pieces_to_notify_peer = new_pieces_to_notify_peer

        with patch.object(DownloadState, "__init__", fake_init):
            state = DownloadState()
            state._notify_peers_about_piece(piece_idx)
            assert state._new_pieces_to_notify_peer == expected_new_pieces_to_notify_peer

    @pytest.mark.parametrize(
        "is_piece_have_downloading_blocks, " \
        "is_piece_have_blocks_to_download, expected",
        [(False, False, True),
         (False, True, False),
         (True, False, False),
         (True, True, False)]
    )
    @patch.object(DownloadState, "_piece_have_blocks_to_download")
    @patch.object(DownloadState, "_piece_have_downloading_blocks")
    def test_all_piece_blocks_downloaded(
        self,
        piece_have_downloading_blocks_mock,
        piece_have_blocks_to_download_mock,

        is_piece_have_downloading_blocks,
        is_piece_have_blocks_to_download,
        expected
    ):
        piece_idx = Mock()
        piece_have_downloading_blocks_mock.return_value = is_piece_have_downloading_blocks
        piece_have_blocks_to_download_mock.return_value = is_piece_have_blocks_to_download

        with patch.object(DownloadState, "__init__", Mock(return_value=None)):
            state = DownloadState()
            assert state._all_piece_blocks_downloaded(piece_idx) is expected

    @patch("torrio.download_state.time", Mock(time=Mock(return_value=1.0)))
    def test_start_block_download(self):
        piece_idx = 0
        block = Mock(piece_idx=piece_idx)

        def fake_init(obj):
            obj._blocks_to_download = {piece_idx: {block}}
            obj._downloading_blocks_to_download_start_times = {}
            obj._pieces_to_downloading_blocks = {piece_idx: set()}

        with patch.object(DownloadState, "__init__", fake_init):
            state = DownloadState()
            state._start_block_download(block)
            assert state._blocks_to_download == {piece_idx: set()}
            assert state._downloading_blocks_to_download_start_times == {block: 1.0}
            assert state._pieces_to_downloading_blocks == {piece_idx: {block}}

    def test_finish_block_download(self):
        piece_idx = 0
        block = Mock(piece_idx=piece_idx, length=100)

        def fake_init(obj):
            obj._downloading_blocks_to_download_start_times = {block: Mock()}
            obj._pieces_to_downloading_blocks = {piece_idx: {block}}
            obj.left = 100
            obj.downloaded = 0

        with patch.object(DownloadState, "__init__", fake_init):
            state = DownloadState()
            state._finish_block_download(block)
            assert state._downloading_blocks_to_download_start_times == {}
            assert state._pieces_to_downloading_blocks == {piece_idx: set()}
            assert state.left == 0
            assert state.downloaded == 100

    @patch("torrio.download_state.time", Mock(time=Mock(return_value=1.0)))
    def test_restart_block_download(self):
        block = Mock()

        def fake_init(obj):
            obj._downloading_blocks_to_download_start_times = {block: 0.5}

        with patch.object(DownloadState, "__init__", fake_init):
            state = DownloadState()
            state._restart_block_download(block)
            assert state._downloading_blocks_to_download_start_times == {block: 1.0}
