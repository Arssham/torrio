import asyncio

from typing import NoReturn
from functools import partial

from torrio.torrent import Torrent
from torrio.utils import wait_cancelled
from torrio.client_info import ClientInfo
from torrio.download_state import DownloadState
from torrio.peer import Peer, PeerCommunicationError
from torrio.tracker import Tracker, TrackerConnectionError


class TorrentClient:
    _MIN_INTERVAL_BETWEEN_REQUESTS_TO_TRACKER = 30

    def __init__(
        self,
        torrent: Torrent,
        client_info: ClientInfo,
        download_state: DownloadState,
        trackers: list[Tracker]
    ) -> None:
        self._torrent = torrent
        self._client_info = client_info
        self._download_state = download_state
        self._trackers = trackers

    async def run(self) -> None:
        available_peers: asyncio.Queue[Peer] = asyncio.Queue()

        getting_peers_tasks = [
            asyncio.create_task(
                self._start_getting_peers_from_tracker(
                    tracker,
                    available_peers
                )
            )
            for tracker in self._trackers
        ]

        manage_peer_communications_task = asyncio.create_task(
            self._start_managing_peer_communications(available_peers)
        )

        await self._download_state.wait_for_full_download()

        for task in getting_peers_tasks:
            task.cancel()
        manage_peer_communications_task.cancel()
        await asyncio.gather(*map(wait_cancelled, getting_peers_tasks))
        await wait_cancelled(manage_peer_communications_task)

    async def _start_getting_peers_from_tracker(
        self,
        tracker: Tracker,
        available_peers: asyncio.Queue[Peer]
    ) -> NoReturn:
        while True:
            try:
                tracker_resp = await tracker.ask()
            except TrackerConnectionError:
                # TODO: Log it
                await asyncio.sleep(
                    self._MIN_INTERVAL_BETWEEN_REQUESTS_TO_TRACKER)
                continue

            if tracker_resp.failure_reason:
                pass  # TODO: Log it
            else:
                for peer in tracker_resp.peers:
                    await available_peers.put(peer)

            await asyncio.sleep(
                tracker_resp.interval
                or self._MIN_INTERVAL_BETWEEN_REQUESTS_TO_TRACKER
            )

    async def _start_managing_peer_communications(
        self,
        available_peers: asyncio.Queue[Peer]
    ) -> NoReturn:
        def communication_done_cb(peer, task: asyncio.Task) -> None:
            peers_to_communication_tasks.pop(peer)
            self._download_state.unregister_peer(peer)
            used_peers.add(peer)
            available_peers.task_done()

        used_peers: set[Peer] = set()
        peers_to_communication_tasks: dict[Peer, asyncio.Task] = {}

        try:
            while True:
                peer = await available_peers.get()
                if peer not in used_peers \
                        and not self._download_state.is_peer_registered(peer):
                    self._download_state.register_peer(peer)
                    communication_task = asyncio.create_task(
                        self._start_communicating_with_peer(peer)
                    )
                    peers_to_communication_tasks[peer] = communication_task
                    communication_task.add_done_callback(
                        partial(communication_done_cb, peer))
                else:
                    available_peers.task_done()
        finally:
            for peer in peers_to_communication_tasks:
                peers_to_communication_tasks[peer].cancel()
            await asyncio.gather(
                *map(wait_cancelled, peers_to_communication_tasks.values()))

    async def _start_communicating_with_peer(self, peer: Peer) -> None:
        try:
            await peer.start_communicating(
                self._torrent.info_hash,
                self._client_info.peer_id,
                self._download_state,
            )
        except PeerCommunicationError:
            pass  # TODO: Log it
