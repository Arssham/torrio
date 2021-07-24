__version__ = "0.1.0"

import asyncio
import argparse

from operator import methodcaller

from tqdm import tqdm  # type: ignore[import]

from torrio.saver import Saver
from torrio.torrent import Torrent
from torrio.utils import wait_cancelled
from torrio.client import TorrentClient
from torrio.client_info import ClientInfo
from torrio.tracker import get_tracker_class
from torrio.download_state import DownloadState


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--file", type=str)
    parser.add_argument("-p", "--progress", action="store_true")

    args = parser.parse_args()

    torrent = Torrent(args.file)

    client_info = ClientInfo()

    saver = Saver(torrent)
    saver.start()

    download_state = DownloadState(torrent, saver)

    trackers = [
        tracker_cls(
            announce,
            torrent,
            download_state,
            client_info
        )
        for announce, tracker_cls in zip(
            torrent.announce_urls,
            map(get_tracker_class, torrent.announce_urls)
        )
    ]

    torrent_client = TorrentClient(
        torrent,
        client_info,
        download_state,
        trackers
    )

    progress_task = None
    if args.progress:
        progress_task = asyncio.create_task(
            print_progress(download_state, torrent.size))

    try:
        await torrent_client.run()
    finally:
        await asyncio.gather(*map(methodcaller("close"), trackers))
        await saver.stop()
        if progress_task:
            progress_task.cancel()
            await wait_cancelled(progress_task)


PRINT_PROGRESS_DELAY = 0.1
PROGRESS_FMT = "{l_bar}{bar} [{elapsed}<{remaining}]"


async def print_progress(download_state: DownloadState, total: int) -> None:
    with tqdm(total=total, bar_format=PROGRESS_FMT) as pbar:
        downloaded = download_state.downloaded
        while downloaded != total:
            pbar.update(download_state.downloaded - downloaded)
            downloaded = download_state.downloaded
            await asyncio.sleep(PRINT_PROGRESS_DELAY)
