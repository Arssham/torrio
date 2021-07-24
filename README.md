# Torrio
[![Build Status](https://www.travis-ci.com/Arssham/torrio.svg?branch=master)](https://www.travis-ci.com/Arssham/torrio)

BitTorrent client

## Usage
You can download torrent by simply passing \<path\> to .torrent file\
```poetry run python -m torrio --file <path>```

If you want to monitor download progress\
```poetry run python -m torrio --file <path> --progress```

## Supported BEPs
- [The BitTorrent Protocol Specification][bep_0003]
- [UDP Tracker Protocol for BitTorrent][bep_0015]

[bep_0003]: https://www.bittorrent.org/beps/bep_0003.html
[bep_0015]: https://www.bittorrent.org/beps/bep_0015.html