from random import choice


# TODO
class ClientInfo:
    def __init__(self) -> None:
        self.peer_id: bytes = self._gen_peer_id()
        self.port = 6889
        self.compact = 1
        self.no_peer_id = 0
        self.ip_address = 0
        self.key = 1
        self.num_want = -1

    @staticmethod
    def _gen_peer_id() -> bytes:
        return b"".join(
            [choice(b"123456789").to_bytes(1, byteorder='big')
             for _ in range(20)]
        )
