from hashlib import blake2b


def _7byte_hash(s: str) -> int:
    return int.from_bytes(blake2b(s.encode(), digest_size=7).digest(), "little")
