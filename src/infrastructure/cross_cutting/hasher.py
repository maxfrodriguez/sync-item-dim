from hashlib import blake2b
from typing import Any
from deepdiff import DeepHash


def _7byte_hash(s: str) -> int:
    return int.from_bytes(blake2b(s.encode(), digest_size=7).digest(), "little")


def deep_hash(value: Any) -> Any:
    return DeepHash(value, hasher=_7byte_hash)[value]