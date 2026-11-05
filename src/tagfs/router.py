"""Reparto de claves entre shards."""

from __future__ import annotations

import hashlib


def shard_for(key: str, n: int) -> int:
    """Devuelve el shard de una clave.

    """
    if n <= 0:
        raise ValueError(f"El número de shards debe ser positivo: {n}")
    digest = hashlib.sha256(key.encode("utf-8")).digest()
    return int.from_bytes(digest, "big") % n
