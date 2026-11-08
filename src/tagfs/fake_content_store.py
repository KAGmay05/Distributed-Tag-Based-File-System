"""Almacén de contenido en memoria, para probar el gateway sin Ray."""

from __future__ import annotations

from tagfs.content import ContentNotFound, content_hash


class FakeContentStore:
    """Misma interfaz que ContentStore, pero sin réplicas ni actores."""

    def __init__(self) -> None:
        self._blobs: dict[str, bytes] = {}

    def put(self, content: bytes) -> str:
        key = content_hash(content)
        self._blobs[key] = content
        return key

    def get(self, content_hash: str) -> bytes:
        try:
            return self._blobs[content_hash]
        except KeyError:
            raise ContentNotFound(content_hash) from None
