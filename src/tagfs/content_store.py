"""Almacén de contenido: la interfaz que usa el gateway y una versión en memoria."""

from __future__ import annotations

import hashlib
from typing import Protocol

from tagfs.model import ContentNotFound


def content_hash(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


class ContentStore(Protocol):
    def put(self, content: bytes) -> str:
        """Guarda los bytes y devuelve su sha256 en hexadecimal."""
        ...

    def get(self, content_hash: str) -> bytes:
        """Devuelve los bytes de ese hash o lanza ContentNotFound."""
        ...


class FakeContentStore:
    """Guarda el contenido en memoria hasta tener el almacén real."""

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
