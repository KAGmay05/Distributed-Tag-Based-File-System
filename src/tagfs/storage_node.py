"""Nodo de almacenamiento: guarda el contenido de los archivos por su hash."""

from __future__ import annotations

from tagfs.content import ContentCorrupted, ContentNotFound, content_hash


class StorageNode:
    """Guarda contenido inmutable usando su hash como clave."""

    def __init__(self) -> None:
        self._blobs: dict[str, bytes] = {}

    def put(self, key: str, data: bytes) -> None:
        if content_hash(data) != key:
            raise ContentCorrupted(key)
        self._blobs[key] = data

    def get(self, key: str) -> bytes:
        if key not in self._blobs:
            raise ContentNotFound(key)
        return self._blobs[key]

    def has(self, key: str) -> bool:
        return key in self._blobs

    def delete(self, key: str) -> None:
        self._blobs.pop(key, None)

    def list_keys(self) -> list[str]:
        return sorted(self._blobs)
