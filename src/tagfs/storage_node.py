"""Nodo de almacenamiento: guarda el contenido de los archivos por su hash."""

from __future__ import annotations

from tagfs.content import ContentCorrupted, ContentNotFound, NodeUnreachable, content_hash


class StorageNode:
    """Guarda contenido inmutable usando su hash como clave.

    disconnect y reconnect simulan una partición de red: mientras está
    desconectado, el nodo conserva sus datos pero rechaza toda petición.
    """

    def __init__(self) -> None:
        self._blobs: dict[str, bytes] = {}
        self._reachable = True

    def put(self, key: str, data: bytes) -> None:
        self._check_reachable()
        if content_hash(data) != key:
            raise ContentCorrupted(key)
        self._blobs[key] = data

    def get(self, key: str) -> bytes:
        self._check_reachable()
        if key not in self._blobs:
            raise ContentNotFound(key)
        return self._blobs[key]

    def find(self, key: str) -> bytes | None:
        """Como get, pero devuelve None si no tiene el contenido.

        Al leer de varias réplicas, que una no lo tenga es normal, no un error.
        """
        try:
            return self.get(key)
        except ContentNotFound:
            return None

    def has(self, key: str) -> bool:
        self._check_reachable()
        return key in self._blobs

    def delete(self, key: str) -> None:
        self._check_reachable()
        self._blobs.pop(key, None)

    def list_keys(self) -> list[str]:
        self._check_reachable()
        return sorted(self._blobs)

    def disconnect(self) -> None:
        self._reachable = False

    def reconnect(self) -> None:
        self._reachable = True

    def _check_reachable(self) -> None:
        if not self._reachable:
            raise NodeUnreachable("El nodo está aislado por una partición")
