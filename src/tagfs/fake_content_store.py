"""Almacén de contenido en memoria, para probar el gateway sin Ray."""

from __future__ import annotations

from typing import Collection

from tagfs.content import ContentNotFound, content_hash
from tagfs.content_store import Reconciliation


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

    def sweep(self, live_keys: Collection[str]) -> int:
        """Borra el contenido que no está en live_keys y devuelve cuántos borró."""
        garbage = self._blobs.keys() - set(live_keys)
        for key in garbage:
            del self._blobs[key]
        return len(garbage)

    def reconcile(self, live_keys: Collection[str]) -> Reconciliation:
        """Sin nodos ni réplicas no hay nada que reparar: solo se recolecta basura."""
        live = set(live_keys)
        garbage = self.sweep(live)
        return Reconciliation([], 0, 0, garbage, sorted(live - self._blobs.keys()))
