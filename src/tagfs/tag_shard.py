"""Shard de etiquetas: índice derivado de los metadatos."""

from __future__ import annotations

from typing import NamedTuple

from tagfs.model import FileId, IndexOp


class _Entry(NamedTuple):
    version: int
    present: bool


class TagShard:
    """Relaciona cada etiqueta con sus archivos.

    Las entradas quitadas se conservan con su versión: así un cambio antiguo
    que llegue tarde no puede revivirlas.
    """

    def __init__(self) -> None:
        self._index: dict[str, dict[FileId, _Entry]] = {}

    def apply(self, op: IndexOp) -> None:
        """Aplica el cambio solo si es más nuevo que lo que ya se conoce."""
        entries = self._index.setdefault(op.tag, {})
        current = entries.get(op.file_id)
        if current is None or op.version > current.version:
            entries[op.file_id] = _Entry(op.version, op.present)

    def lookup(self, tag: str) -> set[FileId]:
        """Devuelve los archivos de una etiqueta ya normalizada."""
        entries = self._index.get(tag, {})
        return {file_id for file_id, entry in entries.items() if entry.present}
