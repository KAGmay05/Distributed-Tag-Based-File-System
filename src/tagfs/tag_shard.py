"""Shard de etiquetas: índice derivado de los metadatos."""

from __future__ import annotations

from typing import Iterable, Mapping, NamedTuple

from tagfs.model import FileId, FileMeta, IndexOp


class _Entry(NamedTuple):
    version: int
    present: bool


class IndexCheck(NamedTuple):
    """Diferencias entre el índice y lo que dicen los metadatos."""

    # Pares (etiqueta, archivo). Uno que falta oculta el archivo en las consultas.
    missing: list[tuple[str, FileId]]
    # Uno que sobra no cambia los resultados: query comprueba los candidatos con los metadatos.
    extra: list[tuple[str, FileId]]


def check_index(files: Iterable[FileMeta], index: Mapping[str, Iterable[FileId]]) -> IndexCheck:
    """Compara un índice con el que se obtendría de las fichas vivas."""
    expected = {(tag, meta.file_id) for meta in files for tag in meta.tags}
    actual = {(tag, file_id) for tag, file_ids in index.items() for file_id in file_ids}
    return IndexCheck(sorted(expected - actual), sorted(actual - expected))


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

    def entries(self) -> dict[str, set[FileId]]:
        """Devuelve los archivos de cada etiqueta, para comparar el índice entero."""
        return {tag: found for tag in self._index if (found := self.lookup(tag))}
