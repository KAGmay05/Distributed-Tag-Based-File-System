"""Shard de metadatos: la fuente de verdad de cada archivo."""

from __future__ import annotations

from dataclasses import replace
from typing import Any, Iterable

from tagfs.model import (
    FileId,
    FileIdConflict,
    FileMeta,
    FileNotFound,
    IndexOp,
    Mutation,
    normalize_tags,
)


def _index_ops(
    file_id: FileId, tags: Iterable[str], present: bool, version: int
) -> tuple[IndexOp, ...]:
    # Se ordena porque el orden de un set de strings cambia entre procesos.
    return tuple(IndexOp(tag, file_id, present, version) for tag in sorted(tags))


class MetadataShard:
    """Guarda los metadatos de los archivos que le corresponden.

    Cada cambio devuelve los IndexOp que genera, pero no los envía: el shard no
    conoce a los TagShard. Un cambio sin efecto no sube la versión ni genera
    IndexOp, de modo que se puede reintentar sin riesgo.
    """

    def __init__(self) -> None:
        self._files: dict[FileId, FileMeta] = {}

    def create(
        self, file_id: FileId, name: str, tags: Iterable[str], content_hash: str
    ) -> Mutation:
        meta = FileMeta(file_id, name, normalize_tags(tags), content_hash)
        existing = self._files.get(file_id)
        if existing == meta:
            return Mutation(existing.version, ())
        if existing is not None:
            raise FileIdConflict(file_id)
        self._files[file_id] = meta
        return Mutation(meta.version, _index_ops(file_id, meta.tags, True, meta.version))

    def get(self, file_id: FileId) -> FileMeta:
        return self._require_live(file_id)

    def get_many(self, file_ids: Iterable[FileId]) -> dict[FileId, FileMeta]:
        found = (self._files.get(file_id) for file_id in file_ids)
        return {meta.file_id: meta for meta in found if meta and not meta.deleted}

    def list_ids(self) -> list[FileId]:
        return sorted(meta.file_id for meta in self._files.values() if not meta.deleted)

    def add_tags(self, file_id: FileId, tags: Iterable[str]) -> Mutation:
        meta = self._require_live(file_id)
        added = normalize_tags(tags) - meta.tags
        if not added:
            return Mutation(meta.version, ())
        updated = self._commit(meta, tags=meta.tags | added)
        return Mutation(updated.version, _index_ops(file_id, added, True, updated.version))

    def remove_tags(self, file_id: FileId, tags: Iterable[str]) -> Mutation:
        meta = self._require_live(file_id)
        removed = normalize_tags(tags) & meta.tags
        if not removed:
            return Mutation(meta.version, ())
        updated = self._commit(meta, tags=meta.tags - removed)
        return Mutation(updated.version, _index_ops(file_id, removed, False, updated.version))

    def rename(self, file_id: FileId, new_name: str) -> Mutation:
        meta = self._require_live(file_id)
        if new_name == meta.name:
            return Mutation(meta.version, ())
        updated = self._commit(meta, name=new_name)
        return Mutation(updated.version, ())

    def delete(self, file_id: FileId) -> Mutation:
        meta = self._files.get(file_id)
        if meta is None:
            raise FileNotFound(file_id)
        if meta.deleted:
            return Mutation(meta.version, ())
        updated = self._commit(meta, tags=frozenset(), deleted=True)
        return Mutation(updated.version, _index_ops(file_id, meta.tags, False, updated.version))

    def _require_live(self, file_id: FileId) -> FileMeta:
        meta = self._files.get(file_id)
        if meta is None or meta.deleted:
            raise FileNotFound(file_id)
        return meta

    def _commit(self, meta: FileMeta, **changes: Any) -> FileMeta:
        updated = replace(meta, version=meta.version + 1, **changes)
        self._files[meta.file_id] = updated
        return updated
