"""Gateway: implementa la API pública llamando a cada parte en el orden correcto."""

from __future__ import annotations

from collections import defaultdict
from typing import Iterable, Sequence
from uuid import uuid4

from tagfs.content_store import ContentStore, content_hash
from tagfs.metadata_shard import MetadataShard
from tagfs.model import ContentIntegrityError, FileId, FileMeta, IndexOp, normalize_tags
from tagfs.query import candidates, matches, parse_query
from tagfs.router import shard_for
from tagfs.tag_shard import TagShard


def _verify(content: bytes, expected_hash: str) -> None:
    if content_hash(content) != expected_hash:
        raise ContentIntegrityError(expected_hash)


class Gateway:
    """Conecta el almacén, los MetadataShard y los TagShard, que no se conocen entre sí.

    No guarda estado, así que cada cliente puede tener el suyo. Le da igual si
    recibe objetos en memoria o actores, mientras tengan la misma interfaz.
    """

    def __init__(
        self,
        metadata: Sequence[MetadataShard],
        tags: Sequence[TagShard],
        content: ContentStore,
    ) -> None:
        self._metadata = metadata
        self._tags = tags
        self._content = content

    def put(self, name: str, content: bytes, tags: Iterable[str]) -> FileId:
        normalized = normalize_tags(tags)  # una etiqueta inválida falla antes de guardar nada
        file_id = uuid4().hex

        stored_hash = self._content.put(content)
        _verify(content, stored_hash)
        _, index_ops = self._metadata_for(file_id).create(file_id, name, normalized, stored_hash)
        self._deliver(index_ops)

        return file_id

    def get(self, file_id: FileId) -> tuple[str, bytes, set[str], int]:
        meta = self._metadata_for(file_id).get(file_id)
        content = self._content.get(meta.content_hash)
        _verify(content, meta.content_hash)
        return meta.name, content, set(meta.tags), meta.version

    def delete(self, file_id: FileId) -> None:
        _, index_ops = self._metadata_for(file_id).delete(file_id)
        self._deliver(index_ops)

    def add_tags(self, file_id: FileId, tags: Iterable[str]) -> int:
        version, index_ops = self._metadata_for(file_id).add_tags(file_id, tags)
        self._deliver(index_ops)
        return version

    def remove_tags(self, file_id: FileId, tags: Iterable[str]) -> int:
        version, index_ops = self._metadata_for(file_id).remove_tags(file_id, tags)
        self._deliver(index_ops)
        return version

    def rename(self, file_id: FileId, new_name: str) -> int:
        version, _ = self._metadata_for(file_id).rename(file_id, new_name)
        return version

    def query(self, expr: str) -> list[FileId]:
        parsed = parse_query(expr)
        found = candidates(parsed, self._lookup)
        files = self._read_many(found)
        return sorted(file_id for file_id, meta in files.items() if matches(parsed, meta.tags))

    def list(self) -> list[FileId]:
        return sorted(file_id for shard in self._metadata for file_id in shard.list_ids())

    def _metadata_for(self, file_id: FileId) -> MetadataShard:
        return self._metadata[shard_for(file_id, len(self._metadata))]

    def _tag_shard_for(self, tag: str) -> TagShard:
        return self._tags[shard_for(tag, len(self._tags))]

    def _deliver(self, index_ops: Iterable[IndexOp]) -> None:
        for op in index_ops:
            self._tag_shard_for(op.tag).apply(op)

    def _lookup(self, tag: str) -> set[FileId]:
        return self._tag_shard_for(tag).lookup(tag)

    def _read_many(self, file_ids: Iterable[FileId]) -> dict[FileId, FileMeta]:
        by_shard: dict[int, list[FileId]] = defaultdict(list)
        for file_id in file_ids:
            by_shard[shard_for(file_id, len(self._metadata))].append(file_id)

        files: dict[FileId, FileMeta] = {}
        for index, ids in by_shard.items():
            files.update(self._metadata[index].get_many(ids))
        return files
