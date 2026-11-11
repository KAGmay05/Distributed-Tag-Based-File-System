"""Gateway: lo que usa cada cliente para trabajar con el sistema."""

from __future__ import annotations

import uuid
from collections import defaultdict
from typing import Any, Collection, Iterable, NamedTuple, Protocol, Sequence

import ray
from ray.actor import ActorHandle
from ray.exceptions import GetTimeoutError

from tagfs.content import ContentCorrupted, content_hash
from tagfs.content_store import Reconciliation
from tagfs.model import FileId, FileMeta, IndexOp, normalize_tags
from tagfs.query import candidates, index_tags, matches, parse_query
from tagfs.router import shard_for


class ContentAPI(Protocol):
    """Lo que el gateway necesita de un almacén de contenido."""

    def put(self, content: bytes) -> str: ...

    def get(self, content_hash: str) -> bytes: ...

    def sweep(self, live_keys: Collection[str]) -> int: ...

    def reconcile(self, live_keys: Collection[str]) -> Reconciliation: ...


def _verify(content: bytes, expected_hash: str) -> None:
    if content_hash(content) != expected_hash:
        raise ContentCorrupted(expected_hash)


class StoredFile(NamedTuple):
    """Archivo completo tal como lo recibe el cliente."""

    name: str
    content: bytes
    tags: frozenset[str]
    version: int


class Gateway:
    """Lo que usa un cliente para trabajar con el sistema.

    Recibe los shards de metadatos y de etiquetas y un almacén de contenido. Los
    shards son handles de actores de Ray, como los que crea start_cluster en
    tagfs.ray_runtime, u objetos locales para probar la lógica sin Ray. Con
    actores, las peticiones independientes a varios shards se envían a la vez.
    Cada cliente tiene su propio Gateway; todos comparten los mismos actores.

    Los metadatos son la fuente de verdad y el índice se deriva de ellos: un
    cambio está hecho en cuanto lo aceptan los metadatos.

    Si un TagShard no responde a tiempo, el gateway pide a Ray que lo reinicie.
    Los TagShard de start_cluster se reinician con el mismo handle y se rellenan
    desde los metadatos, así que la reparación sirve a todos los clientes.
    """

    def __init__(
        self,
        metadata: Sequence[Any],
        tags: Sequence[Any],
        content: ContentAPI,
        *,
        index_timeout: float = 5.0,
        restart_timeout: float = 120.0,
    ) -> None:
        if not metadata or not tags:
            raise ValueError("Se necesita al menos un shard de metadatos y etiquetas")
        are_actors = [isinstance(shard, ActorHandle) for shard in (*metadata, *tags)]
        if any(are_actors) and not all(are_actors):
            raise ValueError("Los shards deben ser todos actores de Ray o todos objetos locales")
        # Con actores se les llama con .remote() y se espera después; con objetos
        # locales, directamente.
        self._remote = all(are_actors)
        self._metadata = list(metadata)
        self._tags = list(tags)
        self._content = content
        self._index_timeout = index_timeout
        self._restart_timeout = restart_timeout

    def put(self, name: str, content: bytes, tags: Iterable[str] = ()) -> FileId:
        """Crea un archivo y devuelve su ID.

        Guarda primero el contenido, luego los metadatos y al final el índice,
        para que los metadatos nunca apunten a un contenido que no existe.
        """
        normalized = normalize_tags(tags)  # una etiqueta inválida no debe dejar bytes huérfanos
        file_id = str(uuid.uuid4())
        stored_hash = self._content.put(content)
        _verify(content, stored_hash)
        shard = self._metadata_for(file_id)
        mutation = self._resolve(self._call(shard, "create", file_id, name, normalized, stored_hash))
        self._deliver(mutation.index_ops)
        return file_id

    def get(self, file_id: FileId) -> StoredFile:
        """Devuelve el archivo o lanza FileNotFound si no existe o fue eliminado."""
        meta = self._resolve(self._call(self._metadata_for(file_id), "get", file_id))
        content = self._content.get(meta.content_hash)
        _verify(content, meta.content_hash)
        return StoredFile(meta.name, content, meta.tags, meta.version)

    def delete(self, file_id: FileId) -> None:
        """Elimina el archivo y lo saca del índice.

        Los bytes no se borran aquí: otro archivo puede compartir el mismo
        contenido. Se encargará el recolector de basura.
        """
        self._commit(self._call(self._metadata_for(file_id), "delete", file_id))

    def add_tags(self, file_id: FileId, tags: Iterable[str]) -> int:
        """Agrega etiquetas y devuelve la nueva versión del archivo."""
        return self._commit(self._call(self._metadata_for(file_id), "add_tags", file_id, tuple(tags)))

    def remove_tags(self, file_id: FileId, tags: Iterable[str]) -> int:
        """Quita etiquetas y devuelve la nueva versión del archivo."""
        return self._commit(
            self._call(self._metadata_for(file_id), "remove_tags", file_id, tuple(tags))
        )

    def rename(self, file_id: FileId, new_name: str) -> int:
        """Cambia el nombre y devuelve la nueva versión del archivo."""
        return self._commit(self._call(self._metadata_for(file_id), "rename", file_id, new_name))

    def query(self, expr: str) -> list[FileId]:
        """Devuelve, ordenados, los archivos vivos que cumplen la consulta.

        El índice solo propone candidatos; la decisión final se toma con las
        etiquetas actuales de los metadatos, porque el índice puede ir atrasado.
        """
        parsed = parse_query(expr)
        index = self._lookup_many(index_tags(parsed))  # todas las etiquetas a la vez
        found = candidates(parsed, lambda tag: index[tag])
        files = self._read_many(found)
        return sorted(file_id for file_id, meta in files.items() if matches(parsed, meta.tags))

    def list(self) -> list[FileId]:
        """Devuelve, ordenados, los IDs de todos los archivos vivos."""
        answers = self._resolve([self._call(shard, "list_ids") for shard in self._metadata])
        return sorted(file_id for ids in answers for file_id in ids)

    def collect_garbage(self) -> int:
        """Borra de los almacenes el contenido que ningún archivo vivo usa.

        Devuelve cuántos contenidos distintos se borraron. No debe correr a la
        vez que un put de ningún cliente: borraría los bytes que ese put ya
        guardó y cuya ficha aún no existe. El llamador debe detener antes las
        escrituras de todos los clientes.
        """
        live = {meta.content_hash for meta in self._live_files()}
        return self._content.sweep(live)

    def reconcile(self) -> Reconciliation:
        """Repara los almacenes después de fallos o particiones.

        Readmite a los almacenes que vuelven a responder, deja cada contenido
        vivo en las réplicas que indica el anillo y borra lo que nadie usa.
        Tiene la misma condición que collect_garbage: no debe cruzarse con un put.
        """
        live = {meta.content_hash for meta in self._live_files()}
        return self._content.reconcile(live)

    def _lookup_many(self, tags: Iterable[str]) -> dict[str, set[FileId]]:
        """Devuelve los archivos de cada etiqueta, pidiéndolas todas antes de esperar."""
        if not self._remote:
            return {tag: self._tag_shard_for(tag).lookup(tag) for tag in tags}
        by_shard: dict[int, list[str]] = defaultdict(list)
        for tag in tags:
            by_shard[self._tag_index(tag)].append(tag)
        sent = {
            index: [self._tags[index].lookup.remote(tag) for tag in group]
            for index, group in by_shard.items()
        }
        found: dict[str, set[FileId]] = {}
        for index, group in by_shard.items():
            found.update(zip(group, self._wait_for_index(index, sent[index])))
        return found

    def _read_many(self, file_ids: Iterable[FileId]) -> dict[FileId, FileMeta]:
        # Una sola llamada por shard, y todas a la vez, en lugar de una por archivo.
        by_shard: dict[int, list[FileId]] = defaultdict(list)
        for file_id in file_ids:
            by_shard[shard_for(file_id, len(self._metadata))].append(file_id)
        refs = [self._call(self._metadata[index], "get_many", ids) for index, ids in by_shard.items()]
        files: dict[FileId, FileMeta] = {}
        for answer in self._resolve(refs):
            files.update(answer)
        return files

    def _commit(self, mutation_ref: Any) -> int:
        # Espera el cambio de metadatos y lleva al índice lo que haya cambiado.
        mutation = self._resolve(mutation_ref)
        self._deliver(mutation.index_ops)
        return mutation.version

    def _deliver(self, ops: Iterable[IndexOp]) -> None:
        if not self._remote:
            for op in ops:
                self._tag_shard_for(op.tag).apply(op)
            return
        sent: dict[int, list[Any]] = defaultdict(list)
        for op in ops:
            index = self._tag_index(op.tag)
            sent[index].append(self._tags[index].apply.remote(op))
        for index, refs in sent.items():
            self._wait_for_index(index, refs)

    def _wait_for_index(self, index: int, refs: list[Any]) -> list[Any]:
        """Espera las respuestas de un TagShard; si no llegan a tiempo, lo reinicia.

        Un shard muerto y uno colgado se ven igual desde fuera. Con no_restart=False,
        Ray lo reinicia con el mismo handle, el actor nuevo se rellena desde los
        metadatos y Ray le repite las peticiones pendientes. Repetirlas es seguro:
        lookup solo lee y apply ignora un cambio que ya conoce.
        """
        try:
            return ray.get(refs, timeout=self._index_timeout)
        except GetTimeoutError:
            ray.kill(self._tags[index], no_restart=False)
            return ray.get(refs, timeout=self._restart_timeout)

    def _live_files(self) -> list[FileMeta]:
        ids = self._resolve([self._call(shard, "list_ids") for shard in self._metadata])
        refs = [self._call(shard, "get_many", shard_ids) for shard, shard_ids in zip(self._metadata, ids)]
        return [meta for answer in self._resolve(refs) for meta in answer.values()]

    def _call(self, target: Any, method: str, *args: Any) -> Any:
        function = getattr(target, method)
        return function.remote(*args) if self._remote else function(*args)

    def _resolve(self, result: Any) -> Any:
        return ray.get(result) if self._remote else result

    def _metadata_for(self, file_id: FileId) -> Any:
        return self._metadata[shard_for(file_id, len(self._metadata))]

    def _tag_shard_for(self, tag: str) -> Any:
        return self._tags[self._tag_index(tag)]

    def _tag_index(self, tag: str) -> int:
        return shard_for(tag, len(self._tags))
