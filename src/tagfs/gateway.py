"""Gateway: lo que usa cada cliente para trabajar con el sistema."""

from __future__ import annotations

import uuid
from collections import defaultdict
from typing import Any, Collection, Iterable, NamedTuple, Protocol, Sequence

import ray
from ray.actor import ActorHandle
from ray.exceptions import GetTimeoutError, RayActorError

from tagfs.content import ContentCorrupted, content_hash
from tagfs.content_store import Reconciliation
from tagfs.model import (
    BulkResult,
    FileId,
    FileInfo,
    FileMeta,
    IndexOp,
    Mutation,
    normalize_tags,
)
from tagfs.query import Expr, candidates, index_tags, matches, parse_query
from tagfs.router import shard_for
from tagfs.tag_shard import IndexCheck, check_index

# Un TagShard muerto que no se pudo reiniciar, o uno que no respondió ni después de reiniciarlo.
_INDEX_UNAVAILABLE = (RayActorError, GetTimeoutError)


class ContentAPI(Protocol):
    """Lo que el gateway necesita de un almacén de contenido."""

    def put(self, content: bytes) -> str: ...

    def get(self, content_hash: str) -> bytes: ...

    def sweep(self, live_keys: Collection[str]) -> int: ...

    def reconcile(self, live_keys: Collection[str]) -> Reconciliation: ...


def _verify(content: bytes, expected_hash: str) -> None:
    if content_hash(content) != expected_hash:
        raise ContentCorrupted(expected_hash)


def _check_name(name: str) -> None:
    if not isinstance(name, str):
        raise TypeError(f"El nombre debe ser texto, no {type(name).__name__}")


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
    cambio está hecho en cuanto lo aceptan los metadatos. El shard de metadatos lo
    apunta en su outbox hasta que el gateway confirma que llegó al índice. Si el
    cliente se cae antes, o el índice no responde, el cambio sigue apuntado y
    repair_index, desde cualquier cliente, lo entrega después.

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
        # Se valida todo antes de guardar: un error aquí no debe dejar bytes huérfanos.
        _check_name(name)
        normalized = normalize_tags(tags)
        file_id = str(uuid.uuid4())
        stored_hash = self._content.put(content)
        _verify(content, stored_hash)
        self._change(file_id, "create", name, normalized, stored_hash)
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
        self._change(file_id, "delete")

    def add_tags(self, file_id: FileId, tags: Iterable[str]) -> int:
        """Agrega etiquetas y devuelve la nueva versión del archivo."""
        return self._change(file_id, "add_tags", normalize_tags(tags))

    def remove_tags(self, file_id: FileId, tags: Iterable[str]) -> int:
        """Quita etiquetas y devuelve la nueva versión del archivo."""
        return self._change(file_id, "remove_tags", normalize_tags(tags))

    def rename(self, file_id: FileId, new_name: str) -> int:
        """Cambia el nombre y devuelve la nueva versión del archivo."""
        _check_name(new_name)
        return self._change(file_id, "rename", new_name)

    def query(self, expr: str) -> list[FileId]:
        """Devuelve, ordenados, los IDs de los archivos vivos que cumplen la consulta."""
        return sorted(self._find(expr))

    def list(self, expr: str | None = None) -> list[FileInfo]:
        """Describe, ordenados por ID, los archivos vivos que cumplen la consulta.

        Sin consulta, los describe todos. Devuelve el nombre, las etiquetas y la
        versión de cada uno sin descargar su contenido.
        """
        files = self._live_files() if expr is None else self._find(expr).values()
        found = [FileInfo(meta.file_id, meta.name, meta.tags, meta.version) for meta in files]
        return sorted(found, key=lambda info: info.file_id)

    def delete_by_query(self, expr: str) -> BulkResult:
        """Elimina los archivos vivos que cumplen la consulta.

        El índice da los candidatos y cada shard de metadatos vuelve a evaluar la
        consulta con las etiquetas actuales justo antes de cambiar el archivo: uno
        que dejó de cumplirla no se toca. Igual que query, no alcanza a un archivo
        que el índice todavía no conoce.

        Devuelve en done la versión resultante de cada archivo que cumplía la
        consulta, y en unknown los candidatos de los shards de metadatos que no
        respondieron: no se sabe si cambiaron. Lo que hicieron los demás shards no
        se deshace. Si el índice no responde, no hay error: igual que en un cambio
        de un solo archivo, los cambios quedan pendientes para repair_index.
        """
        return self._change_by_query(expr, "delete_matching")

    def add_tags_by_query(self, expr: str, tags: Iterable[str]) -> BulkResult:
        """Como delete_by_query, pero agrega etiquetas.

        done también incluye los archivos que ya las tenían, con su versión sin cambiar.
        """
        return self._change_by_query(expr, "add_tags_matching", normalize_tags(tags))

    def remove_tags_by_query(self, expr: str, tags: Iterable[str]) -> BulkResult:
        """Como add_tags_by_query, pero quita etiquetas."""
        return self._change_by_query(expr, "remove_tags_matching", normalize_tags(tags))

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

    def repair_index(self) -> int:
        """Entrega al índice los cambios que quedaron pendientes en los metadatos.

        Un cambio queda pendiente si su cliente se cayó después de que lo
        aceptaran los metadatos y antes de avisar al índice, o si el índice no
        respondió. Devuelve cuántos cambios entregó. Se puede llamar en cualquier
        momento, también con escrituras en curso: entregar dos veces un cambio
        no tiene efecto.
        """
        pending = self._resolve([self._call(shard, "pending") for shard in self._metadata])
        self._send_to_index({op for changes in pending for ops in changes.values() for op in ops})
        self._resolve(
            [
                self._call(shard, "ack", list(changes))
                for shard, changes in zip(self._metadata, pending)
                if changes
            ]
        )
        return sum(len(changes) for changes in pending)

    def verify_index(self) -> IndexCheck:
        """Compara el índice con el que se obtendría desde los metadatos.

        No cambia nada. Con escrituras en curso puede ver diferencias pasajeras.
        """
        index: dict[str, set[FileId]] = {}
        for entries in self._ask_every_tag_shard("entries"):
            index.update(entries)
        return check_index(self._live_files(), index)

    def _find(self, expr: str) -> dict[FileId, FileMeta]:
        """Devuelve las fichas de los archivos vivos que cumplen la consulta.

        El índice solo propone candidatos; la decisión final se toma con las
        etiquetas actuales de los metadatos, porque el índice puede ir atrasado.
        """
        parsed = parse_query(expr)
        files = self._read_many(self._candidates(parsed))
        return {file_id: meta for file_id, meta in files.items() if matches(parsed, meta.tags)}

    def _candidates(self, parsed: Expr) -> set[FileId]:
        """Devuelve los posibles archivos de la consulta según el índice."""
        index = self._lookup_many(index_tags(parsed))  # todas las etiquetas a la vez
        return candidates(parsed, lambda tag: index[tag])

    def _change_by_query(self, expr: str, method: str, *args: Any) -> BulkResult:
        """Pide el cambio, a la vez, a cada shard de metadatos que tiene candidatos."""
        parsed = parse_query(expr)
        by_shard = self._group_by_metadata_shard(self._candidates(parsed))
        sent = {
            index: self._call(self._metadata[index], method, ids, parsed, *args)
            for index, ids in by_shard.items()
        }
        answers = self._resolve_each(sent)
        self._deliver(answers)
        done = {
            file_id: mutation.version
            for mutations in answers.values()
            for file_id, mutation in mutations.items()
        }
        unknown = [
            file_id for index, ids in by_shard.items() if index not in answers for file_id in ids
        ]
        return BulkResult(dict(sorted(done.items())), sorted(unknown))

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
        by_shard = self._group_by_metadata_shard(file_ids)
        refs = [self._call(self._metadata[index], "get_many", ids) for index, ids in by_shard.items()]
        files: dict[FileId, FileMeta] = {}
        for answer in self._resolve(refs):
            files.update(answer)
        return files

    def _group_by_metadata_shard(self, file_ids: Iterable[FileId]) -> dict[int, list[FileId]]:
        by_shard: dict[int, list[FileId]] = defaultdict(list)
        for file_id in file_ids:
            by_shard[self._metadata_index(file_id)].append(file_id)
        return by_shard

    def _resolve_each(self, sent: dict[int, Any]) -> dict[int, Any]:
        """Espera por separado la respuesta de cada shard de metadatos.

        Devuelve solo las de los shards que respondieron, para que la caída de uno
        no oculte lo que ya hicieron los demás.
        """
        if not self._remote:
            return sent
        answers: dict[int, Any] = {}
        for index, ref in sent.items():
            try:
                answers[index] = ray.get(ref)
            except RayActorError:  # el shard murió
                continue
        return answers

    def _change(self, file_id: FileId, method: str, *args: Any) -> int:
        """Hace un cambio en los metadatos, lo lleva al índice y devuelve la versión."""
        index = self._metadata_index(file_id)
        mutation = self._resolve(self._call(self._metadata[index], method, file_id, *args))
        self._deliver({index: {file_id: mutation}})
        return mutation.version

    def _deliver(self, changes: dict[int, dict[FileId, Mutation]]) -> None:
        """Lleva cambios al índice y los saca del outbox de sus shards de metadatos.

        changes agrupa los cambios por shard de metadatos. Van todos juntos al
        índice, con una llamada por TagShard. Si el índice no responde, no se
        lanza error: los cambios ya están hechos porque los aceptaron los
        metadatos, y siguen en el outbox para repair_index. En ese caso no se
        saca ninguno, aunque algún TagShard sí los recibiera: entregar dos veces
        un cambio no tiene efecto.
        """
        ops: list[IndexOp] = []
        to_ack: dict[int, list[tuple[FileId, int]]] = defaultdict(list)
        for index, mutations in changes.items():
            for file_id, mutation in mutations.items():
                if mutation.index_ops:  # un cambio sin efecto no entró en el outbox
                    ops.extend(mutation.index_ops)
                    to_ack[index].append((file_id, mutation.version))
        if not ops:
            return
        try:
            self._send_to_index(ops)
        except _INDEX_UNAVAILABLE:
            return
        self._resolve(
            [self._call(self._metadata[index], "ack", acks) for index, acks in to_ack.items()]
        )

    def _send_to_index(self, ops: Iterable[IndexOp]) -> None:
        """Entrega los cambios al índice, con una sola llamada por TagShard y todas a la vez.

        Con una llamada por cambio, entregar miles tardaría más que index_timeout
        y el gateway reiniciaría un TagShard sano.
        """
        by_shard: dict[int, list[IndexOp]] = defaultdict(list)
        for op in ops:
            by_shard[self._tag_index(op.tag)].append(op)
        if not self._remote:
            for index, group in by_shard.items():
                self._tags[index].apply_many(group)
            return
        sent = {
            index: self._tags[index].apply_many.remote(group) for index, group in by_shard.items()
        }
        for index, ref in sent.items():
            self._wait_for_index(index, [ref])

    def _wait_for_index(self, index: int, refs: list[Any]) -> list[Any]:
        """Espera las respuestas de un TagShard; si no llegan a tiempo, lo reinicia.

        Un shard muerto y uno colgado se ven igual desde fuera. Con no_restart=False,
        Ray lo reinicia con el mismo handle, el actor nuevo se rellena desde los
        metadatos y Ray le repite las peticiones pendientes. Repetirlas es seguro:
        lookup solo lee y apply_many ignora los cambios que ya conoce.
        """
        try:
            return ray.get(refs, timeout=self._index_timeout)
        except GetTimeoutError:
            ray.kill(self._tags[index], no_restart=False)
            return ray.get(refs, timeout=self._restart_timeout)

    def _ask_every_tag_shard(self, method: str) -> list[Any]:
        """Hace la misma pregunta a todos los TagShard a la vez."""
        if not self._remote:
            return [getattr(shard, method)() for shard in self._tags]
        sent = [getattr(shard, method).remote() for shard in self._tags]
        return [self._wait_for_index(index, [ref])[0] for index, ref in enumerate(sent)]

    def _live_files(self) -> list[FileMeta]:
        answers = self._resolve([self._call(shard, "live_files") for shard in self._metadata])
        return [meta for files in answers for meta in files]

    def _call(self, target: Any, method: str, *args: Any) -> Any:
        function = getattr(target, method)
        return function.remote(*args) if self._remote else function(*args)

    def _resolve(self, result: Any) -> Any:
        return ray.get(result) if self._remote else result

    def _metadata_for(self, file_id: FileId) -> Any:
        return self._metadata[self._metadata_index(file_id)]

    def _metadata_index(self, file_id: FileId) -> int:
        return shard_for(file_id, len(self._metadata))

    def _tag_shard_for(self, tag: str) -> Any:
        return self._tags[self._tag_index(tag)]

    def _tag_index(self, tag: str) -> int:
        return shard_for(tag, len(self._tags))
