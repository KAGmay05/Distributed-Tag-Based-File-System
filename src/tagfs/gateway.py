"""Gateway: punto de entrada del sistema, coordina a todos los actores."""

from __future__ import annotations

import uuid
from collections import defaultdict
from typing import Any, Iterable, NamedTuple, Protocol, Sequence

import ray
from ray.exceptions import GetTimeoutError, RayActorError

import tagfs
from tagfs.content import ContentCorrupted, content_hash
from tagfs.content_store import ContentStore, Reconciliation, RemoteStorageNode
from tagfs.metadata_shard import MetadataShard
from tagfs.model import FileId, FileMeta, IndexOp, normalize_tags
from tagfs.query import candidates, matches, parse_query
from tagfs.router import shard_for
from tagfs.tag_shard import TagShard

RemoteMetadataShard = ray.remote(MetadataShard)
RemoteTagShard = ray.remote(TagShard)


class ContentAPI(Protocol):
    """Interfaz mínima para conectar un almacén de contenido al gateway."""

    def put(self, content: bytes) -> str: ...

    def get(self, content_hash: str) -> bytes: ...


def _verify(content: bytes, expected_hash: str) -> None:
    if content_hash(content) != expected_hash:
        raise ContentCorrupted(expected_hash)


class StoredFile(NamedTuple):
    """Archivo completo tal como lo recibe el cliente."""

    name: str
    content: bytes
    tags: frozenset[str]
    version: int


def start_ray(**options: Any) -> None:
    """Arranca Ray enviando el paquete a los workers, que no conocen src/."""
    ray.init(runtime_env={"py_modules": [tagfs]}, **options)


class Gateway:
    """Coordina los componentes del sistema.

    Sin dependencias crea los actores de Ray y espera a que estén listos.
    También acepta metadata, tags y content explícitos, como objetos locales
    o adaptadores síncronos de actores, para reutilizar un clúster existente.

    Los metadatos son la fuente de verdad y el índice se deriva de ellos: un
    cambio está hecho en cuanto lo aceptan los metadatos. En el modo que crea
    sus propios actores, reemplaza un TagShard muerto o sin respuesta por uno
    nuevo reconstruido desde los metadatos.

    El arranque de actores requiere que Ray ya esté iniciado con start_ray.
    """

    def __init__(
        self,
        metadata: Sequence[Any] | None = None,
        tags: Sequence[Any] | None = None,
        content: ContentAPI | None = None,
        *,
        metadata_shards: int = 3,
        tag_shards: int = 4,
        storage_nodes: int = 4,
        replication_factor: int = 2,
        startup_timeout: float = 120.0,
        index_timeout: float = 5.0,
    ) -> None:
        supplied = (metadata is not None, tags is not None, content is not None)
        if any(supplied) and not all(supplied):
            raise ValueError("Se deben proporcionar metadata, tags y content juntos")
        self._remote = not any(supplied)
        self._startup_timeout = startup_timeout
        self._index_timeout = index_timeout
        if not self._remote:
            if not metadata or not tags:
                raise ValueError("Se necesita al menos un shard de metadatos y etiquetas")
            self._metadata = metadata
            self._tags = tags
            self._content = content
            return
        if min(metadata_shards, tag_shards, storage_nodes, replication_factor) <= 0:
            raise ValueError("Las cantidades de actores y réplicas deben ser positivas")
        if replication_factor > storage_nodes:
            raise ValueError(
                f"No caben {replication_factor} réplicas en {storage_nodes} nodos"
            )
        self._metadata = [RemoteMetadataShard.remote() for _ in range(metadata_shards)]
        self._tags = [RemoteTagShard.remote() for _ in range(tag_shards)]
        self._storage = {
            f"storage-{i}": RemoteStorageNode.remote() for i in range(storage_nodes)
        }
        self._wait_until_ready(startup_timeout)
        self._content = ContentStore(self._storage, replication_factor)

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
        found = candidates(parsed, self._lookup)
        files = self._read_many(found)
        return sorted(file_id for file_id, meta in files.items() if matches(parsed, meta.tags))

    def list(self) -> list[FileId]:
        """Devuelve, ordenados, los IDs de todos los archivos vivos."""
        answers = self._resolve([self._call(shard, "list_ids") for shard in self._metadata])
        return sorted(file_id for ids in answers for file_id in ids)

    def collect_garbage(self) -> int:
        """Borra de los almacenes el contenido que ningún archivo vivo usa.

        Devuelve cuántos contenidos distintos se borraron. No debe correr a la
        vez que un put: borraría los bytes que ese put ya guardó y cuya ficha aún
        no existe. start_gateway serializa las operaciones de sus clientes. Con
        clientes independientes, el llamador debe detener todas las escrituras.
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

    def _lookup(self, tag: str) -> set[FileId]:
        index = self._tag_index(tag)
        if not self._remote:
            return self._tags[index].lookup(tag)
        try:
            return ray.get(self._tags[index].lookup.remote(tag), timeout=self._index_timeout)
        except (RayActorError, GetTimeoutError):
            self._rebuild_tag_shard(index)
            return ray.get(self._tags[index].lookup.remote(tag), timeout=self._index_timeout)

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
        by_shard: dict[int, list[Any]] = defaultdict(list)
        for op in ops:
            index = self._tag_index(op.tag)
            by_shard[index].append(self._tags[index].apply.remote(op))
        for index, refs in by_shard.items():
            try:
                ray.get(refs, timeout=self._index_timeout)
            except (RayActorError, GetTimeoutError):
                # Los metadatos ya cambiaron, así que la reconstrucción incluye este cambio.
                self._rebuild_tag_shard(index)

    def _rebuild_tag_shard(self, index: int) -> None:
        # El estado del actor viejo se da por perdido aunque solo fuera lento:
        # se descarta y el nuevo se llena con las etiquetas actuales de cada archivo.
        ray.kill(self._tags[index])
        shard = RemoteTagShard.remote()
        refs = [
            shard.apply.remote(IndexOp(tag, meta.file_id, True, meta.version))
            for meta in self._live_files()
            for tag in meta.tags
            if self._tag_index(tag) == index
        ]
        ray.get(refs + [shard.lookup.remote("")], timeout=self._startup_timeout)
        self._tags[index] = shard

    def _live_files(self) -> list[FileMeta]:
        ids = self._resolve([self._call(shard, "list_ids") for shard in self._metadata])
        refs = [self._call(shard, "get_many", shard_ids) for shard, shard_ids in zip(self._metadata, ids)]
        return [meta for answer in self._resolve(refs) for meta in answer.values()]

    def _call(self, target: Any, method: str, *args: Any) -> Any:
        function = getattr(target, method)
        return function.remote(*args) if self._remote else function(*args)

    def _resolve(self, result: Any) -> Any:
        return ray.get(result) if self._remote else result

    def _wait_until_ready(self, timeout: float) -> None:
        # Un actor recién creado tarda en arrancar. Se espera aquí para que ese
        # retraso no se confunda después con un nodo caído.
        refs = [shard.list_ids.remote() for shard in self._metadata]
        refs += [shard.lookup.remote("") for shard in self._tags]
        refs += [node.list_keys.remote() for node in self._storage.values()]
        ray.get(refs, timeout=timeout)

    def _metadata_for(self, file_id: FileId) -> Any:
        return self._metadata[shard_for(file_id, len(self._metadata))]

    def _tag_shard_for(self, tag: str) -> Any:
        return self._tags[self._tag_index(tag)]

    def _tag_index(self, tag: str) -> int:
        return shard_for(tag, len(self._tags))


RemoteGateway = ray.remote(Gateway)


def start_gateway(**options: Any) -> Any:
    """Crea el Gateway como actor, para que varios clientes lo compartan.

    Cada cliente llama a sus métodos con .remote(). El actor atiende una
    operación a la vez, así que reconstruir el índice o recolectar basura
    nunca se cruza con otra operación.
    """
    gateway = RemoteGateway.remote(**options)
    ray.get(gateway.list.remote())  # espera a que termine de crear sus actores
    return gateway
