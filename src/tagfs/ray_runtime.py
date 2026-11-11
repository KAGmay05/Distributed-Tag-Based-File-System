"""Monta el sistema sobre Ray: el único sitio que arranca Ray y crea actores.

Las clases de lógica (MetadataShard, TagShard, StorageNode) no importan Ray.
start_cluster crea sus actores una sola vez; cada cliente usa cluster.gateway()
y habla directamente con ellos. content_store.py reparte los bytes entre los
actores StorageNode.
"""

from __future__ import annotations

import os
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Sequence

import ray

import tagfs
from tagfs.content_store import ContentStore
from tagfs.gateway import Gateway
from tagfs.metadata_shard import MetadataShard
from tagfs.model import FileMeta, IndexOp
from tagfs.router import shard_for
from tagfs.storage_node import StorageNode
from tagfs.tag_shard import TagShard

# Tamaño del sistema por defecto.
METADATA_SHARDS = 4
TAG_SHARDS = 4
STORAGE_NODES = 4
REPLICATION_FACTOR = 2

WORKER_START_TIMEOUT_SECONDS = 120


def start_ray(**options: Any) -> None:
    """Arranca Ray enviando el paquete a los workers, que no conocen src/."""
    ray.init(runtime_env={"py_modules": [tagfs]}, **options)


class _ProcessInfo:
    def pid(self) -> int:
        return os.getpid()


# Los MetadataShard y los StorageNode no se reinician: un actor nuevo empezaría
# vacío, y un MetadataShard vacío diría que sus archivos no existen en lugar de
# avisar del fallo.
@ray.remote
class _MetadataShardActor(_ProcessInfo, MetadataShard):
    pass


def _live_files(metadata: Sequence[Any]) -> list[FileMeta]:
    """Lee a la vez las fichas vivas de todos los shards de metadatos."""
    ids = ray.get([shard.list_ids.remote() for shard in metadata])
    answers = ray.get([shard.get_many.remote(found) for shard, found in zip(metadata, ids)])
    return [meta for answer in answers for meta in answer.values()]


# Un TagShard sí se reinicia, porque su estado se puede reconstruir desde los
# metadatos. max_restarts=-1: Ray lo arranca de nuevo, con el mismo handle, cada
# vez que muere su proceso. max_task_retries=-1: las peticiones que estaban en
# curso se repiten en el actor nuevo, lo que es seguro porque lookup solo lee y
# apply ignora un cambio que ya conoce.
@ray.remote(max_restarts=-1, max_task_retries=-1)
class _TagShardActor(_ProcessInfo, TagShard):
    """TagShard que se rellena desde los metadatos cada vez que arranca."""

    def __init__(self, index: int, shard_count: int, metadata_names: Sequence[str]) -> None:
        super().__init__()
        # Recibe nombres y no handles. Para reiniciar el actor, Ray repite __init__
        # con los mismos argumentos; unos handles irían al almacén de objetos, de
        # donde se pueden perder, y entonces el reinicio fallaría.
        metadata = [ray.get_actor(name) for name in metadata_names]
        # La primera vez los metadatos están vacíos. Tras un reinicio, Ray vuelve
        # a ejecutar __init__ y así se recupera el índice desde la fuente de verdad.
        for meta in _live_files(metadata):
            for tag in meta.tags:
                if shard_for(tag, shard_count) == index:
                    self.apply(IndexOp(tag, meta.file_id, True, meta.version))


@ray.remote
class _StorageNodeActor(_ProcessInfo, StorageNode):
    pass


class ActorProxy:
    """Permite usar un actor como si fuera un objeto normal.

    Cada llamada espera su respuesta. Es cómodo para demos y tests, pero quien
    necesite hablar con varios actores a la vez debe usar handle.
    """

    def __init__(self, actor: Any) -> None:
        self._actor = actor

    @property
    def handle(self) -> Any:
        """El handle de Ray del actor, para enviar peticiones sin esperar."""
        return self._actor

    def __getattr__(self, method: str) -> Callable[..., Any]:
        # Al deserializar, pickle busca atributos como __setstate__ cuando _actor
        # aún no existe: sin este filtro, acceder a _actor se llamaría a sí mismo.
        if method.startswith("_"):
            raise AttributeError(method)
        remote_method = getattr(self._actor, method)
        return lambda *args, **kwargs: ray.get(remote_method.remote(*args, **kwargs))


@dataclass(frozen=True)
class RayCluster:
    """Referencias a los actores; se puede enviar a otros workers."""

    metadata: tuple[ActorProxy, ...]
    tags: tuple[ActorProxy, ...]
    # Handles directos, sin ActorProxy: ContentStore llama a varios nodos a la vez.
    storage: dict[str, Any]
    replication_factor: int = REPLICATION_FACTOR

    def content_store(self) -> ContentStore:
        # Cada cliente crea el suyo, que habla directamente con los StorageNode.
        # Un ContentStore compartido en un actor sería un cuello de botella y, si ese
        # actor muriera, nadie podría leer ni escribir contenido.
        return ContentStore(self.storage, self.replication_factor)

    def gateway(self, **options: Any) -> Gateway:
        """Crea el gateway de un cliente; options se pasan a Gateway (timeouts)."""
        # Con los handles, sin ActorProxy, el gateway envía a la vez las peticiones
        # a varios shards en lugar de esperar cada respuesta antes de la siguiente.
        return Gateway(
            [proxy.handle for proxy in self.metadata],
            [proxy.handle for proxy in self.tags],
            self.content_store(),
            **options,
        )


def start_cluster(
    metadata_shards: int = METADATA_SHARDS,
    tag_shards: int = TAG_SHARDS,
    storage_nodes: int = STORAGE_NODES,
    replication_factor: int = REPLICATION_FACTOR,
) -> RayCluster:
    """Arranca Ray si hace falta y crea los actores del sistema.

    Los actores viven mientras se conserve el RayCluster devuelto.
    """
    if min(metadata_shards, tag_shards, storage_nodes, replication_factor) <= 0:
        raise ValueError("Las cantidades de actores y réplicas deben ser positivas")
    if replication_factor > storage_nodes:
        raise ValueError("El factor de replicación no puede superar los nodos")
    if not ray.is_initialized():
        start_ray(include_dashboard=False, logging_level="ERROR")
    # Los TagShard buscan los shards de metadatos por nombre. El prefijo distingue
    # los actores de cada clúster cuando hay varios en la misma sesión de Ray.
    prefix = f"tagfs-{uuid.uuid4().hex}"
    metadata_names = [f"{prefix}-metadata-{i}" for i in range(metadata_shards)]
    metadata = [_MetadataShardActor.options(name=name).remote() for name in metadata_names]
    tags = [_TagShardActor.remote(index, tag_shards, metadata_names) for index in range(tag_shards)]
    storage = {f"storage-{i}": _StorageNodeActor.remote() for i in range(storage_nodes)}
    # Un actor recién creado tarda en arrancar. Se espera aquí a que todos respondan
    # para que ese retraso no se confunda después con un actor caído.
    ray.get(
        [actor.pid.remote() for actor in (*metadata, *tags, *storage.values())],
        timeout=WORKER_START_TIMEOUT_SECONDS,
    )
    return RayCluster(
        metadata=tuple(ActorProxy(actor) for actor in metadata),
        tags=tuple(ActorProxy(actor) for actor in tags),
        storage=storage,
        replication_factor=replication_factor,
    )


def stop_cluster() -> None:
    ray.shutdown()


@ray.remote
class _StartBarrier:
    """Hace que varios workers empiecen a la vez, aunque Ray tarde en arrancar alguno."""

    def __init__(self, parties: int) -> None:
        self._parties = parties
        self._arrived = 0

    def arrive(self) -> None:
        self._arrived += 1

    def is_open(self) -> bool:
        return self._arrived >= self._parties


# num_cpus=0: quien espera en la barrera no debe ocupar el CPU que otro necesita para arrancar.
# max_retries=0: por defecto Ray repite una task cuyo worker muere, y repetir un
# cliente repetiría sus put, cada uno con un ID nuevo. Mejor que el fallo se vea.
@ray.remote(num_cpus=0, max_retries=0)
def _run_after_barrier(barrier: Any, function: Callable[..., Any], args: tuple[Any, ...]) -> Any:
    ray.get(barrier.arrive.remote())
    deadline = time.monotonic() + WORKER_START_TIMEOUT_SECONDS
    while not ray.get(barrier.is_open.remote()):
        if time.monotonic() > deadline:
            raise TimeoutError("Algún worker no llegó a arrancar")
        time.sleep(0.001)
    return function(*args)


def run_in_workers(function: Callable[..., Any], calls: Iterable[tuple[Any, ...]]) -> list[Any]:
    """Ejecuta cada llamada en su propio worker, todas a la vez, y devuelve los resultados en orden."""
    calls = list(calls)
    barrier = _StartBarrier.remote(len(calls))
    return ray.get([_run_after_barrier.remote(barrier, function, args) for args in calls])
