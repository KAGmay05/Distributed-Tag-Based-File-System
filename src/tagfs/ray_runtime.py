"""Monta el sistema sobre Ray.

Las clases de lógica (MetadataShard, TagShard, StorageNode) no importan Ray.
Ray aparece solo en dos adaptadores: este archivo, que convierte las clases en
actores, y content_store.py, que reparte el contenido entre los StorageNode.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any, Callable, Iterable

import ray

import tagfs
from tagfs.content_store import ContentStore
from tagfs.gateway import Gateway
from tagfs.metadata_shard import MetadataShard
from tagfs.storage_node import StorageNode
from tagfs.tag_shard import TagShard

METADATA_SHARDS = 4
TAG_SHARDS = 4
STORAGE_NODES = 3
REPLICATION_FACTOR = 2
WORKER_START_TIMEOUT_SECONDS = 120


class _ProcessInfo:
    def pid(self) -> int:
        return os.getpid()


@ray.remote
class _MetadataShardActor(_ProcessInfo, MetadataShard):
    pass


@ray.remote
class _TagShardActor(_ProcessInfo, TagShard):
    pass


@ray.remote
class _StorageNodeActor(_ProcessInfo, StorageNode):
    pass


class ActorProxy:
    """Permite usar un actor como si fuera un objeto normal."""

    def __init__(self, actor: Any) -> None:
        self._actor = actor

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

    def gateway(self) -> Gateway:
        return Gateway(self.metadata, self.tags, self.content_store())


def start_cluster(
    metadata_shards: int = METADATA_SHARDS,
    tag_shards: int = TAG_SHARDS,
    storage_nodes: int = STORAGE_NODES,
    replication_factor: int = REPLICATION_FACTOR,
) -> RayCluster:
    """Crea los actores. Viven mientras se conserve el RayCluster devuelto."""
    if not ray.is_initialized():
        # py_modules envía el paquete a los workers: así no dependen de que tagfs
        # esté instalado en su máquina.
        ray.init(
            include_dashboard=False,
            logging_level="ERROR",
            runtime_env={"py_modules": [tagfs]},
        )
    storage = {f"storage-{i}": _StorageNodeActor.remote() for i in range(storage_nodes)}
    # ContentStore descarta un nodo que no responde dentro de su timeout. Un actor
    # recién creado puede tardar en arrancar, así que se espera a que todos respondan.
    ray.get([node.pid.remote() for node in storage.values()])
    return RayCluster(
        metadata=tuple(ActorProxy(_MetadataShardActor.remote()) for _ in range(metadata_shards)),
        tags=tuple(ActorProxy(_TagShardActor.remote()) for _ in range(tag_shards)),
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
@ray.remote(num_cpus=0)
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
