"""Replicación: guarda cada contenido en varios StorageNode y lee del más rápido."""

from __future__ import annotations

from typing import Any, Mapping

import ray
from ray.exceptions import GetTimeoutError, RayActorError

from tagfs.content import ( ContentCorrupted, ContentNotFound,  StorageUnavailable, content_hash,)
from tagfs.ring import HashRing
from tagfs.storage_node import StorageNode

RemoteStorageNode = ray.remote(StorageNode)


class ContentStore:
    """Reparte el contenido entre actores StorageNode usando el anillo.
    Un nodo que muere o no responde a una escritura sale del anillo. Sus copias
    no se regeneran: el contenido queda con menos réplicas de las pedidas.
    """

    def __init__( self, nodes: Mapping[str, Any],  replication_factor: int = 2, timeout: float = 5.0, ) -> None:
        if replication_factor <= 0:
            raise ValueError(f"El factor de replicación debe ser positivo: {replication_factor}")
        self._nodes = dict(nodes)
        self._replication_factor = replication_factor
        self._timeout = timeout
        self._ring = HashRing()
        for name in self._nodes:
            self._ring.add_node(name)

    def replicas(self, key: str) -> list[str]:
        """Devuelve los nombres de los nodos que deben guardar la clave."""
        return self._ring.nodes_for(key, self._replication_factor)

    def put(self, data: bytes) -> str:
        """Guarda el contenido y devuelve su hash.

        Espera la confirmación de cada réplica. Si un nodo falla, escribe en el
        siguiente del anillo. Solo falla si ningún nodo pudo guardar el contenido.
        """
        key = content_hash(data)
        data_ref = ray.put(data)  # una sola copia aunque se envíe a varios nodos
        stored: set[str] = set()
        while len(stored) < self._replication_factor:
            targets = [name for name in self.replicas(key) if name not in stored]
            if not targets:
                break  # hay menos nodos vivos que réplicas pedidas
            refs = {self._nodes[name].put.remote(key, data_ref): name for name in targets}
            for ref, name in refs.items():
                try:
                    ray.get(ref, timeout=self._timeout)
                except (RayActorError, GetTimeoutError):
                    self._forget(name)
                else:
                    stored.add(name)
        if not stored:
            raise StorageUnavailable(key)
        return key

    def get(self, key: str) -> bytes:
        """Devuelve el contenido de la primera réplica que responda bien.

        Si las réplicas esperadas no lo tienen, busca en el resto de los nodos:
        pudo guardarse en otros mientras esas réplicas estaban caídas.
        """
        ordered = self._ring.nodes_for(key, max(len(self._nodes), 1))
        expected = ordered[: self._replication_factor]
        others = ordered[self._replication_factor :]
        corrupted = False
        for names in (expected, others):
            data, found_corrupted = self._read_first_valid(key, names)
            if data is not None:
                return data
            corrupted = corrupted or found_corrupted
        if corrupted:
            raise ContentCorrupted(key)
        raise ContentNotFound(key)

    def _read_first_valid(self, key: str, names: list[str]) -> tuple[bytes | None, bool]:
        """Pide la clave a varios nodos; devuelve los bytes válidos y si hubo daños."""
        refs = {self._nodes[name].get.remote(key): name for name in names}
        pending = list(refs)
        corrupted = False
        while pending:
            ready, pending = ray.wait(pending, num_returns=1, timeout=self._timeout)
            if not ready:
                break  # ninguno de los nodos restantes respondió a tiempo
            try:
                data = ray.get(ready[0])
            except ContentNotFound:
                continue
            except RayActorError:
                self._forget(refs[ready[0]])
                continue
            if content_hash(data) == key:
                return data, corrupted
            corrupted = True
        return None, corrupted

    def _forget(self, name: str) -> None:
        self._ring.remove_node(name)
        self._nodes.pop(name, None)
