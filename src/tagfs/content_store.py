"""Replicación: guarda cada contenido en varios StorageNode y lee del más rápido."""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Collection, Mapping, NamedTuple

import ray
from ray.exceptions import GetTimeoutError, RayActorError

from tagfs.content import ( ContentCorrupted, ContentNotFound, NodeUnreachable, StorageUnavailable, content_hash,)
from tagfs.ring import HashRing
from tagfs.storage_node import StorageNode

RemoteStorageNode = ray.remote(StorageNode)

# Un nodo muerto, lento o aislado por una partición se ve igual desde fuera.
_UNREACHABLE = (RayActorError, GetTimeoutError, NodeUnreachable)


class Reconciliation(NamedTuple):
    """Resumen de lo que hizo ContentStore.reconcile."""

    rejoined: list[str]  # nodos que volvieron a entrar al anillo
    copied: int  # copias escritas donde faltaban
    removed: int  # copias sobrantes borradas de nodos que no les tocaba
    garbage: int  # contenidos que ningún archivo vivo usa, borrados
    unavailable: list[str]  # contenidos vivos que ningún nodo alcanzable tiene


class ContentStore:
    """Reparte el contenido entre actores StorageNode usando el anillo.

    Un nodo que muere o no responde sale del anillo, pero se recuerda: si
    vuelve a responder, reconcile lo readmite y repara las copias.
    """

    def __init__( self, nodes: Mapping[str, Any],  replication_factor: int = 2, timeout: float = 5.0, ) -> None:
        if replication_factor <= 0:
            raise ValueError(f"El factor de replicación debe ser positivo: {replication_factor}")
        self._known = dict(nodes)  # todos los nodos, también los que salieron del anillo
        self._nodes = dict(nodes)  # solo los que están en el anillo
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
            stored |= self._wait_each({self._nodes[name].put.remote(key, data_ref): name for name in targets})
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

    def sweep(self, live_keys: Collection[str]) -> int:
        """Borra de todos los nodos el contenido que no está en live_keys.

        Devuelve cuántos contenidos distintos se borraron. Un nodo que no
        responde se olvida y se sigue con los demás.
        """
        live = set(live_keys)
        listings = self._listings()
        garbage = {name: [key for key in keys if key not in live] for name, keys in listings.items()}
        self._delete(garbage)
        return len({key for keys in garbage.values() for key in keys})

    def reconcile(self, live_keys: Collection[str]) -> Reconciliation:
        """Repara el contenido después de fallos o particiones.

        1. Readmite en el anillo a los nodos conocidos que vuelven a responder.
        2. Borra el contenido que ningún archivo vivo usa (pudo quedar en un
           nodo aislado mientras el recolector borraba en los demás).
        3. Copia cada contenido vivo a las réplicas que indica el anillo y
           borra las copias que se escribieron en otros nodos mientras tanto.
        """
        returning = [name for name in self._known if name not in self._nodes]
        for name in returning:
            self._nodes[name] = self._known[name]
            self._ring.add_node(name)
        listings = self._listings()  # quien siga sin responder vuelve a salir
        rejoined = [name for name in returning if name in self._nodes]

        live = set(live_keys)
        holders: dict[str, set[str]] = defaultdict(set)
        for name, keys in listings.items():
            for key in keys:
                holders[key].add(name)
        garbage = {name: [key for key in keys if key not in live] for name, keys in listings.items()}
        self._delete(garbage)
        garbage_count = len({key for keys in garbage.values() for key in keys})

        copied = removed = 0
        unavailable = sorted(key for key in live if not holders.get(key))
        for key in sorted(live & holders.keys()):
            targets = self.replicas(key)
            missing = [name for name in targets if name not in holders[key]]
            if missing:
                data, _ = self._read_first_valid(key, sorted(holders[key] & self._nodes.keys()))
                if data is None:
                    unavailable.append(key)
                    continue
                data_ref = ray.put(data)
                written = self._wait_each({self._nodes[name].put.remote(key, data_ref): name for name in missing})
                copied += len(written)
                if len(written) < len(missing):
                    continue  # sin todas las réplicas, las copias sobrantes se conservan
            extra = sorted((holders[key] - set(targets)) & self._nodes.keys())
            removed += len(self._delete({name: [key] for name in extra}))
        return Reconciliation(rejoined, copied, removed, garbage_count, sorted(unavailable))

    def _listings(self) -> dict[str, list[str]]:
        """Pide a cada nodo del anillo lo que guarda; olvida a los que no responden."""
        refs = {node.list_keys.remote(): name for name, node in self._nodes.items()}
        listings: dict[str, list[str]] = {}
        for ref, name in refs.items():
            try:
                listings[name] = ray.get(ref, timeout=self._timeout)
            except _UNREACHABLE:
                self._forget(name)
        return listings

    def _delete(self, keys_by_node: Mapping[str, Collection[str]]) -> set[str]:
        """Borra las claves indicadas de cada nodo; devuelve los nodos que confirmaron."""
        refs = {
            self._nodes[name].delete.remote(key): name
            for name, keys in keys_by_node.items()
            for key in keys
        }
        return self._wait_each(refs)

    def _wait_each(self, refs: Mapping[Any, str]) -> set[str]:
        """Espera cada respuesta; olvida a los nodos que fallan y devuelve los que respondieron."""
        done: set[str] = set()
        failed: set[str] = set()
        for ref, name in refs.items():
            try:
                ray.get(ref, timeout=self._timeout)
            except _UNREACHABLE:
                failed.add(name)
                self._forget(name)
            else:
                done.add(name)
        return done - failed

    def _read_first_valid(self, key: str, names: list[str]) -> tuple[bytes | None, bool]:
        """Pide la clave a varios nodos; devuelve los bytes válidos y si hubo daños."""
        # find en vez de get: las respuestas que no se llegan a leer no deben ser errores,
        # porque Ray avisa de cada error que nadie recoge.
        refs = {self._nodes[name].find.remote(key): name for name in names}
        pending = list(refs)
        corrupted = False
        while pending:
            ready, pending = ray.wait(pending, num_returns=1, timeout=self._timeout)
            if not ready:
                break  # ninguno de los nodos restantes respondió a tiempo
            try:
                data = ray.get(ready[0])
            except _UNREACHABLE:
                self._forget(refs[ready[0]])
                continue
            if data is None:
                continue  # este nodo no lo tiene
            if content_hash(data) == key:
                return data, corrupted
            corrupted = True
        return None, corrupted

    def _forget(self, name: str) -> None:
        # Sale del anillo pero sigue en _known, para que reconcile pueda readmitirlo.
        self._ring.remove_node(name)
        self._nodes.pop(name, None)
