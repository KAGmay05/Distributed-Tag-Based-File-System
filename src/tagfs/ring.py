"""Anillo de hashing consistente: decide qué nodos guardan cada contenido."""

from __future__ import annotations

import bisect
import hashlib


def _position(text: str) -> int:
    digest = hashlib.sha256(text.encode("utf-8")).digest()
    return int.from_bytes(digest, "big")


class HashRing:
    """Reparte claves entre nodos; al cambiar los nodos se mueven pocas claves."""

    def __init__(self, virtual_nodes: int = 100) -> None:
        if virtual_nodes <= 0:
            raise ValueError(f"Debe haber nodos virtuales: {virtual_nodes}")
        self._virtual_nodes = virtual_nodes
        self._nodes: set[str] = set()
        self._points: list[tuple[int, str]] = []

    def add_node(self, node_id: str) -> None:
        if node_id in self._nodes:
            return
        self._nodes.add(node_id)
        for i in range(self._virtual_nodes):
            bisect.insort(self._points, (_position(f"{node_id}#{i}"), node_id))

    def remove_node(self, node_id: str) -> None:
        self._nodes.discard(node_id)
        self._points = [point for point in self._points if point[1] != node_id]

    def nodes_for(self, key: str, count: int) -> list[str]:
        if count <= 0:
            raise ValueError(f"El número de réplicas debe ser positivo: {count}")
        start = bisect.bisect_left(self._points, (_position(key), ""))
        found: list[str] = []
        for offset in range(len(self._points)):
            _, node_id = self._points[(start + offset) % len(self._points)]
            if node_id not in found:
                found.append(node_id)
                if len(found) == count:
                    break
        return found
