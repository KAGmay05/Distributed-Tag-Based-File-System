"""Garantías del mantenimiento, independientes de cómo se ejecute el sistema.

No son parte del contrato de gateway_contract.py. La versión nativa debe dar las
mismas garantías (el índice acaba igual que los metadatos, el contenido que nadie
usa se borra y las copias se reparan), pero puede darlas sin estos métodos: por
ejemplo, entregando los cambios pendientes al índice en segundo plano.

Como el contrato, cada implementación hereda MaintenanceChecks y aporta el
fixture `gateway`.
"""

import pytest

from tagfs.content import ContentNotFound


class MaintenanceChecks:
    def test_collect_garbage_removes_only_content_no_live_file_uses(self, gateway):
        first = gateway.put("a.txt", b"igual", {"doc"})
        second = gateway.put("b.txt", b"igual", {"doc"})
        alone = gateway.put("c.txt", b"solo", {"doc"})

        gateway.delete(first)
        gateway.delete(alone)

        assert gateway.collect_garbage() == 1  # b"solo"; b"igual" lo sigue usando second
        assert gateway.get(second).content == b"igual"

    def test_collect_garbage_removes_content_left_by_a_failed_put(self, gateway):
        # Un put que guardó el contenido y falló antes de crear la ficha deja bytes
        # que ningún archivo usa. Se simula guardándolos directamente en el almacén.
        key = gateway._content.put(b"huerfano")

        assert gateway.collect_garbage() == 1

        with pytest.raises(ContentNotFound):
            gateway._content.get(key)

    def test_index_matches_metadata_after_every_kind_of_change(self, gateway):
        kept = gateway.put("a.txt", b"x", {"doc", "foto"})
        changed = gateway.put("b.txt", b"y", {"doc"})
        gateway.add_tags(kept, {"urgente"})
        gateway.remove_tags(kept, {"foto"})
        gateway.rename(kept, "c.txt")
        gateway.delete(changed)

        assert gateway.verify_index() == ([], [])
        assert gateway.repair_index() == 0  # todo llegó al índice: no queda nada pendiente

    def test_index_matches_metadata_after_changes_by_query(self, gateway):
        for i in range(12):
            gateway.put(f"{i}.txt", b"x", {"doc", f"grupo-{i % 3}"})

        gateway.add_tags_by_query("grupo-0", {"urgente"})
        gateway.remove_tags_by_query("grupo-1", {"doc"})
        gateway.delete_by_query("grupo-2")

        assert gateway.verify_index() == ([], [])
        assert gateway.repair_index() == 0

    def test_reconcile_without_failures_changes_nothing(self, gateway):
        file_id = gateway.put("a.txt", b"x", {"doc"})

        assert gateway.reconcile() == ([], 0, 0, 0, [])
        assert gateway.get(file_id).content == b"x"
