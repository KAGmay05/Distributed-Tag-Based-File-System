"""Comportamiento externo del sistema, independiente de cómo se ejecute.

Cada implementación hereda GatewayContract y aporta el fixture `gateway`. La
clase no empieza por "Test", así que pytest solo ejecuta sus herederas.
"""

import pytest

from tagfs.model import FileNotFound, InvalidQuery, InvalidTag


def ids(files):
    return [file.file_id for file in files]


class GatewayContract:
    def test_put_and_get_round_trip(self, gateway):
        file_id = gateway.put("playa.jpg", b"contenido", {"Foto", "viaje"})

        assert gateway.get(file_id) == ("playa.jpg", b"contenido", {"foto", "viaje"}, 1)

    def test_identical_files_get_different_ids(self, gateway):
        first = gateway.put("a.txt", b"igual", {"doc"})
        second = gateway.put("a.txt", b"igual", {"doc"})

        assert first != second
        assert ids(gateway.list()) == sorted([first, second])

    def test_every_operation_on_an_unknown_file_fails(self, gateway):
        for operation in [
            lambda: gateway.get("desconocido"),
            lambda: gateway.delete("desconocido"),
            lambda: gateway.add_tags("desconocido", {"doc"}),
            lambda: gateway.remove_tags("desconocido", {"doc"}),
            lambda: gateway.rename("desconocido", "b.txt"),
        ]:
            with pytest.raises(FileNotFound):
                operation()

    def test_change_without_effect_keeps_the_version(self, gateway):
        file_id = gateway.put("a.txt", b"x", {"doc"})

        assert gateway.add_tags(file_id, {"DOC"}) == 1
        assert gateway.remove_tags(file_id, {"nunca-la-tuvo"}) == 1
        assert gateway.rename(file_id, "a.txt") == 1
        assert gateway.get(file_id).version == 1

    def test_invalid_tag_is_rejected_and_changes_nothing(self, gateway):
        file_id = gateway.put("a.txt", b"x", {"doc"})

        with pytest.raises(InvalidTag):
            gateway.put("b.txt", b"no debe guardarse", {"valida", "and"})
        with pytest.raises(InvalidTag):
            gateway.add_tags(file_id, {"valida", "dos palabras"})

        assert ids(gateway.list()) == [file_id]
        assert gateway.get(file_id) == ("a.txt", b"x", {"doc"}, 1)
        assert gateway.query("valida") == []
        assert gateway.collect_garbage() == 0  # el put rechazado no dejó bytes

    def test_a_string_is_not_a_collection_of_tags(self, gateway):
        # Sin esta regla, "foto" se guardaría como las etiquetas f, o y t.
        file_id = gateway.put("a.txt", b"x", {"doc"})

        with pytest.raises(InvalidTag):
            gateway.put("b.txt", b"no debe guardarse", "foto")
        with pytest.raises(InvalidTag):
            gateway.add_tags(file_id, "foto")
        with pytest.raises(InvalidTag):
            gateway.remove_tags(file_id, "doc")

        assert gateway.list() == [(file_id, "a.txt", {"doc"}, 1)]
        assert gateway.collect_garbage() == 0

    def test_a_name_must_be_text(self, gateway):
        file_id = gateway.put("a.txt", b"x", {"doc"})

        for name in [None, 7, b"a.txt"]:
            with pytest.raises(TypeError):
                gateway.put(name, b"no debe guardarse", {"doc"})
            with pytest.raises(TypeError):
                gateway.rename(file_id, name)

        assert gateway.list() == [(file_id, "a.txt", {"doc"}, 1)]
        assert gateway.collect_garbage() == 0

    def test_invalid_query_is_rejected(self, gateway):
        for expr in ["", "doc AND", "(doc", "NOT doc", "doc OR NOT borrador"]:
            with pytest.raises(InvalidQuery):
                gateway.query(expr)
            with pytest.raises(InvalidQuery):
                gateway.list(expr)

    def test_deleted_file_can_be_deleted_again_but_not_changed(self, gateway):
        file_id = gateway.put("a.txt", b"x", {"doc"})
        gateway.delete(file_id)

        assert gateway.delete(file_id) is None
        for operation in [
            lambda: gateway.add_tags(file_id, {"nueva"}),
            lambda: gateway.remove_tags(file_id, {"doc"}),
            lambda: gateway.rename(file_id, "b.txt"),
        ]:
            with pytest.raises(FileNotFound):
                operation()

    def test_add_tags_returns_new_version_and_makes_file_queryable(self, gateway):
        file_id = gateway.put("a.txt", b"x", {"doc"})

        assert gateway.add_tags(file_id, {"Urgente", "2024"}) == 2

        assert gateway.get(file_id)[2] == {"doc", "urgente", "2024"}
        assert gateway.query("urgente") == [file_id]
        assert gateway.query("2024") == [file_id]

    def test_remove_tags_returns_new_version_and_hides_file_from_query(self, gateway):
        file_id = gateway.put("a.txt", b"x", {"doc", "urgente"})

        assert gateway.remove_tags(file_id, {"URGENTE"}) == 2

        assert gateway.get(file_id)[2] == {"doc"}
        assert gateway.query("urgente") == []
        assert gateway.query("doc") == [file_id]

    def test_rename_returns_new_version_and_keeps_the_rest(self, gateway):
        file_id = gateway.put("a.txt", b"x", {"doc"})

        assert gateway.rename(file_id, "b.txt") == 2

        assert gateway.get(file_id) == ("b.txt", b"x", {"doc"}, 2)
        assert gateway.query("doc") == [file_id]

    def test_delete_hides_the_file_everywhere(self, gateway):
        file_id = gateway.put("a.txt", b"x", {"doc"})

        assert gateway.delete(file_id) is None

        with pytest.raises(FileNotFound):
            gateway.get(file_id)
        assert gateway.list() == []
        assert gateway.query("doc") == []

    def test_deleting_a_file_keeps_identical_content_of_another(self, gateway):
        first = gateway.put("a.txt", b"igual", {"doc"})
        second = gateway.put("b.txt", b"igual", {"doc"})

        gateway.delete(first)

        assert gateway.get(second)[1] == b"igual"

    def test_query_combines_tags(self, gateway):
        foto = gateway.put("foto.jpg", b"1", {"foto", "2024"})
        borrador = gateway.put("borrador.jpg", b"2", {"foto", "2024", "borrador"})
        video = gateway.put("video.mp4", b"3", {"video", "2023"})

        cases = {
            "foto": [foto, borrador],
            "FOTO": [foto, borrador],
            "foto AND borrador": [borrador],
            "foto AND NOT borrador": [foto],
            "foto OR video": [foto, borrador, video],
            "(foto OR video) AND NOT 2023": [foto, borrador],
            "foto AND (borrador OR NOT 2024)": [borrador],
            "desconocida": [],
        }
        for expr, expected in cases.items():
            assert gateway.query(expr) == sorted(expected), expr
            assert ids(gateway.list(expr)) == sorted(expected), expr

    def test_list_returns_every_live_file_in_order(self, gateway):
        created = [gateway.put(f"{i}.txt", b"x", {"doc"}) for i in range(10)]

        gateway.delete(created[0])

        assert ids(gateway.list()) == sorted(created[1:])

    def test_list_describes_each_file(self, gateway):
        foto = gateway.put("foto.jpg", b"1", {"Foto", "2024"})
        video = gateway.put("video.mp4", b"2", {"video"})
        gateway.rename(foto, "playa.jpg")
        gateway.add_tags(video, {"2024"})

        files = [(foto, "playa.jpg", {"foto", "2024"}, 2), (video, "video.mp4", {"video", "2024"}, 2)]
        assert gateway.list() == sorted(files)
        assert gateway.list("2024") == sorted(files)
        assert gateway.list("foto AND 2024") == [files[0]]

    def test_list_with_a_query_follows_every_change(self, gateway):
        file_id = gateway.put("a.txt", b"x", {"doc"})

        gateway.add_tags(file_id, {"urgente"})
        assert gateway.list("urgente") == [(file_id, "a.txt", {"doc", "urgente"}, 2)]

        gateway.remove_tags(file_id, {"urgente"})
        assert gateway.list("urgente") == []

        gateway.delete(file_id)
        assert gateway.list("doc") == []

    def test_collect_garbage_removes_only_content_no_live_file_uses(self, gateway):
        first = gateway.put("a.txt", b"igual", {"doc"})
        second = gateway.put("b.txt", b"igual", {"doc"})
        alone = gateway.put("c.txt", b"solo", {"doc"})

        gateway.delete(first)
        gateway.delete(alone)

        assert gateway.collect_garbage() == 1  # b"solo"; b"igual" lo sigue usando second
        assert gateway.get(second).content == b"igual"

    def test_index_matches_metadata_after_every_kind_of_change(self, gateway):
        kept = gateway.put("a.txt", b"x", {"doc", "foto"})
        changed = gateway.put("b.txt", b"y", {"doc"})
        gateway.add_tags(kept, {"urgente"})
        gateway.remove_tags(kept, {"foto"})
        gateway.rename(kept, "c.txt")
        gateway.delete(changed)

        assert gateway.verify_index() == ([], [])
        assert gateway.repair_index() == 0  # todo llegó al índice: no queda nada pendiente

    def test_reconcile_without_failures_changes_nothing(self, gateway):
        file_id = gateway.put("a.txt", b"x", {"doc"})

        assert gateway.reconcile() == ([], 0, 0, 0, [])
        assert gateway.get(file_id).content == b"x"
