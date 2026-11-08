"""Comportamiento externo del sistema, independiente de cómo se ejecute.

Cada implementación hereda GatewayContract y aporta el fixture `gateway`. La
clase no empieza por "Test", así que pytest solo ejecuta sus herederas.
"""

import pytest

from tagfs.model import FileNotFound


class GatewayContract:
    def test_put_and_get_round_trip(self, gateway):
        file_id = gateway.put("playa.jpg", b"contenido", {"Foto", "viaje"})

        assert gateway.get(file_id) == ("playa.jpg", b"contenido", {"foto", "viaje"}, 1)

    def test_identical_files_get_different_ids(self, gateway):
        first = gateway.put("a.txt", b"igual", {"doc"})
        second = gateway.put("a.txt", b"igual", {"doc"})

        assert first != second
        assert gateway.list() == sorted([first, second])

    def test_get_of_unknown_file_fails(self, gateway):
        with pytest.raises(FileNotFound):
            gateway.get("desconocido")

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

    def test_list_returns_every_live_file_in_order(self, gateway):
        created = [gateway.put(f"{i}.txt", b"x", {"doc"}) for i in range(10)]

        gateway.delete(created[0])

        assert gateway.list() == sorted(created[1:])
