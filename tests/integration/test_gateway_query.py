"""Prueba query y list del Gateway con actores de Ray reales.

Todos los tests comparten un Gateway, así que cada uno usa etiquetas propias
para no ver los archivos de los demás.
"""

import pytest
import ray

from tagfs.model import IndexOp, InvalidQuery
from tagfs.ray_runtime import start_cluster


@pytest.fixture(scope="module")
def gateway():
    return start_cluster().gateway()


def test_query_combines_tags(gateway):
    foto = gateway.put("foto.jpg", b"1", {"q1-foto", "q1-2024"})
    borrador = gateway.put("borrador.jpg", b"2", {"q1-foto", "q1-2024", "q1-borrador"})
    video = gateway.put("video.mp4", b"3", {"q1-video", "q1-2023"})

    cases = {
        "q1-foto": [foto, borrador],
        "Q1-FOTO": [foto, borrador],
        "q1-foto AND q1-borrador": [borrador],
        "q1-foto AND NOT q1-borrador": [foto],
        "q1-foto OR q1-video": [foto, borrador, video],
        "(q1-foto OR q1-video) AND NOT q1-2023": [foto, borrador],
        "q1-foto AND (q1-borrador OR NOT q1-2024)": [borrador],
    }
    for expr, expected in cases.items():
        assert gateway.query(expr) == sorted(expected), expr


def test_query_of_unknown_tag_is_empty(gateway):
    assert gateway.query("q2-desconocida") == []


def test_query_finds_files_in_every_shard(gateway):
    created = [gateway.put(f"{i}.txt", b"x", {"q3-doc"}) for i in range(20)]

    assert gateway.query("q3-doc") == sorted(created)


def test_query_follows_tag_changes(gateway):
    file_id = gateway.put("a.txt", b"x", {"q4-doc"})

    gateway.add_tags(file_id, {"q4-urgente"})
    assert gateway.query("q4-urgente") == [file_id]

    gateway.remove_tags(file_id, {"q4-urgente"})
    assert gateway.query("q4-urgente") == []
    assert gateway.query("q4-doc") == [file_id]


def test_query_skips_deleted_files(gateway):
    kept = gateway.put("a.txt", b"x", {"q5-doc"})
    deleted = gateway.put("b.txt", b"y", {"q5-doc"})

    gateway.delete(deleted)

    assert gateway.query("q5-doc") == [kept]


def test_query_trusts_metadata_over_a_wrong_index_entry(gateway):
    file_id = gateway.put("a.txt", b"x", {"q6-doc"})
    # El índice dice que el archivo tiene "q6-fantasma", pero sus metadatos no.
    stale = IndexOp("q6-fantasma", file_id, True, 99)
    ray.get(gateway._tag_shard_for("q6-fantasma").apply.remote(stale))

    assert gateway.query("q6-fantasma") == []


@pytest.mark.parametrize("expr", ["", "q7-a AND", "NOT q7-a", "(q7-a"])
def test_invalid_query_is_rejected(gateway, expr):
    with pytest.raises(InvalidQuery):
        gateway.query(expr)


def test_list_returns_live_files_in_order(gateway):
    created = [gateway.put(f"{i}.txt", b"x", {"q8-doc"}) for i in range(10)]

    gateway.delete(created[0])
    listed = gateway.list()

    assert listed == sorted(listed)
    assert set(created[1:]) <= set(listed)
    assert created[0] not in listed


def test_list_of_a_new_system_is_empty():
    assert start_cluster(metadata_shards=2, tag_shards=1, storage_nodes=2).gateway().list() == []
