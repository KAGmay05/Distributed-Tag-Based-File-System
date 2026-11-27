"""Prueba el arranque del sistema con start_cluster y actores de Ray reales.

Los tamaños inválidos se prueban sin Ray en tests/unit/test_ray_runtime.py, y el
tamaño por defecto en tests/integration/test_ray_cluster.py.
"""

from tagfs.ray_runtime import start_cluster


def test_starts_the_requested_number_of_actors():
    cluster = start_cluster(metadata_shards=1, tag_shards=2, storage_nodes=3)

    assert len(cluster.metadata) == 1
    assert len(cluster.tags) == 2
    assert len(cluster.storage) == 3


def test_content_can_be_stored_right_after_starting():
    store = start_cluster().content_store()

    key = store.put(b"hola")

    assert store.get(key) == b"hola"
