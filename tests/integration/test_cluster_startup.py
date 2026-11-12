"""Prueba el arranque del sistema con start_cluster y actores de Ray reales.

Los tamaños inválidos se prueban sin Ray en tests/unit/test_ray_runtime.py.
"""

import ray

from tagfs.ray_runtime import start_cluster
from tagfs.router import shard_for


def test_starts_the_default_number_of_actors():
    cluster = start_cluster()

    assert len(cluster.metadata) == 4
    assert len(cluster.tags) == 4
    assert len(cluster.storage) == 4


def test_starts_the_requested_number_of_actors():
    cluster = start_cluster(metadata_shards=1, tag_shards=2, storage_nodes=3)

    assert len(cluster.metadata) == 1
    assert len(cluster.tags) == 2
    assert len(cluster.storage) == 3


def test_every_actor_answers_and_starts_empty():
    cluster = start_cluster()

    assert [shard.list_ids() for shard in cluster.metadata] == [[]] * 4
    assert [shard.lookup("foto") for shard in cluster.tags] == [set()] * 4
    assert ray.get([node.list_keys.remote() for node in cluster.storage.values()]) == [[]] * 4


def test_content_can_be_stored_right_after_starting():
    store = start_cluster().content_store()

    key = store.put(b"hola")

    assert store.get(key) == b"hola"


def test_every_client_routes_each_key_to_the_same_actor():
    cluster = start_cluster()
    first, second = cluster.gateway(), cluster.gateway()

    metadata = cluster.metadata[shard_for("f1", 4)].handle
    tags = cluster.tags[shard_for("foto", 4)].handle

    assert first._metadata_for("f1") is second._metadata_for("f1") is metadata
    assert first._tag_shard_for("foto") is second._tag_shard_for("foto") is tags
