"""Prueba el arranque del Gateway con actores de Ray reales."""

import pytest
import ray

from tagfs.gateway import Gateway
from tagfs.router import shard_for


def test_starts_the_default_number_of_actors():
    gateway = Gateway()

    assert len(gateway._metadata) == 3
    assert len(gateway._tags) == 4
    assert len(gateway._storage) == 4


def test_starts_the_requested_number_of_actors():
    gateway = Gateway(metadata_shards=1, tag_shards=2, storage_nodes=3)

    assert len(gateway._metadata) == 1
    assert len(gateway._tags) == 2
    assert len(gateway._storage) == 3


def test_every_actor_answers():
    gateway = Gateway()

    assert ray.get([shard.list_ids.remote() for shard in gateway._metadata]) == [[]] * 3
    assert ray.get([shard.lookup.remote("foto") for shard in gateway._tags]) == [set()] * 4
    assert ray.get([node.list_keys.remote() for node in gateway._storage.values()]) == [[]] * 4


def test_content_can_be_stored_right_after_starting():
    gateway = Gateway()

    key = gateway._content.put(b"hola")

    assert gateway._content.get(key) == b"hola"


def test_routes_each_key_with_the_shared_router():
    gateway = Gateway()

    assert gateway._metadata_for("f1") is gateway._metadata[shard_for("f1", 3)]
    assert gateway._tag_shard_for("foto") is gateway._tags[shard_for("foto", 4)]


@pytest.mark.parametrize(
    "options",
    [
        {"metadata_shards": 0},
        {"tag_shards": 0},
        {"storage_nodes": 0},
        {"replication_factor": 0},
        {"storage_nodes": 1, "replication_factor": 2},
    ],
    ids=["metadata", "tags", "storage", "replicas", "more_replicas_than_nodes"],
)
def test_rejects_invalid_sizes(options):
    with pytest.raises(ValueError):
        Gateway(**options)
