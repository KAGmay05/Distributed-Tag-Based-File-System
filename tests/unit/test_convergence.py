"""Prueba varios shards juntos, en un solo proceso, con entregas desordenadas."""

import random

import pytest

from tagfs.metadata_shard import MetadataShard
from tagfs.model import FileNotFound
from tagfs.router import shard_for
from tagfs.tag_shard import TagShard

TAGS = ["foto", "viaje", "2024", "borrador", "trabajo", "familia"]
SEEDS = range(200)


class Cluster:
    def __init__(self, metadata_shards=3, tag_shards=4):
        self.metadata = [MetadataShard() for _ in range(metadata_shards)]
        self.tags = [TagShard() for _ in range(tag_shards)]

    def metadata_for(self, file_id):
        return self.metadata[shard_for(file_id, len(self.metadata))]

    def tag_shard_for(self, tag):
        return self.tags[shard_for(tag, len(self.tags))]

    def deliver(self, ops):
        for op in ops:
            self.tag_shard_for(op.tag).apply(op)

    def index(self):
        return {tag: self.tag_shard_for(tag).lookup(tag) for tag in TAGS}

    def index_from_metadata(self):
        files = [
            meta
            for shard in self.metadata
            for meta in shard.get_many(shard.list_ids()).values()
        ]
        return {tag: {meta.file_id for meta in files if tag in meta.tags} for tag in TAGS}


def random_tags(rng):
    return rng.sample(TAGS, rng.randint(1, 3))


def run_random_operations(rng, cluster, steps=60):
    """Ejecuta operaciones aleatorias y devuelve los IndexOp sin entregar."""
    pending = []
    file_ids = []
    for step in range(steps):
        action = rng.choices(
            ["create", "add_tags", "remove_tags", "rename", "delete"],
            weights=[3, 4, 4, 1, 1],
        )[0]
        if action == "create" or not file_ids:
            file_id = f"file-{step}"
            file_ids.append(file_id)
            mutation = cluster.metadata_for(file_id).create(
                file_id, f"name-{step}", random_tags(rng), f"hash-{step}"
            )
        else:
            file_id = rng.choice(file_ids)
            shard = cluster.metadata_for(file_id)
            try:
                if action == "add_tags":
                    mutation = shard.add_tags(file_id, random_tags(rng))
                elif action == "remove_tags":
                    mutation = shard.remove_tags(file_id, random_tags(rng))
                elif action == "rename":
                    mutation = shard.rename(file_id, f"renamed-{step}")
                else:
                    mutation = shard.delete(file_id)
            except FileNotFound:
                continue  # los archivos borrados siguen entre los elegibles
        pending.extend(mutation.index_ops)
    return pending


def with_reordering_and_duplicates(rng, ops):
    delivery = ops + rng.sample(ops, len(ops) // 3)
    rng.shuffle(delivery)
    return delivery


@pytest.mark.parametrize("seed", SEEDS)
def test_index_converges_when_delivery_is_reordered_and_duplicated(seed):
    rng = random.Random(seed)
    cluster = Cluster()
    ops = run_random_operations(rng, cluster)

    cluster.deliver(with_reordering_and_duplicates(rng, ops))

    assert cluster.index() == cluster.index_from_metadata()
