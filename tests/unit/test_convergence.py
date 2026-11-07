"""Prueba varios shards juntos, en un solo proceso, con entregas desordenadas."""

import random

import pytest

from tagfs.metadata_shard import MetadataShard
from tagfs.model import FileNotFound, IndexOp
from tagfs.query import candidates, matches, parse_query
from tagfs.router import shard_for
from tagfs.tag_shard import TagShard

TAGS = ["foto", "viaje", "2024", "borrador", "trabajo", "familia"]
QUERIES = [
    "foto",
    "foto AND viaje",
    "foto OR trabajo",
    "foto AND NOT borrador",
    "(foto OR trabajo) AND NOT (borrador OR familia)",
    "2024 AND (viaje OR NOT familia)",
]
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

    def live_files(self):
        return [
            meta
            for shard in self.metadata
            for meta in shard.get_many(shard.list_ids()).values()
        ]

    def query(self, text):
        expr = parse_query(text)
        found = candidates(expr, lambda tag: self.tag_shard_for(tag).lookup(tag))
        result = set()
        for file_id in found:
            meta = self.metadata_for(file_id).get_many([file_id]).get(file_id)
            if meta is not None and matches(expr, meta.tags):
                result.add(file_id)
        return result

    def truth(self, text):
        expr = parse_query(text)
        return {meta.file_id for meta in self.live_files() if matches(expr, meta.tags)}

    def index(self):
        return {tag: self.tag_shard_for(tag).lookup(tag) for tag in TAGS}

    def index_from_metadata(self):
        files = self.live_files()
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


@pytest.mark.parametrize("seed", SEEDS)
def test_converged_queries_return_exactly_the_truth(seed):
    rng = random.Random(seed)
    cluster = Cluster()
    ops = run_random_operations(rng, cluster)

    cluster.deliver(with_reordering_and_duplicates(rng, ops))

    for text in QUERIES:
        assert cluster.query(text) == cluster.truth(text)


@pytest.mark.parametrize("seed", SEEDS)
def test_queries_never_return_a_wrong_file_while_the_index_catches_up(seed):
    rng = random.Random(seed)
    cluster = Cluster()
    ops = run_random_operations(rng, cluster)

    for op in with_reordering_and_duplicates(rng, ops):
        cluster.deliver([op])
        for text in QUERIES:
            assert cluster.query(text) <= cluster.truth(text)


def test_undelivered_add_hides_the_file_until_it_arrives():
    cluster = Cluster()
    _, ops = cluster.metadata_for("f1").create("f1", "a.jpg", {"foto"}, "hash")

    assert cluster.query("foto") == set()

    cluster.deliver(ops)

    assert cluster.query("foto") == {"f1"}


def test_undelivered_removal_does_not_return_the_file():
    cluster = Cluster()
    shard = cluster.metadata_for("f1")
    cluster.deliver(shard.create("f1", "a.jpg", {"foto"}, "hash").index_ops)

    shard.remove_tags("f1", {"foto"})

    assert cluster.query("foto") == set()


def test_undelivered_delete_does_not_return_the_file():
    cluster = Cluster()
    shard = cluster.metadata_for("f1")
    cluster.deliver(shard.create("f1", "a.jpg", {"foto"}, "hash").index_ops)

    shard.delete("f1")

    assert cluster.query("foto") == set()


def test_stale_entry_of_a_negated_tag_does_not_hide_a_valid_file():
    cluster = Cluster()
    shard = cluster.metadata_for("f1")
    cluster.deliver(shard.create("f1", "a.jpg", {"foto", "borrador"}, "hash").index_ops)

    shard.remove_tags("f1", {"borrador"})

    assert cluster.tag_shard_for("borrador").lookup("borrador") == {"f1"}
    assert cluster.query("foto AND NOT borrador") == {"f1"}


def test_concurrent_add_and_remove_converge_to_the_last_metadata_version():
    cluster = Cluster()
    shard = cluster.metadata_for("f1")
    cluster.deliver(shard.create("f1", "a.jpg", {"foto"}, "hash").index_ops)
    _, add_ops = shard.add_tags("f1", {"viaje"})
    _, remove_ops = shard.remove_tags("f1", {"viaje"})

    cluster.deliver(remove_ops)
    cluster.deliver(add_ops)

    assert remove_ops == (IndexOp("viaje", "f1", False, 3),)
    assert cluster.tag_shard_for("viaje").lookup("viaje") == set()
    assert cluster.query("viaje") == set()
