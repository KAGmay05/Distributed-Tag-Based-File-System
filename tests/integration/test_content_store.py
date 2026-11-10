"""Prueba la replicación con actores de Ray reales."""

import pytest
import ray

from tagfs.content import ContentCorrupted, ContentNotFound, NodeUnreachable, StorageUnavailable, content_hash
from tagfs.content_store import ContentStore, RemoteStorageNode
from tagfs.storage_node import StorageNode

DATA = b"hola"
KEY = content_hash(DATA)


def make_store(replication_factor=2, names=("A", "B", "C")):
    nodes = {name: RemoteStorageNode.remote() for name in names}
    return ContentStore(nodes, replication_factor), nodes


def make_corrupting_node():
    # Se define aquí porque los workers no pueden importar este archivo de tests.
    class CorruptingNode(StorageNode):
        """Nodo averiado: devuelve bytes que no coinciden con su clave."""

        def get(self, key):
            return b"datos danados"

    return ray.remote(CorruptingNode).remote()


def holders(nodes):
    return {name for name, node in nodes.items() if ray.get(node.has.remote(KEY))}


def test_put_returns_the_hash_and_get_returns_the_bytes():
    store, _ = make_store()

    assert store.put(DATA) == KEY
    assert store.get(KEY) == DATA


@pytest.mark.parametrize("replication_factor", [1, 2, 3])
def test_put_stores_exactly_r_copies_on_the_nodes_of_the_ring(replication_factor):
    store, nodes = make_store(replication_factor)

    store.put(DATA)

    assert holders(nodes) == set(store.replicas(KEY))
    assert len(holders(nodes)) == replication_factor


def test_repeated_put_does_not_create_more_copies():
    store, nodes = make_store()

    store.put(DATA)
    store.put(DATA)

    assert len(holders(nodes)) == 2


def test_get_of_unknown_content_fails():
    store, _ = make_store()

    with pytest.raises(ContentNotFound):
        store.get(KEY)


def test_with_two_copies_get_keeps_working_after_the_ring_forgets_the_dead_node():
    store, nodes = make_store(replication_factor=2)
    store.put(DATA)
    ray.kill(nodes[store.replicas(KEY)[0]])

    assert store.get(KEY) == DATA
    assert store.get(KEY) == DATA


def test_put_writes_to_the_next_node_when_one_is_dead():
    store, nodes = make_store(replication_factor=2)
    dead = store.replicas(KEY)[0]
    ray.kill(nodes[dead])

    store.put(DATA)

    alive = {name: node for name, node in nodes.items() if name != dead}
    assert len(holders(alive)) == 2


def test_put_keeps_fewer_copies_when_too_few_nodes_are_alive():
    store, nodes = make_store(replication_factor=2, names=("A", "B"))
    ray.kill(nodes["A"])

    store.put(DATA)

    assert holders({"B": nodes["B"]}) == {"B"}


def test_put_fails_when_every_node_is_dead():
    store, nodes = make_store(replication_factor=2, names=("A", "B"))
    ray.kill(nodes["A"])
    ray.kill(nodes["B"])

    with pytest.raises(StorageUnavailable):
        store.put(DATA)


def test_a_new_store_finds_content_written_while_the_replicas_were_down():
    store, nodes = make_store(replication_factor=2, names=("A", "B", "C", "D"))
    for name in store.replicas(KEY):
        ray.kill(nodes[name])
    store.put(DATA)

    new_store = ContentStore(nodes, replication_factor=2)

    assert new_store.get(KEY) == DATA


def test_get_finds_content_when_the_expected_replicas_are_alive_but_empty():
    store, nodes = make_store(replication_factor=2, names=("A", "B", "C", "D"))
    other = next(name for name in nodes if name not in store.replicas(KEY))
    ray.get(nodes[other].put.remote(KEY, DATA))

    assert store.get(KEY) == DATA


def test_get_fails_when_every_node_is_dead():
    store, nodes = make_store(replication_factor=2, names=("A", "B"))
    ray.kill(nodes["A"])
    ray.kill(nodes["B"])

    with pytest.raises(ContentNotFound):
        store.get(KEY)
    with pytest.raises(ContentNotFound):
        store.get(KEY)


def test_get_skips_a_corrupted_replica():
    good = RemoteStorageNode.remote()
    store = ContentStore({"good": good, "bad": make_corrupting_node()}, replication_factor=2)
    store.put(DATA)

    assert store.get(KEY) == DATA


def test_get_reports_corruption_when_no_replica_is_valid():
    store = ContentStore({"bad": make_corrupting_node()}, replication_factor=1)
    store.put(DATA)

    with pytest.raises(ContentCorrupted):
        store.get(KEY)


def test_sweep_removes_every_copy_of_content_that_is_not_live():
    store, nodes = make_store(replication_factor=2)
    kept = store.put(b"vivo")
    store.put(DATA)

    assert store.sweep({kept}) == 1

    assert holders(nodes) == set()
    assert store.get(kept) == b"vivo"


def test_sweep_with_nothing_to_remove_returns_zero():
    store, _ = make_store()
    store.put(DATA)

    assert store.sweep({KEY}) == 0
    assert store.get(KEY) == DATA


def test_sweep_skips_a_dead_node_and_cleans_the_rest():
    store, nodes = make_store(replication_factor=3)
    store.put(DATA)
    ray.kill(nodes["A"])

    assert store.sweep(set()) == 1

    assert holders({name: node for name, node in nodes.items() if name != "A"}) == set()


def make_damageable_store():
    class DamageableNode(StorageNode):
        def corrupt(self, key):
            self._blobs[key] = b"bytes corruptos"

    actor = ray.remote(DamageableNode)
    nodes = {name: actor.remote() for name in ("A", "B", "C")}
    ray.get([node.list_keys.remote() for node in nodes.values()])
    return ContentStore(nodes, replication_factor=2), nodes


def test_reconcile_repairs_corrupt_destinations_before_removing_the_last_good_copy():
    store, nodes = make_damageable_store()
    ray.get([node.put.remote(KEY, DATA) for node in nodes.values()])
    targets = store.replicas(KEY)
    ray.get([nodes[name].corrupt.remote(KEY) for name in targets])

    report = store.reconcile({KEY})

    assert report.copied == 2
    assert report.removed == 1
    assert report.unavailable == []
    assert holders(nodes) == set(targets)
    assert ray.get([nodes[name].get.remote(KEY) for name in targets]) == [DATA, DATA]


def test_reconcile_reports_content_with_only_corrupted_copies_as_unavailable():
    store, nodes = make_damageable_store()
    store.put(DATA)
    ray.get([nodes[name].corrupt.remote(KEY) for name in store.replicas(KEY)])

    report = store.reconcile({KEY})

    assert report.unavailable == [KEY]
    assert report.copied == report.removed == 0
    assert holders(nodes) == set(store.replicas(KEY))
    with pytest.raises(ContentCorrupted):
        store.get(KEY)


def test_sweep_counts_only_confirmed_deletions():
    class UnreachableOnDelete(StorageNode):
        def delete(self, key):
            raise NodeUnreachable("Se perdió la conexión antes del borrado")

    node = ray.remote(UnreachableOnDelete).remote()
    ray.get(node.put.remote(KEY, DATA))
    store = ContentStore({"A": node}, replication_factor=1)

    assert store.sweep(set()) == 0
    assert ray.get(node.has.remote(KEY))
