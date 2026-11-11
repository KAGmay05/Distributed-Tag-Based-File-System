"""Prueba particiones de red en los almacenes y la reconciliación posterior.

Una partición se simula con disconnect: el nodo sigue vivo y conserva sus
datos, pero rechaza toda petición hasta reconnect.
"""

import pytest
import ray

from tagfs.content import ContentNotFound, content_hash
from tagfs.content_store import ContentStore, RemoteStorageNode
from tagfs.ray_runtime import start_cluster

DATA = b"hola"
KEY = content_hash(DATA)


def make_store(names=("A", "B", "C", "D")):
    nodes = {name: RemoteStorageNode.remote() for name in names}
    ray.get([node.list_keys.remote() for node in nodes.values()])  # espera a que arranquen
    return ContentStore(nodes, replication_factor=2), nodes


def holders(nodes, key):
    return {name for name, node in nodes.items() if ray.get(node.has.remote(key))}


def disconnect(nodes, *names):
    ray.get([nodes[name].disconnect.remote() for name in names])


def reconnect(nodes, *names):
    ray.get([nodes[name].reconnect.remote() for name in names])


def test_content_stays_readable_while_one_replica_is_partitioned():
    store, nodes = make_store()
    store.put(DATA)
    disconnect(nodes, store.replicas(KEY)[0])

    assert store.get(KEY) == DATA


def test_a_partitioned_node_keeps_its_data_and_rejoins():
    store, nodes = make_store()
    store.put(DATA)
    isolated = store.replicas(KEY)[0]
    disconnect(nodes, isolated)
    # Una escritura espera a todas las réplicas, así que detecta la partición
    # y saca al nodo del anillo; una lectura podría no notarlo si otra réplica responde antes.
    store.put(DATA)
    assert isolated not in store.replicas(KEY)

    reconnect(nodes, isolated)
    report = store.reconcile({KEY})

    assert report.rejoined == [isolated]
    assert isolated in store.replicas(KEY)
    assert holders(nodes, KEY) == set(store.replicas(KEY))


def test_writes_during_a_partition_move_back_to_their_place():
    store, nodes = make_store()
    isolated = "A"
    disconnect(nodes, isolated)
    keys = [store.put(f"escrito durante la particion {i}".encode()) for i in range(20)]

    reconnect(nodes, isolated)
    report = store.reconcile(set(keys))

    assert report.copied > 0  # A recibe lo que le corresponde
    assert report.removed > 0  # y se borran las copias que lo sustituyeron
    for key in keys:
        assert holders(nodes, key) == set(store.replicas(key))
        assert store.get(key).startswith(b"escrito durante")


def test_content_with_every_replica_partitioned_is_unavailable_but_not_lost():
    store, nodes = make_store()
    store.put(DATA)
    replicas = store.replicas(KEY)
    disconnect(nodes, *replicas)

    with pytest.raises(ContentNotFound):
        store.get(KEY)
    assert store.reconcile({KEY}).unavailable == [KEY]

    reconnect(nodes, *replicas)
    report = store.reconcile({KEY})

    assert sorted(report.rejoined) == sorted(replicas)
    assert report.unavailable == []
    assert store.get(KEY) == DATA


def test_garbage_left_on_a_partitioned_node_is_removed_when_it_returns():
    store, nodes = make_store()
    store.put(DATA)
    isolated = store.replicas(KEY)[0]
    disconnect(nodes, isolated)

    assert store.sweep(set()) == 1  # el recolector no alcanza al nodo aislado
    reconnect(nodes, isolated)
    assert holders(nodes, KEY) == {isolated}

    assert store.reconcile(set()).garbage == 1
    assert holders(nodes, KEY) == set()


def test_reconcile_does_not_resurrect_a_dead_node():
    store, nodes = make_store()
    store.put(DATA)
    dead = store.replicas(KEY)[0]
    ray.kill(nodes[dead])

    report = store.reconcile({KEY})

    assert report.rejoined == []
    assert dead not in store.replicas(KEY)
    alive = {name: node for name, node in nodes.items() if name != dead}
    assert holders(alive, KEY) == set(store.replicas(KEY))  # recupera las 2 copias


def test_reconcile_without_failures_changes_nothing():
    store, nodes = make_store()
    store.put(DATA)

    assert store.reconcile({KEY}) == ([], 0, 0, 0, [])
    assert holders(nodes, KEY) == set(store.replicas(KEY))


def test_gateway_reconciles_after_a_partition():
    cluster = start_cluster()
    gateway = cluster.gateway()
    nodes = cluster.storage
    kept = [gateway.put(f"{i}.txt", f"antes {i}".encode(), {"doc"}) for i in range(10)]
    isolated = "storage-1"
    disconnect(nodes, isolated)

    # Durante la partición el sistema sigue funcionando.
    during = [gateway.put(f"d{i}.txt", f"durante {i}".encode(), {"doc"}) for i in range(10)]
    deleted = kept.pop()
    gateway.delete(deleted)
    gateway.collect_garbage()
    assert all(gateway.get(f).content for f in kept + during)

    reconnect(nodes, isolated)
    report = gateway.reconcile()

    assert report.rejoined == [isolated]
    assert report.unavailable == []
    live = {gateway.get(f).content for f in kept + during}
    for content in live:
        key = content_hash(content)
        assert holders(nodes, key) == set(gateway._content.replicas(key))
    stored = {key for node in nodes.values() for key in ray.get(node.list_keys.remote())}
    assert stored == {content_hash(c) for c in live}  # no queda basura en ningún almacén
    assert gateway.query("doc") == sorted(kept + during)
