"""Prueba qué pasa cuando muere o se cuelga un TagShard.

Un TagShard de start_cluster se reinicia con el mismo handle y se rellena desde
los metadatos. Una caída se simula con ray.kill(..., no_restart=False); el
ray.kill por defecto es una muerte definitiva que Ray no repara.

Todos los tests comparten un clúster, así que cada uno usa etiquetas propias.
Las clases que corren en workers se definen dentro de cada test: así Ray las
envía por valor, porque los workers no pueden importar este módulo.
"""

import time

import pytest
import ray

from tagfs.gateway import Gateway
from tagfs.ray_runtime import start_cluster


@pytest.fixture(scope="module")
def cluster():
    return start_cluster()


@pytest.fixture(scope="module")
def gateway(cluster):
    return cluster.gateway()


def crash_index_of(gateway, tag):
    # Como si se cayera el proceso del actor: Ray lo vuelve a arrancar.
    ray.kill(gateway._tag_shard_for(tag), no_restart=False)


def raw_index(gateway, tag):
    # Lo que guarda el TagShard, sin el filtro de metadatos que aplica query.
    return ray.get(gateway._tag_shard_for(tag).lookup.remote(tag))


def test_put_succeeds_and_is_queryable_after_its_tag_shard_dies(gateway):
    old = gateway.put("viejo.jpg", b"v", {"i1-foto"})
    crash_index_of(gateway, "i1-foto")

    new = gateway.put("nuevo.jpg", b"n", {"i1-foto"})

    assert gateway.get(new).name == "nuevo.jpg"
    assert gateway.query("i1-foto") == sorted([old, new])


def test_query_recovers_the_index_of_a_dead_tag_shard(gateway):
    file_id = gateway.put("a.txt", b"x", {"i2-doc"})
    crash_index_of(gateway, "i2-doc")

    assert gateway.query("i2-doc") == [file_id]


@pytest.mark.parametrize("operation", ["add", "remove", "delete"])
def test_changes_reach_the_index_after_its_tag_shard_dies(gateway, operation):
    tag = f"i3-{operation}"
    kept = gateway.put("kept.txt", b"k", {tag})
    changed = gateway.put("changed.txt", b"c", {tag} if operation != "add" else set())
    crash_index_of(gateway, tag)

    if operation == "add":
        gateway.add_tags(changed, {tag})
        expected = [kept, changed]
    elif operation == "remove":
        gateway.remove_tags(changed, {tag})
        expected = [kept]
    else:
        gateway.delete(changed)
        expected = [kept]

    assert gateway.query(tag) == sorted(expected)


def test_rebuilt_index_leaves_out_deleted_files_and_removed_tags(gateway):
    live = gateway.put("vivo.txt", b"1", {"i4-doc"})
    deleted = gateway.put("borrado.txt", b"2", {"i4-doc"})
    untagged = gateway.put("sin.txt", b"3", {"i4-doc"})
    gateway.delete(deleted)
    gateway.remove_tags(untagged, {"i4-doc"})

    crash_index_of(gateway, "i4-doc")  # al arrancar de nuevo se rellena desde los metadatos

    assert raw_index(gateway, "i4-doc") == {live}


def test_only_the_dead_tag_shard_restarts_and_keeps_its_handle(gateway):
    gateway.put("a.txt", b"x", {"i5-doc"})
    dead = gateway._tag_index("i5-doc")
    handles = list(gateway._tags)
    before = ray.get([shard.pid.remote() for shard in handles])

    crash_index_of(gateway, "i5-doc")
    gateway.query("i5-doc")

    after = ray.get([shard.pid.remote() for shard in gateway._tags])
    assert all(now is then for now, then in zip(gateway._tags, handles))  # mismo handle
    assert after[dead] != before[dead]  # pero otro proceso
    assert [pid for i, pid in enumerate(after) if i != dead] == [
        pid for i, pid in enumerate(before) if i != dead
    ]


def test_every_client_sees_the_rebuilt_index(cluster):
    writer, reader = cluster.gateway(), cluster.gateway()
    first = writer.put("a.txt", b"x", {"i6-doc"})

    crash_index_of(writer, "i6-doc")

    assert reader.query("i6-doc") == [first]
    second = reader.put("b.txt", b"y", {"i6-doc"})
    assert writer.query("i6-doc") == sorted([first, second])


def test_a_tag_shard_that_stops_answering_is_restarted(cluster):
    @ray.remote
    class Starts:
        def __init__(self):
            self.count = 0

        def add(self):
            self.count += 1
            return self.count

        def read(self):
            return self.count

    @ray.remote(max_restarts=-1, max_task_retries=-1)
    class HangsTheFirstTime:
        # Vivo pero sin responder la primera vez; tras reiniciarse responde.
        def __init__(self, starts):
            self._first = ray.get(starts.add.remote()) == 1

        def ready(self):
            return True

        def lookup(self, tag):
            if self._first:
                time.sleep(3600)
            return set()

    starts = Starts.remote()
    shard = HangsTheFirstTime.remote(starts)
    ray.get(shard.ready.remote())
    gateway = Gateway(
        [cluster.metadata[0].handle], [shard], cluster.content_store(), index_timeout=1
    )

    assert gateway.query("i7-doc") == []
    assert ray.get(starts.read.remote()) == 2  # se reinició una vez
