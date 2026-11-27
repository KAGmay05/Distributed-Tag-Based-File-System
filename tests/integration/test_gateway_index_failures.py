"""Prueba qué pasa cuando muere o se cuelga un TagShard.

Un TagShard de start_cluster se reinicia con el mismo handle y se rellena desde
los metadatos. Una caída se simula con ray.kill(..., no_restart=False); el
ray.kill por defecto es una muerte definitiva que Ray no repara.

Todos los tests comparten un clúster, así que cada uno usa etiquetas propias.
Las clases que corren en workers se definen dentro de cada test: así Ray las
envía por valor, porque los workers no pueden importar este módulo.
"""

import os
import time
from itertools import count

import pytest
import ray
from actor_restarts import crash_and_wait_for_restart

from tagfs.gateway import Gateway
from tagfs.ray_runtime import run_in_workers, start_cluster
from tagfs.router import shard_for
from tagfs.tag_shard import TagShard


@pytest.fixture(scope="module")
def cluster():
    return start_cluster()


@pytest.fixture(scope="module")
def gateway(cluster):
    return cluster.gateway()


def crash_index_of(gateway, tag):
    # Como si se cayera el proceso del actor: Ray lo vuelve a arrancar. No espera:
    # la llamada siguiente puede atenderla aún el proceso viejo. Para leer el
    # índice reconstruido hay que usar crash_index_and_wait_for_restart.
    ray.kill(gateway._tag_shard_for(tag), no_restart=False)


def crash_index_and_wait_for_restart(gateway, tag):
    crash_and_wait_for_restart(gateway._tag_shard_for(tag))


def wait_until_dead(actor, seconds=30, probe=None):
    # ray.kill no espera a que el actor muera: hasta entonces puede seguir respondiendo.
    # probe hace una llamada cualquiera al actor; por defecto, pid.
    probe = probe or actor.pid.remote
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            ray.get(probe(), timeout=seconds)
        except ray.exceptions.RayActorError:
            return
        time.sleep(0.1)
    raise TimeoutError("El actor no murió")


def raw_index(gateway, tag):
    # Lo que guarda el TagShard, sin el filtro de metadatos que aplica query.
    return ray.get(gateway._tag_shard_for(tag).lookup.remote(tag))


def test_put_succeeds_and_is_queryable_after_its_tag_shard_dies(gateway):
    old = gateway.put("viejo.jpg", b"v", {"i1-foto"})
    crash_index_of(gateway, "i1-foto")

    new = gateway.put("nuevo.jpg", b"n", {"i1-foto"})

    assert gateway.get(new).name == "nuevo.jpg"
    assert gateway.query("i1-foto") == sorted([old, new])


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


@pytest.mark.parametrize("operation", ["add", "remove", "delete"])
def test_changes_by_query_reach_the_index_after_its_tag_shard_dies(gateway, operation):
    tag, group = f"i12-{operation}", f"i12-{operation}-grupo"
    kept = gateway.put("kept.txt", b"k", {tag})
    changed = [
        gateway.put(f"{i}.txt", b"c", {group} if operation == "add" else {group, tag})
        for i in range(5)
    ]
    crash_index_of(gateway, tag)

    if operation == "add":
        result = gateway.add_tags_by_query(group, {tag})
        expected = [kept, *changed]
    elif operation == "remove":
        result = gateway.remove_tags_by_query(group, {tag})
        expected = [kept]
    else:
        result = gateway.delete_by_query(group)
        expected = [kept]

    assert result == ({file_id: 2 for file_id in changed}, [])
    assert gateway.query(tag) == sorted(expected)
    assert raw_index(gateway, tag) == set(expected)  # ninguna entrada de más


def test_a_change_by_query_sends_one_batch_to_each_tag_shard():
    # Con una llamada por cambio, entregar miles de cambios tardaría más que
    # index_timeout y el gateway reiniciaría un TagShard que funciona.
    # Usa su propio clúster, como el test del shard colgado.
    cluster = start_cluster(metadata_shards=2, tag_shards=1, storage_nodes=1, replication_factor=1)

    @ray.remote
    class CountingTagShard(TagShard):
        def __init__(self):
            super().__init__()
            self._batches = []

        def apply_many(self, ops):
            ops = list(ops)
            self._batches.append(len(ops))
            super().apply_many(ops)

        def batches(self):
            return self._batches

    shard = CountingTagShard.remote()
    gateway = Gateway([p.handle for p in cluster.metadata], [shard], cluster.content_store())
    created = sorted(gateway.put(f"{i}.txt", b"x", {"i14-doc"}) for i in range(50))

    assert gateway.add_tags_by_query("i14-doc", {"i14-nueva"}) == ({f: 2 for f in created}, [])

    assert ray.get(shard.batches.remote()) == [1] * 50 + [50]  # un lote por put y uno para todo
    assert gateway.query("i14-nueva") == created


def test_rebuilt_index_leaves_out_deleted_files_and_removed_tags(gateway):
    live = gateway.put("vivo.txt", b"1", {"i4-doc"})
    deleted = gateway.put("borrado.txt", b"2", {"i4-doc"})
    untagged = gateway.put("sin.txt", b"3", {"i4-doc"})
    gateway.delete(deleted)
    gateway.remove_tags(untagged, {"i4-doc"})

    crash_index_and_wait_for_restart(gateway, "i4-doc")  # se rellena desde los metadatos

    assert raw_index(gateway, "i4-doc") == {live}


def test_only_the_dead_tag_shard_restarts_and_keeps_its_handle(gateway):
    gateway.put("a.txt", b"x", {"i5-doc"})
    dead = gateway._tag_index("i5-doc")
    before = ray.get([shard.pid.remote() for shard in gateway._tags])

    crash_index_and_wait_for_restart(gateway, "i5-doc")
    gateway.query("i5-doc")

    after = ray.get([shard.pid.remote() for shard in gateway._tags])
    assert after[dead] != before[dead]  # el mismo handle llega a otro proceso
    assert [pid for i, pid in enumerate(after) if i != dead] == [
        pid for i, pid in enumerate(before) if i != dead
    ]


def test_every_client_sees_the_rebuilt_index(cluster):
    writer, reader = cluster.gateway(), cluster.gateway()
    first = writer.put("a.txt", b"x", {"i6-doc"})

    crash_index_and_wait_for_restart(writer, "i6-doc")

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


def test_a_tag_shard_that_hangs_during_a_change_by_query_is_restarted():
    # Usa su propio clúster: sus archivos no llegan nunca al índice del clúster, y
    # el verify_index de otros tests los contaría como entradas que faltan.
    cluster = start_cluster(metadata_shards=2, tag_shards=1, storage_nodes=1, replication_factor=1)

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
    class HangsOnTheFirstBatch(TagShard):
        # Se cuelga con el primer lote de más de un cambio; tras reiniciarse responde.
        # A diferencia de los TagShard de start_cluster, no se rellena al reiniciarse.
        def __init__(self, starts):
            super().__init__()
            self._first = ray.get(starts.add.remote()) == 1

        def ready(self):
            return True

        def apply_many(self, ops):
            ops = list(ops)
            if self._first and len(ops) > 1:
                time.sleep(3600)
            super().apply_many(ops)

    starts = Starts.remote()
    shard = HangsOnTheFirstBatch.remote(starts)
    ray.get(shard.ready.remote())
    gateway = Gateway(
        [p.handle for p in cluster.metadata], [shard], cluster.content_store(), index_timeout=1
    )
    created = sorted(gateway.put(f"{i}.txt", b"x", {"i15-doc"}) for i in range(5))

    # El cliente no ve el fallo: el gateway reinicia el shard y Ray le repite el lote.
    assert gateway.add_tags_by_query("i15-doc", {"i15-nueva"}) == ({f: 2 for f in created}, [])

    assert ray.get(starts.read.remote()) == 2  # se reinició una vez
    assert gateway.query("i15-nueva") == created


def test_a_tag_shard_restarts_even_after_a_metadata_shard_died():
    # Usa su propio clúster: matar un MetadataShard es definitivo.
    gateway = start_cluster().gateway()
    dead = gateway._metadata[0]
    ids = []
    while sum(gateway._metadata_for(f) is dead for f in ids) < 2 or len(ids) < 10:
        ids.append(gateway.put(f"{len(ids)}.txt", b"x", {"i8-doc"}))
    kept = sorted(f for f in ids if gateway._metadata_for(f) is not dead)
    ray.kill(dead)
    wait_until_dead(dead)

    crash_index_and_wait_for_restart(gateway, "i8-doc")  # al arrancar se salta el shard muerto

    assert raw_index(gateway, "i8-doc") == set(kept)
    assert gateway.query("i8-doc") == kept
    assert gateway.remove_tags(kept[0], {"i8-doc"}) == 2
    assert gateway.query("i8-doc") == kept[1:]


def test_a_change_the_index_did_not_receive_is_delivered_by_repair(cluster):
    unreachable = ray.remote(TagShard).remote()
    ray.kill(unreachable)  # muerte definitiva: este índice no vuelve
    cut_off = Gateway([p.handle for p in cluster.metadata], [unreachable], cluster.content_store())

    file_id = cut_off.put("a.txt", b"x", {"i9-doc"})  # no falla: los metadatos lo aceptaron

    gateway = cluster.gateway()
    assert gateway.get(file_id).name == "a.txt"
    assert gateway.query("i9-doc") == []
    assert gateway.repair_index() >= 1
    assert gateway.query("i9-doc") == [file_id]
    assert gateway.repair_index() == 0


def test_changes_by_query_the_index_did_not_receive_are_delivered_by_repair(cluster):
    gateway = cluster.gateway()
    # Un gateway con dos TagShard: el de la consulta es el real del clúster y el de
    # la etiqueta nueva está muerto para siempre.
    searched = next(tag for tag in (f"i11-doc-{n}" for n in count()) if shard_for(tag, 2) == 0)
    added = next(tag for tag in (f"i11-nueva-{n}" for n in count()) if shard_for(tag, 2) == 1)
    unreachable = ray.remote(TagShard).remote()
    ray.kill(unreachable)
    wait_until_dead(unreachable, probe=lambda: unreachable.lookup.remote(added))
    cut_off = Gateway(
        [p.handle for p in cluster.metadata],
        [gateway._tag_shard_for(searched), unreachable],
        cluster.content_store(),
    )
    created = sorted(gateway.put(f"{i}.txt", b"x", {searched}) for i in range(5))

    # No falla: los metadatos aceptaron los cambios.
    assert cut_off.add_tags_by_query(searched, {added}) == ({f: 2 for f in created}, [])

    assert [f.tags for f in gateway.list(searched)] == [{searched, added}] * 5
    assert gateway.query(added) == []
    assert gateway.repair_index() >= 5
    assert gateway.query(added) == created


def test_a_client_that_dies_before_telling_the_index_is_repaired(cluster):
    def client_that_dies_halfway(cluster):
        gateway = cluster.gateway()
        gateway._send_to_index = lambda ops: os._exit(1)  # muere justo antes de avisar al índice
        gateway.put("a-medias.txt", b"x", {"i10-doc"})

    with pytest.raises(ray.exceptions.WorkerCrashedError):
        run_in_workers(client_that_dies_halfway, [(cluster,)])

    gateway = cluster.gateway()
    [file_id] = [f.file_id for f in gateway.list() if f.name == "a-medias.txt"]
    assert gateway.query("i10-doc") == []
    assert ("i10-doc", file_id) in gateway.verify_index().missing

    gateway.repair_index()

    assert gateway.query("i10-doc") == [file_id]
    assert gateway.verify_index() == ([], [])


def test_a_client_that_dies_in_the_middle_of_a_change_by_query_is_repaired(cluster):
    gateway = cluster.gateway()
    created = sorted(gateway.put(f"{i}.txt", b"x", {"i13-doc"}) for i in range(5))

    def client_that_dies_halfway(cluster):
        client = cluster.gateway()
        client._send_to_index = lambda ops: os._exit(1)  # muere justo antes de avisar al índice
        client.add_tags_by_query("i13-doc", {"i13-nueva"})

    with pytest.raises(ray.exceptions.WorkerCrashedError):
        run_in_workers(client_that_dies_halfway, [(cluster,)])

    # Los metadatos ya tienen los cambios, pero el índice no.
    assert [f.tags for f in gateway.list("i13-doc")] == [{"i13-doc", "i13-nueva"}] * 5
    assert gateway.query("i13-nueva") == []

    assert gateway.repair_index() >= 5

    assert gateway.query("i13-nueva") == created
