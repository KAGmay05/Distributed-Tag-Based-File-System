"""Comprueba que un cliente de start_cluster envía a la vez las peticiones independientes.

Los shards de prueba no responden hasta que les han llegado todas las peticiones
esperadas. Si el gateway esperara cada respuesta antes de enviar la siguiente,
la primera petición nunca terminaría y el test fallaría por timeout. Así el test
no depende de medir tiempos.

Las clases que corren en workers se definen dentro de una función: así Ray las
envía por valor, porque los workers no pueden importar este módulo.
"""

import time
from dataclasses import replace

import pytest
import ray

from tagfs.fake_content_store import FakeContentStore
from tagfs.gateway import Gateway
from tagfs.ray_runtime import ActorProxy, start_cluster
from tagfs.router import shard_for
from tagfs.tag_shard import TagShard

SHARDS = 4
WAIT_SECONDS = 10


def waiting_shards(count):
    """Crea `count` shards que no responden hasta recibir `count` peticiones entre todos."""

    @ray.remote
    class Meeting:
        def __init__(self, parties):
            self._parties = parties
            self._arrived = 0

        def arrive(self):
            self._arrived += 1

        def is_open(self):
            return self._arrived >= self._parties

    @ray.remote
    class WaitingShard:
        def __init__(self, meeting, wait_seconds):
            self._meeting = meeting
            self._wait_seconds = wait_seconds

        def ready(self):
            return True

        def list_ids(self):
            self._wait_for_the_rest()
            return []

        def lookup(self, tag):
            self._wait_for_the_rest()
            return set()

        def apply(self, op):
            self._wait_for_the_rest()

        def _wait_for_the_rest(self):
            ray.get(self._meeting.arrive.remote())
            deadline = time.monotonic() + self._wait_seconds
            while not ray.get(self._meeting.is_open.remote()):
                if time.monotonic() > deadline:
                    raise TimeoutError("No llegaron las demás peticiones: se enviaron de una en una")
                time.sleep(0.01)

    meeting = Meeting.remote(count)
    shards = [WaitingShard.remote(meeting, WAIT_SECONDS) for _ in range(count)]
    # Un actor recién creado tarda en arrancar, más aún con muchos procesos vivos.
    # Se espera aquí, como hace start_cluster, para que ese retraso no cuente
    # dentro del timeout del índice del gateway.
    ray.get([meeting.is_open.remote()] + [shard.ready.remote() for shard in shards])
    return tuple(ActorProxy(shard) for shard in shards)


def one_tag_per_shard(shards):
    """Devuelve una etiqueta para cada shard, en orden."""
    found = {}
    number = 0
    while len(found) < shards:
        tag = f"etiqueta-{number}"
        found.setdefault(shard_for(tag, shards), tag)
        number += 1
    return [found[index] for index in range(shards)]


@pytest.fixture
def cluster():
    return start_cluster(metadata_shards=1, tag_shards=1, storage_nodes=1, replication_factor=1)


def test_list_asks_every_metadata_shard_at_once(cluster):
    gateway = replace(cluster, metadata=waiting_shards(SHARDS)).gateway()

    assert gateway.list() == []


def test_query_asks_every_tag_shard_at_once(cluster):
    gateway = replace(cluster, tags=waiting_shards(SHARDS)).gateway()

    assert gateway.query(" OR ".join(one_tag_per_shard(SHARDS))) == []


def test_put_sends_its_index_changes_at_once(cluster):
    gateway = replace(cluster, tags=waiting_shards(SHARDS)).gateway()
    tags = one_tag_per_shard(SHARDS)

    file_id = gateway.put("a.txt", b"x", tags)

    assert gateway.get(file_id).tags == frozenset(tags)


def test_shards_must_be_all_actors_or_all_local_objects(cluster):
    with pytest.raises(ValueError):
        Gateway([cluster.metadata[0].handle], [TagShard()], FakeContentStore())
