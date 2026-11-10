"""Ejecuta el contrato sobre actores reales y prueba lo que solo existe con varios procesos.

Las funciones que corren en workers se definen dentro de cada test: así Ray las
envía por valor, porque los workers no pueden importar este módulo.
"""

import os
import random
import time

import pytest

ray = pytest.importorskip("ray")

from gateway_contract import GatewayContract  # noqa: E402

from tagfs.content import content_hash  # noqa: E402
from tagfs.gateway import Gateway  # noqa: E402
from tagfs.ray_runtime import ActorProxy, run_in_workers, start_cluster  # noqa: E402
from tagfs.router import shard_for  # noqa: E402

CLIENTS = 4
FILES_PER_CLIENT = 25
MAX_DELIVERY_DELAY_SECONDS = 0.01
ROUND_SECONDS = 0.04


def assert_ran_at_the_same_time(spans):
    started, finished = zip(*spans)
    assert max(started) < min(finished), "los clientes no coincidieron en el tiempo"


@pytest.fixture
def cluster():
    return start_cluster()


class TestRayGateway(GatewayContract):
    @pytest.fixture
    def gateway(self):
        return start_cluster(metadata_shards=2, tag_shards=2).gateway()


def test_each_actor_runs_in_its_own_process(cluster):
    storage = [ActorProxy(node) for node in cluster.storage.values()]
    pids = [actor.pid() for actor in (*cluster.metadata, *cluster.tags, *storage)]

    assert len(pids) == 11
    assert len(set(pids)) == 11
    assert os.getpid() not in pids


def test_get_survives_the_death_of_a_storage_node(cluster):
    gateway = cluster.gateway()
    file_id = gateway.put("a.txt", b"contenido", {"doc"})
    replicas = cluster.content_store().replicas(content_hash(b"contenido"))

    ray.kill(cluster.storage[replicas[0]])

    assert gateway.get(file_id)[1] == b"contenido"


def test_concurrent_clients_lose_no_files(cluster):
    def client(cluster, client_id, count):
        started = time.time()
        gateway = cluster.gateway()
        created = [
            gateway.put(
                f"{client_id}-{i}.txt",
                f"{client_id}-{i}".encode(),
                {"comun", f"cliente-{client_id}"},
            )
            for i in range(count)
        ]
        return (started, time.time()), created

    results = run_in_workers(client, [(cluster, i, FILES_PER_CLIENT) for i in range(CLIENTS)])

    assert_ran_at_the_same_time([span for span, _ in results])
    created = [ids for _, ids in results]
    gateway = cluster.gateway()
    everything = [file_id for ids in created for file_id in ids]
    assert len(set(everything)) == CLIENTS * FILES_PER_CLIENT
    assert gateway.list() == sorted(everything)
    assert gateway.query("comun") == sorted(everything)
    for client_id, ids in enumerate(created):
        assert gateway.query(f"cliente-{client_id}") == sorted(ids)


def test_concurrent_tag_changes_on_one_file_stay_consistent(cluster):
    rounds = 30

    def client(cluster, file_id, client_id, rounds, max_delay, round_seconds):
        class DelayedGateway(Gateway):
            # Retrasa cada entrega como lo haría una red real: sin esto, los cambios
            # casi nunca llegan desordenados y la carrera no llegaría a ocurrir.
            def _deliver(self, index_ops):
                for op in index_ops:
                    time.sleep(random.uniform(0, max_delay))
                    super()._deliver([op])

        started = time.time()
        gateway = DelayedGateway(cluster.metadata, cluster.tags, cluster.content_store())
        versions = []
        # Cada ronda es una carrera aparte por su propia etiqueta, entre clientes que la
        # agregan y clientes que la quitan. Si todos compitieran por la misma etiqueta,
        # el último cliente en terminar taparía el resultado de las carreras anteriores.
        # Cada ronda tiene su turno en el reloj para que los clientes no se desalineen.
        for round_id in range(rounds):
            time.sleep(max(0, started + round_id * round_seconds - time.time()))
            unique = f"cliente-{client_id}-ronda-{round_id}"
            disputed = f"disputada-{round_id}"
            if client_id % 2 == 0:
                versions.append(gateway.add_tags(file_id, {unique, disputed}))
            else:
                versions.append(gateway.add_tags(file_id, {unique}))
                gateway.remove_tags(file_id, {disputed})
        return (started, time.time()), versions

    gateway = cluster.gateway()
    file_id = gateway.put("compartido.txt", b"x", {"base"})

    results = run_in_workers(
        client,
        [
            (cluster, file_id, i, rounds, MAX_DELIVERY_DELAY_SECONDS, ROUND_SECONDS)
            for i in range(CLIENTS)
        ],
    )

    assert_ran_at_the_same_time([span for span, _ in results])
    tags = gateway.get(file_id)[2]
    unique_tags = {f"cliente-{c}-ronda-{r}" for c in range(CLIENTS) for r in range(rounds)}
    disputed_tags = {f"disputada-{r}" for r in range(rounds)}

    # Cada add_tags tuvo efecto, así que dos clientes nunca pueden recibir la misma versión.
    versions = [version for _, result in results for version in result]
    assert len(set(versions)) == CLIENTS * rounds
    # Ninguna etiqueta única se vuelve a agregar: si faltara una, se perdió un cambio.
    assert unique_tags <= tags
    # El índice debe acabar igual que los metadatos, sea cual sea el cliente que ganó.
    for tag in unique_tags | disputed_tags:
        shard = cluster.tags[shard_for(tag, len(cluster.tags))]
        assert (file_id in shard.lookup(tag)) == (tag in tags), tag
