"""Prueba varios clientes a la vez, cada uno en su propia tarea de Ray y con su propio Gateway.

Todos usan los mismos actores. Ningún cambio se pierde porque cada MetadataShard
es un actor y atiende una petición a la vez: los cambios de un mismo archivo se
aplican uno detrás de otro aunque vengan de clientes distintos. Un cambio por
consulta comprueba y cambia cada archivo dentro de esa misma petición, así que
otro cliente no puede colarse entre las dos cosas.

Las funciones de los clientes se definen dentro de cada test porque los workers
no pueden importar este archivo de tests.
"""

import random
import time

import pytest
import ray

from tagfs.gateway import Gateway
from tagfs.model import FileNotFound
from tagfs.ray_runtime import run_in_workers, start_cluster
from tagfs.router import shard_for


@pytest.fixture(scope="module")
def cluster():
    return start_cluster()


@pytest.fixture(scope="module")
def gateway(cluster):
    return cluster.gateway()


def run_clients(client, calls):
    """Lanza un cliente por llamada, todos a la vez, y espera sus resultados."""
    # max_retries=0: si Ray repitiera un cliente caído, repetiría sus put.
    remote_client = ray.remote(max_retries=0)(client)
    return ray.get([remote_client.remote(*args) for args in calls])


def test_concurrent_tag_changes_on_one_file_lose_no_update(cluster, gateway):
    removed = {f"c1-viejo-{i}" for i in range(25)}
    added = {f"c1-nuevo-{i}" for i in range(25)}
    file_id = gateway.put("a.txt", b"concurrencia", removed)

    def add(cluster, file_id, tag):
        return cluster.gateway().add_tags(file_id, {tag})

    def remove(cluster, file_id, tag):
        return cluster.gateway().remove_tags(file_id, {tag})

    versions = run_clients(add, [(cluster, file_id, tag) for tag in added])
    versions += run_clients(remove, [(cluster, file_id, tag) for tag in removed])

    stored = gateway.get(file_id)
    assert stored.tags == added
    assert stored.version == 51
    assert sorted(versions) == list(range(2, 52))  # cada cambio recibió su propia versión
    assert gateway.query("c1-nuevo-0 AND NOT c1-viejo-0") == [file_id]


def test_mixed_concurrent_adds_and_removes_lose_no_update(cluster, gateway):
    removed = {f"c2-viejo-{i}" for i in range(25)}
    added = {f"c2-nuevo-{i}" for i in range(25)}
    file_id = gateway.put("b.txt", b"mezcla", removed)

    def change(cluster, file_id, tag, add):
        client = cluster.gateway()
        method = client.add_tags if add else client.remove_tags
        return method(file_id, {tag})

    calls = [(cluster, file_id, tag, True) for tag in added]
    calls += [(cluster, file_id, tag, False) for tag in removed]
    run_clients(change, calls)

    stored = gateway.get(file_id)
    assert stored.tags == added
    assert stored.version == 51


def test_concurrent_puts_create_distinct_files(cluster, gateway):
    def create(cluster, i):
        return cluster.gateway().put(f"{i}.txt", f"contenido {i}".encode(), {"c3-doc"})

    created = run_clients(create, [(cluster, i) for i in range(20)])

    assert len(set(created)) == 20
    assert gateway.query("c3-doc") == sorted(created)
    assert set(created) <= {file.file_id for file in gateway.list()}


def test_delete_by_query_never_deletes_a_file_another_client_just_protected(cluster, gateway):
    # Un cliente borra lo que no está protegido mientras otros dos protegen archivos.
    # Cada archivo acaba en uno de dos casos: se protegió antes del borrado y sigue
    # vivo, o se borró antes y protegerlo falla. Si el borrado decidiera con una
    # consulta hecha antes de cambiar, podría borrar un archivo recién protegido.
    def client(cluster, role, prefix, file_ids):
        client = cluster.gateway()
        if role == "delete":
            result = client.delete_by_query(f"{prefix}-doc AND NOT {prefix}-guardar")
            return list(result.done), result.unknown
        protected = []
        for file_id in file_ids:
            try:
                client.add_tags(file_id, {f"{prefix}-guardar"})
            except FileNotFound:
                continue
            protected.append(file_id)
        return protected, []

    mixed = []
    for round_id in range(3):
        prefix = f"c4-{round_id}"
        files = sorted(gateway.put(f"{i}.txt", b"x", {f"{prefix}-doc"}) for i in range(120))

        results = run_in_workers(
            client,
            [
                (cluster, "delete", prefix, []),
                (cluster, "protect", prefix, files),
                (cluster, "protect", prefix, files[::-1]),
            ],
        )

        (deleted, unknown), *protections = results
        protected = {file_id for ids, _ in protections for file_id in ids}
        assert unknown == []
        assert set(deleted).isdisjoint(protected)
        assert set(deleted) | protected == set(files)
        assert gateway.query(f"{prefix}-guardar") == sorted(protected)
        assert [f.file_id for f in gateway.list(f"{prefix}-doc")] == sorted(protected)
        mixed.append(bool(deleted) and bool(protected))
    assert any(mixed), "el borrado nunca coincidió con las protecciones"


def test_concurrent_changes_by_query_lose_no_update(cluster, gateway):
    adders, rounds = 4, 5
    files, doomed = [], set()
    for i in range(40):
        doom = i % 4 == 0
        file_id = gateway.put(f"{i}.txt", b"x", {"c5-doc", "c5-borrar"} if doom else {"c5-doc"})
        files.append(file_id)
        if doom:
            doomed.add(file_id)

    def client(cluster, client_id, rounds, max_delay):
        class DelayedGateway(Gateway):
            # Retrasa cada entrega al índice, para que lleguen desordenadas entre clientes.
            def _send_to_index(self, ops):
                time.sleep(random.uniform(0, max_delay))
                super()._send_to_index(ops)

        client = DelayedGateway(
            [proxy.handle for proxy in cluster.metadata],
            [proxy.handle for proxy in cluster.tags],
            cluster.content_store(),
        )
        if client_id == "borrador":
            return [client.delete_by_query("c5-doc AND c5-borrar")]
        return [
            client.add_tags_by_query("c5-doc", {f"c5-cliente-{client_id}-ronda-{r}"})
            for r in range(rounds)
        ]

    calls = [(cluster, i, rounds, 0.01) for i in range(adders)] + [(cluster, "borrador", 0, 0.01)]
    *added, [deletion] = run_in_workers(client, calls)

    assert set(deletion.done) == doomed and deletion.unknown == []
    alive = sorted(set(files) - doomed)
    unique = {f"c5-cliente-{c}-ronda-{r}" for c in range(adders) for r in range(rounds)}
    for file_id in alive:
        # Cada agregado tuvo efecto, así que cada uno recibió su propia versión.
        versions = sorted(result.done[file_id] for results in added for result in results)
        assert versions == list(range(2, 2 + adders * rounds)), file_id
        assert gateway.get(file_id).tags == {"c5-doc"} | unique
    assert all(result.unknown == [] for results in added for result in results)
    # Sin el filtro de query: los archivos borrados tampoco quedan en el índice.
    for tag in unique:
        assert cluster.tags[shard_for(tag, len(cluster.tags))].lookup(tag) == set(alive), tag
    assert gateway.query("c5-borrar") == []
