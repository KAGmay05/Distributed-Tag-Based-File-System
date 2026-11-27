"""Prueba varios clientes a la vez, cada uno en su propio worker y con su propio Gateway.

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


def test_concurrent_adds_and_removes_on_one_file_lose_no_update(cluster, gateway):
    # Cada cliente agrega y quita etiquetas que solo él toca, así que cada cambio
    # tiene efecto: si se perdiera uno, faltaría su versión o sobraría una etiqueta.
    clients, rounds = 4, 10
    added = {f"c1-nuevo-{c}-{r}" for c in range(clients) for r in range(rounds)}
    removed = {f"c1-viejo-{c}-{r}" for c in range(clients) for r in range(rounds)}
    file_id = gateway.put("a.txt", b"concurrencia", removed)

    def client(cluster, file_id, client_id, rounds):
        started = time.time()
        gateway = cluster.gateway()
        versions = []
        for r in range(rounds):
            versions.append(gateway.add_tags(file_id, {f"c1-nuevo-{client_id}-{r}"}))
            versions.append(gateway.remove_tags(file_id, {f"c1-viejo-{client_id}-{r}"}))
        return (started, time.time()), versions

    results = run_in_workers(client, [(cluster, file_id, c, rounds) for c in range(clients)])

    started, finished = zip(*(span for span, _ in results))
    assert max(started) < min(finished), "los clientes no coincidieron en el tiempo"
    changes = 2 * clients * rounds
    versions = sorted(version for _, own in results for version in own)
    assert versions == list(range(2, 2 + changes))  # cada cambio recibió su propia versión
    assert gateway.get(file_id)[2:] == (added, 1 + changes)
    # Sin el filtro de query: el índice también tiene cada agregado y ningún quitado.
    for tag in added | removed:
        shard = cluster.tags[shard_for(tag, len(cluster.tags))]
        assert (file_id in shard.lookup(tag)) == (tag in added), tag


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
