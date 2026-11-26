"""Prueba varios clientes a la vez, cada uno en su propia tarea de Ray y con su propio Gateway.

Todos usan los mismos actores. Ningún cambio se pierde porque cada MetadataShard
es un actor y atiende una petición a la vez: los cambios de un mismo archivo se
aplican uno detrás de otro aunque vengan de clientes distintos.

Las funciones de los clientes se definen dentro de cada test porque los workers
no pueden importar este archivo de tests.
"""

import pytest
import ray

from tagfs.ray_runtime import start_cluster


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
    assert set(created) <= set(gateway.list())
