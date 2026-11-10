"""Prueba varios clientes, cada uno en su propia tarea de Ray, usando un único Gateway.

Las funciones de los clientes se definen dentro de cada test porque los workers
no pueden importar este archivo de tests.
"""

import pytest
import ray

from tagfs.gateway import start_gateway


@pytest.fixture(scope="module")
def gateway():
    return start_gateway()


def run_clients(client, calls):
    """Lanza un cliente por llamada, todos a la vez, y espera sus resultados."""
    remote_client = ray.remote(client)
    return ray.get([remote_client.remote(*args) for args in calls])


def test_concurrent_tag_changes_on_one_file_lose_no_update(gateway):
    removed = {f"c1-viejo-{i}" for i in range(25)}
    added = {f"c1-nuevo-{i}" for i in range(25)}
    file_id = ray.get(gateway.put.remote("a.txt", b"concurrencia", removed))

    def add(gateway, file_id, tag):
        return ray.get(gateway.add_tags.remote(file_id, {tag}))

    def remove(gateway, file_id, tag):
        return ray.get(gateway.remove_tags.remote(file_id, {tag}))

    versions = run_clients(add, [(gateway, file_id, tag) for tag in added])
    versions += run_clients(remove, [(gateway, file_id, tag) for tag in removed])

    stored = ray.get(gateway.get.remote(file_id))
    assert stored.tags == added
    assert stored.version == 51
    assert sorted(versions) == list(range(2, 52))  # cada cambio recibió su propia versión
    assert ray.get(gateway.query.remote("c1-nuevo-0 AND NOT c1-viejo-0")) == [file_id]


def test_mixed_concurrent_adds_and_removes_lose_no_update(gateway):
    removed = {f"c2-viejo-{i}" for i in range(25)}
    added = {f"c2-nuevo-{i}" for i in range(25)}
    file_id = ray.get(gateway.put.remote("b.txt", b"mezcla", removed))

    def change(gateway, file_id, tag, add):
        method = gateway.add_tags if add else gateway.remove_tags
        return ray.get(method.remote(file_id, {tag}))

    calls = [(gateway, file_id, tag, True) for tag in added]
    calls += [(gateway, file_id, tag, False) for tag in removed]
    run_clients(change, calls)

    stored = ray.get(gateway.get.remote(file_id))
    assert stored.tags == added
    assert stored.version == 51


def test_concurrent_puts_create_distinct_files(gateway):
    def create(gateway, i):
        return ray.get(gateway.put.remote(f"{i}.txt", f"contenido {i}".encode(), {"c3-doc"}))

    created = run_clients(create, [(gateway, i) for i in range(20)])

    assert len(set(created)) == 20
    assert ray.get(gateway.query.remote("c3-doc")) == sorted(created)
    assert set(created) <= set(ray.get(gateway.list.remote()))
