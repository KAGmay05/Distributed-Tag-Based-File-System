"""El cliente nunca ve errores de Ray.

Un error de nuestro código llega tal cual, sin el envoltorio de Ray ni el PID y
la IP del worker. Un shard muerto o colgado llega como ServiceUnavailable, y a
tiempo: el cliente no se queda bloqueado.

Matar o colgar un shard de metadatos es definitivo, así que esos tests usan un
clúster propio, casi siempre con un solo shard de metadatos: todas las
operaciones pasan por él.
Las funciones y clases que corren en workers se definen dentro de cada test: así
Ray las envía por valor, porque los workers no pueden importar este módulo.
"""

import time

import pytest
import ray
from ray.exceptions import RayActorError, RayError

from tagfs.gateway import Gateway
from tagfs.model import FileNotFound, InvalidTag, ServiceUnavailable
from tagfs.ray_runtime import start_cluster
from tagfs.tag_shard import TagShard

METADATA_TIMEOUT = 1
# Margen para lo que tarda Ray en enviar la petición y avisar del fallo.
MARGIN = 5


@pytest.fixture(scope="module")
def cluster():
    return start_cluster()


@pytest.fixture(scope="module")
def gateway(cluster):
    return cluster.gateway()


def assert_not_from_ray(error):
    assert not isinstance(error, RayError), type(error).__mro__
    assert "pid=" not in str(error), str(error)


def wait_until_dead(actor, seconds=30):
    # ray.kill no espera a que el actor muera: hasta entonces puede seguir respondiendo.
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            ray.get(actor.pid.remote(), timeout=seconds)
        except RayActorError:
            return
        time.sleep(0.1)
    raise TimeoutError("El actor no murió")


def every_operation(gateway, file_id):
    """Una llamada de cada operación del cliente sobre file_id."""
    return {
        "put": lambda: gateway.put("nuevo.txt", b"n", {"doc"}),
        "get": lambda: gateway.get(file_id),
        "add_tags": lambda: gateway.add_tags(file_id, {"otra"}),
        "remove_tags": lambda: gateway.remove_tags(file_id, {"doc"}),
        "rename": lambda: gateway.rename(file_id, "otro.txt"),
        "delete": lambda: gateway.delete(file_id),
        "query": lambda: gateway.query("doc"),
        "list": gateway.list,
        "repair_index": gateway.repair_index,
        "verify_index": gateway.verify_index,
        "collect_garbage": gateway.collect_garbage,
    }


# --- Errores de nuestro código -------------------------------------------------


@pytest.mark.parametrize("operation", ["get", "add_tags", "remove_tags", "rename", "delete"])
def test_unknown_file_raises_file_not_found_without_ray(gateway, operation):
    with pytest.raises(FileNotFound) as raised:
        every_operation(gateway, "no-existe")[operation]()

    assert type(raised.value) is FileNotFound
    assert_not_from_ray(raised.value)


def test_invalid_tag_checked_by_the_metadata_shard_arrives_without_ray(gateway):
    file_id = gateway.put("a.txt", b"x", {"e1-doc"})

    with pytest.raises(InvalidTag) as raised:
        gateway.add_tags(file_id, {"and"})  # la valida el shard de metadatos, en su worker

    assert type(raised.value) is InvalidTag
    assert_not_from_ray(raised.value)


# --- Shard de metadatos muerto o colgado ----------------------------------------


def test_dead_metadata_shard_raises_service_unavailable_on_every_operation():
    cluster = start_cluster(metadata_shards=1)
    gateway = cluster.gateway()
    file_id = gateway.put("a.txt", b"x", {"doc"})
    [shard] = gateway._metadata
    ray.kill(shard)
    wait_until_dead(shard)

    for operation in every_operation(gateway, file_id).values():
        with pytest.raises(ServiceUnavailable, match="metadatos") as raised:
            operation()
        assert_not_from_ray(raised.value)


def test_hung_metadata_shard_raises_service_unavailable_in_time():
    cluster = start_cluster(metadata_shards=1)
    gateway = cluster.gateway(metadata_timeout=METADATA_TIMEOUT)
    file_id = gateway.put("a.txt", b"x", {"doc"})  # query necesita un candidato que leer
    [shard] = gateway._metadata

    def hang(self):
        time.sleep(3600)

    # El actor atiende una petición cada vez: las siguientes esperan detrás del cuelgue.
    shard.__ray_call__.remote(hang)
    try:
        for name, operation in every_operation(gateway, file_id).items():
            started = time.monotonic()
            with pytest.raises(ServiceUnavailable) as raised:
                operation()
            assert time.monotonic() - started < METADATA_TIMEOUT + MARGIN, name
            assert_not_from_ray(raised.value)
    finally:
        ray.kill(shard)  # libera el worker, que si no seguiría dormido una hora


def test_hung_metadata_shard_puts_its_files_in_unknown_in_a_change_by_query():
    # Dos shards de metadatos: uno se cuelga y el otro debe hacer su parte.
    cluster = start_cluster(metadata_shards=2)
    gateway = cluster.gateway(metadata_timeout=METADATA_TIMEOUT)
    hung = gateway._metadata[0]

    def on_hung(file_id):
        return gateway._metadata_for(file_id) is hung

    ids = []
    while sum(map(on_hung, ids)) < 2 or sum(not on_hung(f) for f in ids) < 2:
        ids.append(gateway.put(f"{len(ids)}.txt", b"x", {"doc"}))
    stuck = sorted(f for f in ids if on_hung(f))
    kept = sorted(f for f in ids if not on_hung(f))

    def hang(self):
        time.sleep(3600)

    hung.__ray_call__.remote(hang)
    try:
        started = time.monotonic()
        result = gateway.add_tags_by_query("doc", {"masiva"})
        assert time.monotonic() - started < METADATA_TIMEOUT + MARGIN

        # El shard vivo hace su parte; de los archivos del colgado no se sabe nada.
        assert result == ({f: 2 for f in kept}, stuck)
        assert gateway.query("masiva") == kept
    finally:
        ray.kill(hung)


# --- Shard de etiquetas muerto o colgado ----------------------------------------


def test_dead_tag_shard_fails_queries_but_not_changes(cluster):
    dead = ray.remote(TagShard).remote()
    ray.kill(dead)  # muerte definitiva: Ray no lo reinicia
    gateway = Gateway([p.handle for p in cluster.metadata], [dead], cluster.content_store())

    file_id = gateway.put("a.txt", b"x", {"e2-doc"})  # los metadatos lo aceptaron
    assert gateway.add_tags(file_id, {"e2-otra"}) == 2

    with pytest.raises(ServiceUnavailable) as raised:
        gateway.query("e2-doc")
    assert_not_from_ray(raised.value)


def test_tag_shard_that_hangs_again_after_its_restart_raises_service_unavailable(cluster):
    @ray.remote(max_restarts=-1, max_task_retries=-1)
    class AlwaysHangs:
        # Se reinicia, pero el actor nuevo tampoco responde.
        def ready(self):
            return True

        def lookup(self, tag):
            time.sleep(3600)

        def apply_many(self, ops):
            time.sleep(3600)

    shard = AlwaysHangs.remote()
    ray.get(shard.ready.remote())
    gateway = Gateway(
        [p.handle for p in cluster.metadata],
        [shard],
        cluster.content_store(),
        index_timeout=1,
        restart_timeout=2,
    )
    try:
        file_id = gateway.put("a.txt", b"x", {"e3-doc"})  # no falla: el cambio queda en el outbox
        assert gateway.get(file_id).name == "a.txt"

        started = time.monotonic()
        with pytest.raises(ServiceUnavailable) as raised:
            gateway.query("e3-doc")
        assert time.monotonic() - started < 1 + 2 + MARGIN
        assert_not_from_ray(raised.value)
    finally:
        ray.kill(shard)  # no_restart=True: ahora sí muere del todo
