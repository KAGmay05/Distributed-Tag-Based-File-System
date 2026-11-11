"""Pruebas de extremo a extremo: el sistema completo, usado como un cliente, con fallos.

Cada escenario lleva un Model: lo que el sistema debería contener, calculado
sin el sistema. Tras las operaciones se compara todo (list, get de cada archivo
y muchas consultas) con el modelo, no solo lo que acaba de cambiar.
"""

import os
import random

import pytest
import ray
from ray.exceptions import RayActorError

from tagfs.content import ContentNotFound, StorageUnavailable, content_hash
from tagfs.gateway import StoredFile
from tagfs.model import FileNotFound, InvalidQuery, InvalidTag, normalize_tags
from tagfs.query import matches, parse_query
from tagfs.ray_runtime import run_in_workers, start_cluster

TAGS = ["foto", "viaje", "2024", "2023", "trabajo", "familia", "borrador", "video", "musica", "urgente"]
QUERIES = [
    "foto",
    "FOTO",
    "foto AND viaje",
    "foto OR video",
    "foto AND NOT borrador",
    "2024 AND (foto OR video)",
    "(trabajo OR familia) AND NOT (borrador OR urgente)",
    "musica OR (viaje AND 2023)",
    "foto AND viaje AND 2024 AND familia",
    "video AND NOT (2023 OR 2024)",
    "urgente AND (trabajo OR NOT familia)",
    "nadie-tiene-esta-etiqueta",
    "foto OR nadie-tiene-esta-etiqueta",
]
CONTENTS = [f"contenido {i} ".encode() * (i * 50 + 1) for i in range(10)]


class Model:
    """Lo que el sistema debería contener según las operaciones hechas."""

    def __init__(self) -> None:
        self.files: dict[str, StoredFile] = {}
        self.deleted: set[str] = set()

    def put(self, file_id, name, content, tags):
        self.files[file_id] = StoredFile(name, content, normalize_tags(tags), 1)

    def add_tags(self, file_id, tags):
        return self._update(file_id, tags=self.files[file_id].tags | normalize_tags(tags))

    def remove_tags(self, file_id, tags):
        return self._update(file_id, tags=self.files[file_id].tags - normalize_tags(tags))

    def rename(self, file_id, name):
        return self._update(file_id, name=name)

    def delete(self, file_id):
        self.files.pop(file_id, None)
        self.deleted.add(file_id)

    def query(self, expr):
        parsed = parse_query(expr)
        return sorted(file_id for file_id, f in self.files.items() if matches(parsed, f.tags))

    def live_contents(self):
        return {f.content for f in self.files.values()}

    def _update(self, file_id, **changes):
        # Un cambio sin efecto no sube la versión, igual que en MetadataShard.
        old = self.files[file_id]
        new = old._replace(**changes)
        if new != old:
            new = new._replace(version=old.version + 1)
        self.files[file_id] = new
        return new.version


class Client:
    """Usa el Gateway y anota cada operación en el modelo, comprobando lo que devuelve."""

    def __init__(self, gateway, model):
        self.gateway = gateway
        self.model = model

    def put(self, name, content, tags):
        file_id = self.gateway.put(name, content, tags)
        self.model.put(file_id, name, content, tags)
        return file_id

    def add_tags(self, file_id, tags):
        assert self.gateway.add_tags(file_id, tags) == self.model.add_tags(file_id, tags)

    def remove_tags(self, file_id, tags):
        assert self.gateway.remove_tags(file_id, tags) == self.model.remove_tags(file_id, tags)

    def rename(self, file_id, name):
        assert self.gateway.rename(file_id, name) == self.model.rename(file_id, name)

    def delete(self, file_id):
        assert self.gateway.delete(file_id) is None
        self.model.delete(file_id)


def assert_matches_model(gateway, model, queries=QUERIES):
    """Compara todo lo visible del sistema con el modelo."""
    assert gateway.list() == sorted(model.files)
    for file_id, expected in model.files.items():
        assert gateway.get(file_id) == expected, file_id
    for file_id in model.deleted:
        with pytest.raises(FileNotFound):
            gateway.get(file_id)
    for expr in queries:
        assert gateway.query(expr) == model.query(expr), expr


def alive_nodes(cluster):
    alive = {}
    for name, node in cluster.storage.items():
        try:
            ray.get(node.list_keys.remote(), timeout=10)
        except RayActorError:
            continue
        alive[name] = node
    return alive


def copies_of(cluster, content):
    nodes = alive_nodes(cluster).values()
    return sum(ray.get([node.has.remote(content_hash(content)) for node in nodes]))


def stored_keys(cluster):
    nodes = alive_nodes(cluster).values()
    return {key for keys in ray.get([node.list_keys.remote() for node in nodes]) for key in keys}


def random_tags(rng):
    return set(rng.sample(TAGS, rng.randint(0, 4)))


# --- Escenarios sin fallos ----------------------------------------------------


def test_full_life_of_the_system_survives_failures():
    cluster = start_cluster()
    gateway = cluster.gateway()

    # 1. Alta de archivos: dos comparten contenido.
    playa = gateway.put("playa.jpg", b"bytes de la playa", {"foto", "viaje", "2024"})
    copia = gateway.put("copia.jpg", b"bytes de la playa", {"foto", "backup"})
    informe = gateway.put("informe.pdf", b"bytes del informe", {"trabajo", "2024", "borrador"})

    assert gateway.list() == sorted([playa, copia, informe])
    assert gateway.get(playa) == ("playa.jpg", b"bytes de la playa", {"foto", "viaje", "2024"}, 1)
    assert gateway.query("foto") == sorted([playa, copia])
    assert gateway.query("2024 AND NOT borrador") == [playa]

    # 2. Cambios de etiquetas y de nombre.
    assert gateway.add_tags(informe, {"final"}) == 2
    assert gateway.remove_tags(informe, {"borrador"}) == 3
    assert gateway.rename(playa, "costa.jpg") == 2

    assert gateway.get(playa).name == "costa.jpg"
    assert gateway.query("final") == [informe]
    assert gateway.query("2024 AND NOT borrador") == sorted([playa, informe])

    # 3. Muere un almacén que guarda el contenido compartido.
    shared_hash = content_hash(b"bytes de la playa")
    ray.kill(cluster.storage[gateway._content.replicas(shared_hash)[0]])

    assert gateway.get(copia).content == b"bytes de la playa"
    nuevo = gateway.put("nuevo.txt", b"bytes nuevos", {"texto"})
    assert gateway.get(nuevo).content == b"bytes nuevos"

    # 4. Muere la sección del índice que guarda "foto": Ray la reinicia.
    ray.kill(gateway._tag_shard_for("foto"), no_restart=False)

    assert gateway.query("foto") == sorted([playa, copia])
    gateway.add_tags(nuevo, {"foto"})
    assert gateway.query("foto") == sorted([playa, copia, nuevo])

    # 5. Eliminación: el contenido compartido sobrevive hasta que nadie lo usa.
    gateway.delete(playa)

    with pytest.raises(FileNotFound):
        gateway.get(playa)
    assert gateway.query("foto") == sorted([copia, nuevo])
    gateway.collect_garbage()
    assert gateway.get(copia).content == b"bytes de la playa"

    gateway.delete(copia)
    gateway.collect_garbage()

    with pytest.raises(ContentNotFound):
        gateway._content.get(shared_hash)
    assert gateway.list() == sorted([informe, nuevo])
    assert gateway.get(informe).tags == {"trabajo", "2024", "final"}


def test_large_catalog_with_many_tags_and_queries():
    cluster = start_cluster()
    gateway = cluster.gateway()
    model = Model()
    client = Client(gateway, model)
    rng = random.Random(7)

    # 40 archivos: varios repiten nombre y muchos comparten contenido.
    ids = [
        client.put(f"archivo-{i % 15}.dat", rng.choice(CONTENTS), random_tags(rng))
        for i in range(40)
    ]
    assert_matches_model(gateway, model)

    # 60 cambios de etiquetas y nombres; cada uno comprueba la versión devuelta.
    for step in range(60):
        file_id = rng.choice(ids)
        action = rng.choice(["add", "remove", "rename"])
        if action == "add":
            client.add_tags(file_id, random_tags(rng))
        elif action == "remove":
            client.remove_tags(file_id, random_tags(rng))
        else:
            client.rename(file_id, f"renombrado-{step}.dat")
    assert_matches_model(gateway, model)

    # Se eliminan 15 archivos y se recolecta la basura.
    for file_id in rng.sample(ids, 15):
        client.delete(file_id)
    gateway.collect_garbage()

    assert_matches_model(gateway, model)
    for content in set(CONTENTS) - model.live_contents():
        assert copies_of(cluster, content) == 0
    for content in model.live_contents():
        assert copies_of(cluster, content) == 2


def test_edge_cases_and_invalid_requests_change_nothing():
    cluster = start_cluster()
    gateway = cluster.gateway()
    model = Model()
    client = Client(gateway, model)

    # Casos límite válidos.
    empty = client.put("", b"", set())
    unicode = client.put("año 2024 ñandú ✓.txt", "texto con acentos: áéíóú".encode(), {"Año-2024", "ÑANDÚ"})
    big = client.put("grande.bin", bytes(range(256)) * 4096, {"foto"})  # 1 MB
    twin_a = client.put("igual.txt", b"a", {"foto"})
    twin_b = client.put("igual.txt", b"b", {"foto"})
    assert twin_a != twin_b
    assert gateway.get(unicode).tags == {"año-2024", "ñandú"}
    assert_matches_model(gateway, model)

    # Operaciones sin efecto: no cambian la versión.
    client.add_tags(big, {"FOTO"})
    client.remove_tags(big, {"nunca-la-tuvo"})
    client.rename(big, "grande.bin")
    client.add_tags(empty, set())
    assert gateway.get(big).version == 1

    # Peticiones inválidas: fallan sin guardar nada.
    keys_before = stored_keys(cluster)
    for bad_tags in [{"and"}, {"OR"}, {"dos palabras"}, {"(x"}, {""}, {"bien", "not"}]:
        with pytest.raises(InvalidTag):
            gateway.put("malo.txt", b"bytes que no deben guardarse", bad_tags)
        with pytest.raises(InvalidTag):
            gateway.add_tags(big, bad_tags)
    with pytest.raises(TypeError):
        gateway.put("texto.txt", "no son bytes", {"foto"})
    for bad_query in ["", "foto AND", "NOT foto", "(foto", "foto)", "foto video", "AND"]:
        with pytest.raises(InvalidQuery):
            gateway.query(bad_query)
    assert stored_keys(cluster) == keys_before

    # Un archivo que no existe.
    for operation in [
        lambda: gateway.get("no-existe"),
        lambda: gateway.delete("no-existe"),
        lambda: gateway.add_tags("no-existe", {"foto"}),
        lambda: gateway.remove_tags("no-existe", {"foto"}),
        lambda: gateway.rename("no-existe", "x"),
    ]:
        with pytest.raises(FileNotFound):
            operation()

    # Un archivo eliminado: solo delete se puede repetir.
    client.delete(twin_a)
    gateway.delete(twin_a)
    for operation in [
        lambda: gateway.add_tags(twin_a, {"video"}),
        lambda: gateway.remove_tags(twin_a, {"foto"}),
        lambda: gateway.rename(twin_a, "x"),
    ]:
        with pytest.raises(FileNotFound):
            operation()

    assert_matches_model(gateway, model)
    assert gateway.get(twin_b).name == "igual.txt"


# --- Escenarios con fallos ----------------------------------------------------


def test_storage_nodes_fail_one_after_another():
    cluster = start_cluster()  # 4 almacenes, 2 copias de cada contenido
    gateway = cluster.gateway()
    model = Model()
    client = Client(gateway, model)
    rng = random.Random(3)
    for i in range(20):
        client.put(f"{i}.dat", CONTENTS[i % len(CONTENTS)], random_tags(rng))
    victim = client.put("victima.txt", b"contenido de la victima", {"foto"})
    first, second = gateway._content.replicas(content_hash(b"contenido de la victima"))

    # 1. Cae un almacén: con 2 copias no se pierde nada.
    ray.kill(cluster.storage[first])
    assert_matches_model(gateway, model)
    after_first = client.put("despues-1.txt", b"escrito con un almacen caido", {"video"})
    assert copies_of(cluster, b"escrito con un almacen caido") == 2
    client.add_tags(after_first, {"urgente"})
    assert_matches_model(gateway, model)

    # 2. Cae la otra copia de la víctima: su contenido se pierde, sus metadatos no.
    ray.kill(cluster.storage[second])
    with pytest.raises(ContentNotFound):
        gateway.get(victim)
    assert victim in gateway.list()
    assert victim in gateway.query("foto")
    client.rename(victim, "victima-renombrada.txt")
    readable = 0
    for file_id, expected in model.files.items():
        if copies_of(cluster, expected.content):
            assert gateway.get(file_id) == expected
            readable += 1
        else:
            with pytest.raises(ContentNotFound):
                gateway.get(file_id)
    assert readable > 0
    for expr in QUERIES:
        assert gateway.query(expr) == model.query(expr), expr

    # 3. Caen todos: no se puede escribir, pero los metadatos siguen funcionando.
    for node in alive_nodes(cluster).values():
        ray.kill(node)
    with pytest.raises(StorageUnavailable):
        gateway.put("imposible.txt", b"no hay donde guardarlo", {"foto"})
    with pytest.raises(ContentNotFound):
        gateway.get(after_first)
    assert gateway.list() == sorted(model.files)
    client.add_tags(after_first, {"musica"})
    for expr in QUERIES:
        assert gateway.query(expr) == model.query(expr), expr
    assert gateway.collect_garbage() == 0


def test_every_index_section_fails_and_is_rebuilt():
    cluster = start_cluster()
    gateway = cluster.gateway()
    model = Model()
    client = Client(gateway, model)
    rng = random.Random(5)
    ids = [client.put(f"{i}.dat", rng.choice(CONTENTS), random_tags(rng)) for i in range(25)]

    # Caen todas las secciones del índice a la vez: Ray las reinicia y cada una
    # se rellena desde los metadatos.
    for shard in gateway._tags:
        ray.kill(shard, no_restart=False)
    assert_matches_model(gateway, model)

    # Vuelven a caer, ahora seguidas de cada tipo de operación.
    for action in ["put", "add", "remove", "rename", "delete"]:
        for shard in gateway._tags:
            ray.kill(shard, no_restart=False)
        file_id = rng.choice(sorted(model.files))
        if action == "put":
            ids.append(client.put("nuevo.dat", rng.choice(CONTENTS), {"foto", "urgente"}))
        elif action == "add":
            client.add_tags(file_id, {"foto", "musica"})
        elif action == "remove":
            client.remove_tags(file_id, set(TAGS))
        elif action == "rename":
            client.rename(file_id, "renombrado.dat")
        else:
            client.delete(file_id)
        assert_matches_model(gateway, model)


def test_metadata_shard_failure_loses_its_files():
    # Limitación conocida: las fichas no tienen copias. Este test documenta qué
    # pasa hoy, para que el documento de durabilidad lo explique.
    cluster = start_cluster()
    gateway = cluster.gateway()
    victim = gateway._metadata[0]
    ids = []
    while sum(gateway._metadata_for(f) is victim for f in ids) < 2 or len(ids) < 10:
        ids.append(gateway.put(f"{len(ids)}.txt", f"contenido {len(ids)}".encode(), {"doc"}))
    lost = [f for f in ids if gateway._metadata_for(f) is victim]
    kept = [f for f in ids if gateway._metadata_for(f) is not victim]

    ray.kill(victim)

    for file_id in kept:  # los archivos de los otros shards siguen funcionando
        assert gateway.get(file_id).name.endswith(".txt")
        assert gateway.add_tags(file_id, {"sobrevive"}) == 2
    for file_id in lost:
        with pytest.raises(RayActorError):
            gateway.get(file_id)
    with pytest.raises(RayActorError):
        gateway.list()  # necesita preguntar a todos los shards
    with pytest.raises(RayActorError):
        gateway.query("doc")  # entre los candidatos hay archivos del shard caído
    assert gateway.query("sobrevive") == sorted(kept)


def test_a_client_crash_does_not_stop_the_others():
    # No hay un gateway central: cada cliente tiene el suyo y todos comparten los
    # actores, así que si un cliente se cae los demás siguen trabajando.
    cluster = start_cluster()

    def client_that_crashes(cluster):
        gateway = cluster.gateway()
        for i in range(3):
            gateway.put(f"{i}.txt", f"contenido {i}".encode(), {"doc"})
        os._exit(1)  # muere el proceso del cliente

    with pytest.raises(ray.exceptions.WorkerCrashedError):
        run_in_workers(client_that_crashes, [(cluster,)])

    survivor = cluster.gateway()
    assert len(survivor.list()) == 3
    assert len(survivor.query("doc")) == 3
    file_id = survivor.put("otro.txt", b"sigue funcionando", {"doc"})
    assert survivor.get(file_id).content == b"sigue funcionando"


@pytest.mark.parametrize("seed", [1, 2, 3])
def test_random_operations_with_random_failures(seed):
    """Mezcla al azar operaciones y fallos, y compara el sistema con el modelo.

    Se mata como mucho un almacén: con 2 copias, ningún contenido se pierde.
    """
    cluster = start_cluster()
    gateway = cluster.gateway()
    model = Model()
    client = Client(gateway, model)
    rng = random.Random(seed)
    storage_killed = False

    for step in range(80):
        live = sorted(model.files)
        action = rng.choices(
            ["put", "add", "remove", "rename", "delete", "missing", "gc", "kill_tags", "kill_storage"],
            weights=[5, 3, 3, 1, 2, 1, 1, 1, 1],
        )[0]
        if action == "put" or not live:
            client.put(f"{step}.dat", rng.choice(CONTENTS), random_tags(rng))
        elif action == "add":
            client.add_tags(rng.choice(live), random_tags(rng))
        elif action == "remove":
            client.remove_tags(rng.choice(live), random_tags(rng))
        elif action == "rename":
            client.rename(rng.choice(live), f"renombrado-{step}.dat")
        elif action == "delete":
            client.delete(rng.choice(live))
        elif action == "missing":
            target = rng.choice(sorted(model.deleted) or ["no-existe"])
            with pytest.raises(FileNotFound):
                gateway.add_tags(target, {"foto"})
        elif action == "gc":
            gateway.collect_garbage()
        elif action == "kill_tags":
            ray.kill(rng.choice(gateway._tags), no_restart=False)
        elif not storage_killed:
            ray.kill(cluster.storage[rng.choice(sorted(cluster.storage))])
            storage_killed = True
        if step % 10 == 9:
            assert_matches_model(gateway, model)

    gateway.collect_garbage()
    assert_matches_model(gateway, model)
