"""Prueba put y get del Gateway de extremo a extremo con actores de Ray."""

import pytest
import ray

from tagfs.content import content_hash
from tagfs.model import FileNotFound, InvalidTag
from tagfs.ray_runtime import start_cluster


@pytest.fixture(scope="module")
def cluster():
    return start_cluster()


@pytest.fixture(scope="module")
def gateway(cluster):
    return cluster.gateway()


def copies_of(cluster, content):
    key = content_hash(content)
    answers = ray.get([node.has.remote(key) for node in cluster.storage.values()])
    return sum(answers)


def files_with_tag(gateway, tag):
    return ray.get(gateway._tag_shard_for(tag).lookup.remote(tag))


def test_get_returns_what_put_stored(gateway):
    file_id = gateway.put("playa.jpg", b"bytes de la playa", {"Foto", "viaje"})

    stored = gateway.get(file_id)

    assert stored.name == "playa.jpg"
    assert stored.content == b"bytes de la playa"
    assert stored.tags == {"foto", "viaje"}
    assert stored.version == 1


def test_every_file_is_found_whatever_its_shard(gateway):
    contents = {gateway.put(f"f{i}", f"contenido {i}".encode()): f"contenido {i}".encode() for i in range(20)}

    assert all(gateway.get(file_id).content == content for file_id, content in contents.items())


def test_file_without_tags_and_empty_content_can_be_stored(gateway):
    stored = gateway.get(gateway.put("vacio.txt", b""))

    assert stored.content == b""
    assert stored.tags == frozenset()


def test_put_adds_the_file_to_the_index_of_each_tag(gateway):
    file_id = gateway.put("a.jpg", b"bytes indexados", {"Indexada", "otra-indexada"})

    assert file_id in files_with_tag(gateway, "indexada")
    assert file_id in files_with_tag(gateway, "otra-indexada")


def test_same_content_gives_two_files_but_is_stored_once(cluster, gateway):
    first = gateway.put("playa.jpg", b"contenido compartido", {"original"})
    second = gateway.put("copia.jpg", b"contenido compartido", {"copia"})

    assert first != second
    assert gateway.get(first).name == "playa.jpg"
    assert gateway.get(second).name == "copia.jpg"
    assert gateway.get(first).content == gateway.get(second).content
    assert copies_of(cluster, b"contenido compartido") == 2


def test_get_of_unknown_file_fails(gateway):
    with pytest.raises(FileNotFound):
        gateway.get("no-existe")


def test_invalid_tag_stores_nothing(cluster, gateway):
    with pytest.raises(InvalidTag):
        gateway.put("a.jpg", b"no debe guardarse", {"valida", "and"})

    assert copies_of(cluster, b"no debe guardarse") == 0
    assert files_with_tag(gateway, "valida") == set()


def test_content_that_is_not_bytes_is_rejected(gateway):
    with pytest.raises(TypeError):
        gateway.put("a.txt", "texto")


def test_add_tags_returns_the_new_version_and_updates_the_index(gateway):
    file_id = gateway.put("a.jpg", b"bytes para agregar", {"inicial"})

    assert gateway.add_tags(file_id, {"Agregada"}) == 2
    assert gateway.get(file_id).tags == {"inicial", "agregada"}
    assert file_id in files_with_tag(gateway, "agregada")


def test_adding_a_tag_the_file_already_has_changes_nothing(gateway):
    file_id = gateway.put("a.jpg", b"bytes repetidos", {"repetida"})

    assert gateway.add_tags(file_id, {"repetida"}) == 1


def test_remove_tags_returns_the_new_version_and_updates_the_index(gateway):
    file_id = gateway.put("a.jpg", b"bytes para quitar", {"queda", "se-va"})

    assert gateway.remove_tags(file_id, {"se-va"}) == 2
    assert gateway.get(file_id).tags == {"queda"}
    assert file_id not in files_with_tag(gateway, "se-va")
    assert file_id in files_with_tag(gateway, "queda")


def test_rename_returns_the_new_version_and_keeps_tags_and_content(gateway):
    file_id = gateway.put("viejo.jpg", b"bytes renombrados", {"renombrada"})

    assert gateway.rename(file_id, "nuevo.jpg") == 2
    stored = gateway.get(file_id)
    assert (stored.name, stored.content, stored.tags) == ("nuevo.jpg", b"bytes renombrados", {"renombrada"})
    assert file_id in files_with_tag(gateway, "renombrada")


def test_invalid_tag_in_add_tags_changes_nothing(gateway):
    file_id = gateway.put("a.jpg", b"bytes con etiqueta mala")

    with pytest.raises(InvalidTag):
        gateway.add_tags(file_id, {"buena", "or"})

    assert gateway.get(file_id).version == 1


def test_delete_hides_the_file_and_removes_it_from_the_index(gateway):
    file_id = gateway.put("a.jpg", b"bytes borrados", {"borrable"})

    gateway.delete(file_id)

    with pytest.raises(FileNotFound):
        gateway.get(file_id)
    assert file_id not in files_with_tag(gateway, "borrable")


def test_delete_can_be_repeated(gateway):
    file_id = gateway.put("a.jpg", b"bytes borrados dos veces")

    gateway.delete(file_id)
    gateway.delete(file_id)


def test_delete_of_unknown_file_fails(gateway):
    with pytest.raises(FileNotFound):
        gateway.delete("no-existe")


@pytest.mark.parametrize(
    "operation",
    [
        lambda g, file_id: g.add_tags(file_id, {"nueva"}),
        lambda g, file_id: g.remove_tags(file_id, {"x"}),
        lambda g, file_id: g.rename(file_id, "otro.jpg"),
    ],
    ids=["add_tags", "remove_tags", "rename"],
)
def test_deleted_file_cannot_be_modified(gateway, operation):
    file_id = gateway.put("a.jpg", b"bytes de archivo eliminado", {"x"})
    gateway.delete(file_id)

    with pytest.raises(FileNotFound):
        operation(gateway, file_id)


def test_delete_keeps_the_bytes_shared_with_another_file(cluster, gateway):
    deleted = gateway.put("playa.jpg", b"bytes compartidos al borrar")
    kept = gateway.put("copia.jpg", b"bytes compartidos al borrar")

    gateway.delete(deleted)

    assert gateway.get(kept).content == b"bytes compartidos al borrar"
    assert copies_of(cluster, b"bytes compartidos al borrar") == 2


def test_garbage_collection_removes_the_bytes_of_deleted_files(cluster, gateway):
    file_id = gateway.put("a.jpg", b"bytes para recolectar")
    gateway.delete(file_id)

    assert gateway.collect_garbage() >= 1

    assert copies_of(cluster, b"bytes para recolectar") == 0


def test_garbage_collection_keeps_shared_bytes_until_no_file_uses_them(cluster, gateway):
    first = gateway.put("playa.jpg", b"bytes compartidos al recolectar")
    second = gateway.put("copia.jpg", b"bytes compartidos al recolectar")

    gateway.delete(first)
    gateway.collect_garbage()
    assert gateway.get(second).content == b"bytes compartidos al recolectar"

    gateway.delete(second)
    gateway.collect_garbage()
    assert copies_of(cluster, b"bytes compartidos al recolectar") == 0


def test_garbage_collection_removes_bytes_left_by_a_failed_put(cluster, gateway):
    # Simula un put que guardó el contenido y falló antes de crear la ficha.
    gateway._content.put(b"bytes huerfanos")

    gateway.collect_garbage()

    assert copies_of(cluster, b"bytes huerfanos") == 0


def test_live_files_are_readable_after_garbage_collection(cluster, gateway):
    file_id = gateway.put("vivo.jpg", b"bytes que sobreviven", {"vivo"})

    gateway.collect_garbage()

    assert gateway.get(file_id).content == b"bytes que sobreviven"
    assert copies_of(cluster, b"bytes que sobreviven") == 2


def test_get_survives_the_death_of_a_storage_node():
    cluster = start_cluster()
    gateway = cluster.gateway()
    file_id = gateway.put("playa.jpg", b"bytes replicados", {"foto"})
    first_replica = gateway._content.replicas(content_hash(b"bytes replicados"))[0]

    ray.kill(cluster.storage[first_replica])

    assert gateway.get(file_id).content == b"bytes replicados"
