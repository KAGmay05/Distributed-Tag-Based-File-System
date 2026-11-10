"""Prueba qué hace el Gateway cuando muere un TagShard.

Todos los tests comparten un Gateway, así que cada uno usa etiquetas propias.
"""

import pytest
import ray

from tagfs.gateway import Gateway


@pytest.fixture(scope="module")
def gateway():
    return Gateway()


def kill_index_of(gateway, tag):
    ray.kill(gateway._tag_shard_for(tag))


def raw_index(gateway, tag):
    # Lo que guarda el TagShard, sin el filtro de metadatos que aplica query.
    return ray.get(gateway._tag_shard_for(tag).lookup.remote(tag))


def test_put_succeeds_and_is_queryable_after_its_tag_shard_dies(gateway):
    old = gateway.put("viejo.jpg", b"v", {"i1-foto"})
    kill_index_of(gateway, "i1-foto")

    new = gateway.put("nuevo.jpg", b"n", {"i1-foto"})

    assert gateway.get(new).name == "nuevo.jpg"
    assert gateway.query("i1-foto") == sorted([old, new])


def test_query_recovers_the_index_of_a_dead_tag_shard(gateway):
    file_id = gateway.put("a.txt", b"x", {"i2-doc"})
    kill_index_of(gateway, "i2-doc")

    assert gateway.query("i2-doc") == [file_id]


@pytest.mark.parametrize("operation", ["add", "remove", "delete"])
def test_changes_reach_the_index_after_its_tag_shard_dies(gateway, operation):
    tag = f"i3-{operation}"
    kept = gateway.put("kept.txt", b"k", {tag})
    changed = gateway.put("changed.txt", b"c", {tag} if operation != "add" else set())
    kill_index_of(gateway, tag)

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


def test_rebuilt_index_leaves_out_deleted_files_and_removed_tags(gateway):
    live = gateway.put("vivo.txt", b"1", {"i4-doc"})
    deleted = gateway.put("borrado.txt", b"2", {"i4-doc"})
    untagged = gateway.put("sin.txt", b"3", {"i4-doc"})
    gateway.delete(deleted)
    gateway.remove_tags(untagged, {"i4-doc"})
    kill_index_of(gateway, "i4-doc")

    gateway.query("i4-doc")  # detecta el shard muerto y lo reconstruye

    assert raw_index(gateway, "i4-doc") == {live}


def test_only_the_dead_tag_shard_is_replaced(gateway):
    gateway.put("a.txt", b"x", {"i5-doc"})
    dead = gateway._tag_index("i5-doc")
    before = list(gateway._tags)
    ray.kill(before[dead])

    gateway.query("i5-doc")

    assert gateway._tags[dead] is not before[dead]
    assert all(gateway._tags[i] is before[i] for i in range(len(before)) if i != dead)
