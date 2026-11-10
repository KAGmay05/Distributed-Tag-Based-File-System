import pytest

from tagfs.content import ContentCorrupted, ContentNotFound, NodeUnreachable, content_hash
from tagfs.storage_node import StorageNode

DATA = b"hola"
KEY = content_hash(DATA)


def test_get_returns_what_put_stored():
    node = StorageNode()
    node.put(KEY, DATA)
    assert node.get(KEY) == DATA


def test_put_rejects_bytes_that_do_not_match_the_key():
    node = StorageNode()
    with pytest.raises(ContentCorrupted):
        node.put(KEY, b"datos danados")
    assert not node.has(KEY)


def test_repeated_put_keeps_a_single_copy():
    node = StorageNode()
    node.put(KEY, DATA)
    node.put(KEY, DATA)
    assert node.list_keys() == [KEY]


def test_find_returns_the_bytes_or_none():
    node = StorageNode()
    node.put(KEY, DATA)

    assert node.find(KEY) == DATA
    assert StorageNode().find(KEY) is None


def test_disconnected_node_rejects_every_request_but_keeps_its_data():
    node = StorageNode()
    node.put(KEY, DATA)

    node.disconnect()
    for operation in [
        lambda: node.put(KEY, DATA),
        lambda: node.get(KEY),
        lambda: node.find(KEY),
        lambda: node.has(KEY),
        lambda: node.delete(KEY),
        lambda: node.list_keys(),
    ]:
        with pytest.raises(NodeUnreachable):
            operation()

    node.reconnect()
    assert node.get(KEY) == DATA


def test_get_of_unknown_key_fails():
    with pytest.raises(ContentNotFound):
        StorageNode().get(KEY)


def test_delete_removes_the_content_and_can_be_repeated():
    node = StorageNode()
    node.put(KEY, DATA)
    node.delete(KEY)
    node.delete(KEY)
    assert not node.has(KEY)


def test_each_node_keeps_its_own_data():
    node_a = StorageNode()
    node_b = StorageNode()
    node_a.put(KEY, DATA)
    assert not node_b.has(KEY)
