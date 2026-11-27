import pytest
from gateway_contract import GatewayContract
from maintenance_checks import MaintenanceChecks

from tagfs.content import ContentCorrupted
from tagfs.fake_content_store import FakeContentStore
from tagfs.gateway import Gateway, StoredFile
from tagfs.metadata_shard import MetadataShard
from tagfs.model import InvalidTag
from tagfs.router import shard_for
from tagfs.tag_shard import TagShard


def build_gateway(content=None, metadata_shards=3, tag_shards=4):
    metadata = [MetadataShard() for _ in range(metadata_shards)]
    tags = [TagShard() for _ in range(tag_shards)]
    return Gateway(metadata, tags, content or FakeContentStore()), metadata, tags


class TestInMemoryGateway(GatewayContract):
    @pytest.fixture
    def gateway(self):
        return build_gateway()[0]


class TestInMemoryMaintenance(MaintenanceChecks):
    @pytest.fixture
    def gateway(self):
        return build_gateway()[0]


class RecordingStore(FakeContentStore):
    def __init__(self):
        super().__init__()
        self.put_calls = 0
        self.get_calls = 0

    def put(self, content):
        self.put_calls += 1
        return super().put(content)

    def get(self, content_hash):
        self.get_calls += 1
        return super().get(content_hash)


class WrongHashStore(FakeContentStore):
    def put(self, content):
        super().put(content)
        return "hash-incorrecto"


class CorruptingStore(FakeContentStore):
    def get(self, content_hash):
        return super().get(content_hash) + b"!"


class CountingShard(MetadataShard):
    def __init__(self):
        super().__init__()
        self.get_many_calls = 0
        self.delete_matching_calls = 0

    def get_many(self, file_ids):
        self.get_many_calls += 1
        return super().get_many(file_ids)

    def delete_matching(self, file_ids, expr):
        self.delete_matching_calls += 1
        return super().delete_matching(file_ids, expr)


class CountingTagShard(TagShard):
    def __init__(self):
        super().__init__()
        self.apply_many_calls = 0

    def apply_many(self, ops):
        self.apply_many_calls += 1
        super().apply_many(ops)


@pytest.mark.parametrize("tags", [{"valida", "or"}, {"valida", 2024}, "foto"])
def test_invalid_tags_fail_before_storing_content(tags):
    store = RecordingStore()
    gateway, _, _ = build_gateway(store)

    with pytest.raises(InvalidTag):
        gateway.put("a.txt", b"x", tags)

    assert store.put_calls == 0


def test_invalid_name_fails_before_storing_content():
    store = RecordingStore()
    gateway, _, _ = build_gateway(store)

    with pytest.raises(TypeError):
        gateway.put(None, b"x", {"doc"})

    assert store.put_calls == 0


def test_list_does_not_read_any_content():
    store = RecordingStore()
    gateway, _, _ = build_gateway(store)
    gateway.put("a.txt", b"x", {"doc"})

    assert len(gateway.list()) == len(gateway.list("doc")) == 1

    assert store.get_calls == 0


def test_wrong_hash_from_store_stops_put_before_metadata():
    gateway, _, _ = build_gateway(WrongHashStore())

    with pytest.raises(ContentCorrupted):
        gateway.put("a.txt", b"x", {"doc"})

    assert gateway.list() == []


def test_get_detects_corrupted_content():
    gateway, _, _ = build_gateway(CorruptingStore())
    file_id = gateway.put("a.txt", b"x", {"doc"})

    with pytest.raises(ContentCorrupted):
        gateway.get(file_id)


def test_each_file_lives_only_in_its_metadata_shard():
    gateway, metadata, _ = build_gateway()

    created = [gateway.put(f"{i}.txt", b"x", set()) for i in range(30)]

    for index, shard in enumerate(metadata):
        expected = [f for f in created if shard_for(f, len(metadata)) == index]
        assert shard.list_ids() == sorted(expected)


def test_each_tag_lives_only_in_its_tag_shard():
    gateway, _, tags = build_gateway()

    file_id = gateway.put("a.txt", b"x", {"foto", "viaje", "2024", "trabajo", "familia"})

    for tag in ["foto", "viaje", "2024", "trabajo", "familia"]:
        home = shard_for(tag, len(tags))
        for index, shard in enumerate(tags):
            assert shard.lookup(tag) == ({file_id} if index == home else set())


def test_removals_reach_the_index():
    # Las consultas filtran contra los metadatos, así que no detectarían una entrada sobrante.
    gateway, _, tags = build_gateway()
    file_id = gateway.put("a.txt", b"x", {"foto", "viaje"})

    gateway.remove_tags(file_id, {"foto"})
    assert tags[shard_for("foto", len(tags))].lookup("foto") == set()

    gateway.delete(file_id)
    assert tags[shard_for("viaje", len(tags))].lookup("viaje") == set()


def test_query_reads_each_metadata_shard_once():
    metadata = [CountingShard() for _ in range(3)]
    gateway = Gateway(metadata, [TagShard() for _ in range(4)], FakeContentStore())
    created = [gateway.put(f"{i}.txt", b"x", {"doc"}) for i in range(30)]

    assert gateway.query("doc") == sorted(created)
    assert [shard.get_many_calls for shard in metadata] == [1, 1, 1]

    assert len(gateway.list("doc")) == 30  # describirlos no cuesta llamadas de más
    assert [shard.get_many_calls for shard in metadata] == [2, 2, 2]


def test_query_filters_stale_index_entries_with_metadata():
    gateway, metadata, _ = build_gateway()
    file_id = gateway.put("a.txt", b"x", {"doc"})

    metadata[shard_for(file_id, len(metadata))].remove_tags(file_id, {"doc"})

    assert gateway.query("doc") == []


def test_a_change_by_query_checks_the_query_again_in_the_metadata():
    gateway, metadata, _ = build_gateway()
    file_id = gateway.put("a.txt", b"x", {"doc", "foto"})
    # Los metadatos pierden "doc" sin avisar al índice, que lo sigue dando como candidato.
    metadata[shard_for(file_id, len(metadata))].remove_tags(file_id, {"doc"})

    assert gateway.delete_by_query("doc") == ({}, [])
    assert gateway.add_tags_by_query("doc", {"nueva"}) == ({}, [])

    assert gateway.get(file_id).tags == {"foto"}


def test_a_change_by_query_calls_each_shard_once():
    metadata = [CountingShard() for _ in range(3)]
    tags = [CountingTagShard() for _ in range(4)]
    gateway = Gateway(metadata, tags, FakeContentStore())
    created = [gateway.put(f"{i}.txt", b"x", {"doc"}) for i in range(30)]
    before = [shard.apply_many_calls for shard in tags]

    assert gateway.delete_by_query("doc") == ({file_id: 2 for file_id in created}, [])

    assert [shard.delete_matching_calls for shard in metadata] == [1, 1, 1]
    # Los 30 cambios del índice son de "doc" y llegan a su TagShard en una sola llamada.
    home = shard_for("doc", len(tags))
    calls = [shard.apply_many_calls - old for shard, old in zip(tags, before)]
    assert calls == [1 if index == home else 0 for index in range(len(tags))]


def test_stored_file_supports_attributes_and_tuple_access():
    gateway, _, _ = build_gateway()
    file_id = gateway.put("a.txt", b"x")

    stored = gateway.get(file_id)

    assert isinstance(stored, StoredFile)
    assert stored.name == stored[0] == "a.txt"
    assert stored.content == stored[1] == b"x"
    assert stored.tags == frozenset()


class ClientStopped(Exception):
    """El cliente se detiene, como si su proceso muriera."""


def stop_before_the_index(gateway):
    # El cliente se cae justo después de que los metadatos aceptan el cambio.
    def stop(ops):
        raise ClientStopped

    gateway._send_to_index = stop
    return gateway


def test_a_file_left_out_of_the_index_is_found_after_repair():
    store = FakeContentStore()
    gateway, metadata, tags = build_gateway(store)
    crashing = stop_before_the_index(Gateway(metadata, tags, store))

    with pytest.raises(ClientStopped):
        crashing.put("a.txt", b"x", {"doc"})

    [(file_id, *_)] = gateway.list()  # los metadatos lo tienen
    assert gateway.query("doc") == []  # pero el índice no
    assert gateway.verify_index() == ([("doc", file_id)], [])

    assert gateway.repair_index() == 1

    assert gateway.query("doc") == [file_id]
    assert gateway.verify_index() == ([], [])
    assert gateway.repair_index() == 0  # no quedó nada pendiente


@pytest.mark.parametrize(
    "operation,args,missing_tags,extra_tags",
    [
        ("add_tags", ({"urgente"},), ["urgente"], []),
        ("remove_tags", ({"doc"},), [], ["doc"]),
        ("delete", (), [], ["doc"]),
    ],
)
def test_a_change_left_out_of_the_index_is_delivered_by_repair(
    operation, args, missing_tags, extra_tags
):
    store = FakeContentStore()
    gateway, metadata, tags = build_gateway(store)
    file_id = gateway.put("a.txt", b"x", {"doc"})
    crashing = stop_before_the_index(Gateway(metadata, tags, store))

    with pytest.raises(ClientStopped):
        getattr(crashing, operation)(file_id, *args)

    assert gateway.verify_index() == (
        [(tag, file_id) for tag in missing_tags],
        [(tag, file_id) for tag in extra_tags],
    )
    assert gateway.repair_index() == 1
    assert gateway.verify_index() == ([], [])


@pytest.mark.parametrize(
    "operation,args,missing_tags,extra_tags",
    [
        ("add_tags_by_query", ({"urgente"},), ["urgente"], []),
        ("remove_tags_by_query", ({"doc"},), [], ["doc"]),
        ("delete_by_query", (), [], ["doc"]),
    ],
)
def test_changes_by_query_left_out_of_the_index_are_delivered_by_repair(
    operation, args, missing_tags, extra_tags
):
    store = FakeContentStore()
    gateway, metadata, tags = build_gateway(store)
    created = sorted(gateway.put(f"{i}.txt", b"x", {"doc"}) for i in range(10))
    crashing = stop_before_the_index(Gateway(metadata, tags, store))

    with pytest.raises(ClientStopped):
        getattr(crashing, operation)("doc", *args)

    assert gateway.verify_index() == (
        [(tag, file_id) for tag in missing_tags for file_id in created],
        [(tag, file_id) for tag in extra_tags for file_id in created],
    )
    assert gateway.repair_index() == 10
    assert gateway.verify_index() == ([], [])


@pytest.mark.parametrize("metadata,tags", [([], [TagShard()]), ([MetadataShard()], [])])
def test_gateway_rejects_empty_injected_shards(metadata, tags):
    with pytest.raises(ValueError):
        Gateway(metadata, tags, FakeContentStore())
