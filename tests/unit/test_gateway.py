import pytest
from gateway_contract import GatewayContract

from tagfs.content_store import FakeContentStore
from tagfs.gateway import Gateway
from tagfs.metadata_shard import MetadataShard
from tagfs.model import ContentIntegrityError, InvalidTag
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


class RecordingStore(FakeContentStore):
    def __init__(self):
        super().__init__()
        self.put_calls = 0

    def put(self, content):
        self.put_calls += 1
        return super().put(content)


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

    def get_many(self, file_ids):
        self.get_many_calls += 1
        return super().get_many(file_ids)


def test_invalid_tag_fails_before_storing_content():
    store = RecordingStore()
    gateway, _, _ = build_gateway(store)

    with pytest.raises(InvalidTag):
        gateway.put("a.txt", b"x", {"valida", "or"})

    assert store.put_calls == 0


def test_wrong_hash_from_store_stops_put_before_metadata():
    gateway, _, _ = build_gateway(WrongHashStore())

    with pytest.raises(ContentIntegrityError):
        gateway.put("a.txt", b"x", {"doc"})

    assert gateway.list() == []


def test_get_detects_corrupted_content():
    gateway, _, _ = build_gateway(CorruptingStore())
    file_id = gateway.put("a.txt", b"x", {"doc"})

    with pytest.raises(ContentIntegrityError):
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


def test_query_filters_stale_index_entries_with_metadata():
    gateway, metadata, _ = build_gateway()
    file_id = gateway.put("a.txt", b"x", {"doc"})

    metadata[shard_for(file_id, len(metadata))].remove_tags(file_id, {"doc"})

    assert gateway.query("doc") == []
