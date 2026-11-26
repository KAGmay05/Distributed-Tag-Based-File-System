import pytest

from tagfs.metadata_shard import MetadataShard
from tagfs.model import FileIdConflict, FileNotFound, IndexOp, InvalidTag


@pytest.fixture
def shard():
    shard = MetadataShard()
    shard.create("f1", "playa.jpg", {"foto", "viaje"}, "hash-1")
    return shard


def test_create_returns_first_version_and_normalized_index_ops():
    version, ops = MetadataShard().create("f1", "a.jpg", {"Viaje", "FOTO"}, "hash-1")

    assert version == 1
    assert ops == (
        IndexOp("foto", "f1", True, 1),
        IndexOp("viaje", "f1", True, 1),
    )


def test_create_repeated_with_same_data_changes_nothing(shard):
    assert shard.create("f1", "playa.jpg", {"FOTO", "viaje"}, "hash-1") == (1, ())
    assert shard.get("f1").version == 1


def test_create_repeated_with_other_data_is_a_conflict(shard):
    with pytest.raises(FileIdConflict):
        shard.create("f1", "otro.jpg", {"foto", "viaje"}, "hash-1")


def test_deleted_id_is_never_reused(shard):
    shard.delete("f1")

    with pytest.raises(FileIdConflict):
        shard.create("f1", "playa.jpg", {"foto", "viaje"}, "hash-1")


def test_add_tags_bumps_version_once_and_only_reports_new_tags(shard):
    version, ops = shard.add_tags("f1", {"FOTO", "2024", "familia"})

    assert version == 2
    assert ops == (
        IndexOp("2024", "f1", True, 2),
        IndexOp("familia", "f1", True, 2),
    )
    assert shard.get("f1").tags == {"foto", "viaje", "2024", "familia"}


def test_add_tags_without_new_tags_changes_nothing(shard):
    assert shard.add_tags("f1", {"Foto"}) == (1, ())


def test_remove_tags_only_reports_tags_that_existed(shard):
    version, ops = shard.remove_tags("f1", {"Viaje", "inexistente"})

    assert version == 2
    assert ops == (IndexOp("viaje", "f1", False, 2),)
    assert shard.get("f1").tags == {"foto"}


def test_remove_tags_without_matches_changes_nothing(shard):
    assert shard.remove_tags("f1", {"inexistente"}) == (1, ())


def test_rename_bumps_version_without_touching_the_index(shard):
    assert shard.rename("f1", "costa.jpg") == (2, ())
    assert shard.get("f1").name == "costa.jpg"


def test_rename_to_same_name_changes_nothing(shard):
    assert shard.rename("f1", "playa.jpg") == (1, ())


def test_invalid_tag_leaves_the_file_untouched(shard):
    with pytest.raises(InvalidTag):
        shard.add_tags("f1", {"valida", "and"})

    assert shard.get("f1").version == 1
    assert shard.get("f1").tags == {"foto", "viaje"}


def test_delete_removes_every_tag_from_the_index(shard):
    version, ops = shard.delete("f1")

    assert version == 2
    assert ops == (
        IndexOp("foto", "f1", False, 2),
        IndexOp("viaje", "f1", False, 2),
    )


def test_second_delete_does_not_fail(shard):
    shard.delete("f1")

    assert shard.delete("f1") == (2, ())


def test_delete_of_unknown_file_fails():
    with pytest.raises(FileNotFound):
        MetadataShard().delete("f1")


@pytest.mark.parametrize(
    "operation",
    [
        lambda s: s.get("f1"),
        lambda s: s.add_tags("f1", {"nueva"}),
        lambda s: s.remove_tags("f1", {"foto"}),
        lambda s: s.rename("f1", "nuevo.jpg"),
    ],
    ids=["get", "add_tags", "remove_tags", "rename"],
)
def test_deleted_file_behaves_as_not_found(shard, operation):
    shard.delete("f1")

    with pytest.raises(FileNotFound):
        operation(shard)


def test_listing_and_batch_read_hide_deleted_files(shard):
    shard.create("f2", "b.jpg", {"foto"}, "hash-2")
    shard.delete("f1")

    assert shard.list_ids() == ["f2"]
    assert list(shard.get_many(["f1", "f2", "desconocido"])) == ["f2"]
    assert [meta.file_id for meta in shard.live_files()] == ["f2"]


def test_changes_wait_in_the_outbox_until_ack():
    shard = MetadataShard()
    shard.create("f1", "a.jpg", {"foto"}, "hash-1")
    shard.add_tags("f1", {"viaje"})

    assert list(shard.pending()) == [("f1", 1), ("f1", 2)]

    shard.ack([("f1", 1)])
    assert list(shard.pending()) == [("f1", 2)]

    shard.ack([("f1", 2), ("f1", 2), ("desconocido", 7)])  # repetir un ack no falla
    assert shard.pending() == {}


def test_changes_that_do_not_touch_the_index_leave_nothing_pending(shard):
    shard.ack(list(shard.pending()))

    shard.create("f2", "sin-etiquetas.txt", set(), "hash-2")
    shard.rename("f1", "costa.jpg")
    shard.add_tags("f1", {"FOTO"})
    shard.remove_tags("f1", {"inexistente"})

    assert shard.pending() == {}


def test_pending_changes_carry_the_current_state_of_each_tag():
    shard = MetadataShard()
    shard.create("f1", "a.jpg", {"foto"}, "hash-1")
    shard.ack([("f1", 1)])
    shard.add_tags("f1", {"viaje"})  # versión 2, sin entregar
    shard.remove_tags("f1", {"viaje"})  # versión 3, sin entregar

    # Un TagShard reconstruido no recuerda que "viaje" se quitó: si recibiera el
    # cambio de la versión 2 tal cual, volvería a ponerla.
    assert shard.pending() == {
        ("f1", 2): (IndexOp("viaje", "f1", False, 3),),
        ("f1", 3): (IndexOp("viaje", "f1", False, 3),),
    }


def test_pending_changes_of_a_deleted_file_take_it_out_of_the_index(shard):
    shard.delete("f1")

    assert shard.pending()[("f1", 1)] == (
        IndexOp("foto", "f1", False, 2),
        IndexOp("viaje", "f1", False, 2),
    )
