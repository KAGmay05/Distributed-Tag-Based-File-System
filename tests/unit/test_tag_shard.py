from tagfs.model import FileMeta, IndexOp
from tagfs.tag_shard import TagShard, check_index


def test_late_add_cannot_undo_a_newer_removal():
    shard = TagShard()

    shard.apply(IndexOp("foto", "f1", False, 6))
    shard.apply(IndexOp("foto", "f1", True, 5))

    assert shard.lookup("foto") == set()


def test_late_removal_cannot_undo_a_newer_add():
    shard = TagShard()

    shard.apply(IndexOp("foto", "f1", True, 6))
    shard.apply(IndexOp("foto", "f1", False, 5))

    assert shard.lookup("foto") == {"f1"}


def test_newer_add_restores_a_removed_entry():
    shard = TagShard()

    shard.apply(IndexOp("foto", "f1", False, 6))
    shard.apply(IndexOp("foto", "f1", True, 7))

    assert shard.lookup("foto") == {"f1"}


def test_versions_are_independent_per_tag_and_file():
    shard = TagShard()

    shard.apply(IndexOp("foto", "f1", True, 9))
    shard.apply(IndexOp("foto", "f2", True, 1))
    shard.apply(IndexOp("viaje", "f1", True, 1))

    assert shard.lookup("foto") == {"f1", "f2"}
    assert shard.lookup("viaje") == {"f1"}


def test_entries_leave_out_removed_files_and_empty_tags():
    shard = TagShard()

    shard.apply(IndexOp("foto", "f1", True, 1))
    shard.apply(IndexOp("foto", "f2", True, 1))
    shard.apply(IndexOp("viaje", "f1", True, 1))
    shard.apply(IndexOp("viaje", "f1", False, 2))

    assert shard.entries() == {"foto": {"f1", "f2"}}


def test_check_index_reports_missing_and_extra_entries():
    files = [
        FileMeta("f1", "a.jpg", frozenset({"foto", "viaje"}), "hash-1"),
        FileMeta("f2", "b.jpg", frozenset({"foto"}), "hash-2"),
    ]

    assert check_index(files, {"foto": {"f1", "f2"}, "viaje": {"f1"}}) == ([], [])
    assert check_index(files, {"foto": {"f1", "f3"}, "viaje": {"f1"}}) == (
        [("foto", "f2")],
        [("foto", "f3")],
    )
