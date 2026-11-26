import pytest

from tagfs.content import ContentNotFound
from tagfs.fake_content_store import FakeContentStore


def test_get_of_unknown_hash_fails():
    with pytest.raises(ContentNotFound):
        FakeContentStore().get("desconocido")


def test_sweep_deletes_only_content_that_is_not_live():
    store = FakeContentStore()
    kept = store.put(b"vivo")
    garbage = store.put(b"basura")

    assert store.sweep({kept}) == 1

    assert store.get(kept) == b"vivo"
    with pytest.raises(ContentNotFound):
        store.get(garbage)


def test_reconcile_collects_garbage_and_reports_missing_live_content():
    store = FakeContentStore()
    kept = store.put(b"vivo")
    store.put(b"basura")

    report = store.reconcile({kept, "perdido"})

    assert report == ([], 0, 0, 1, ["perdido"])
