import hashlib

import pytest

from tagfs.content_store import FakeContentStore
from tagfs.model import ContentNotFound


def test_put_returns_the_sha256_of_the_content():
    assert FakeContentStore().put(b"hola") == hashlib.sha256(b"hola").hexdigest()


def test_get_returns_what_was_stored():
    store = FakeContentStore()

    assert store.get(store.put(b"hola")) == b"hola"


def test_identical_content_shares_a_hash():
    store = FakeContentStore()

    assert store.put(b"igual") == store.put(b"igual")


def test_get_of_unknown_hash_fails():
    with pytest.raises(ContentNotFound):
        FakeContentStore().get("desconocido")
