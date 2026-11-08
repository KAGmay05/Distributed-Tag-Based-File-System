import pytest

from tagfs.content import ContentNotFound
from tagfs.fake_content_store import FakeContentStore


def test_get_of_unknown_hash_fails():
    with pytest.raises(ContentNotFound):
        FakeContentStore().get("desconocido")
