import pytest

from tagfs.content import (
    ContentCorrupted,
    ContentNotFound,
    NodeUnreachable,
    StorageUnavailable,
    content_hash,
)
from tagfs.model import TagFSError


def test_known_value_of_empty_content():
    assert content_hash(b"") == (
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    )


@pytest.mark.parametrize(
    "error", [ContentNotFound, ContentCorrupted, StorageUnavailable, NodeUnreachable]
)
def test_errors_belong_to_the_project_family(error):
    assert issubclass(error, TagFSError)
