import pytest

from tagfs.content import ContentCorrupted, ContentNotFound, content_hash
from tagfs.model import TagFSError


def test_same_bytes_give_same_hash():
    assert content_hash(b"hola") == content_hash(b"hola")


def test_changing_one_letter_changes_the_hash():
    assert content_hash(b"hola") != content_hash(b"Hola")


def test_known_value_of_empty_content():
    assert content_hash(b"") == (
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    )


def test_text_is_rejected():
    with pytest.raises(TypeError):
        content_hash("hola")


def test_errors_belong_to_the_project_family():
    assert issubclass(ContentNotFound, TagFSError)
    assert issubclass(ContentCorrupted, TagFSError)
