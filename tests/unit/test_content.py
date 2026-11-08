from tagfs.content import ContentCorrupted, ContentNotFound, content_hash
from tagfs.model import TagFSError


def test_known_value_of_empty_content():
    assert content_hash(b"") == (
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    )


def test_errors_belong_to_the_project_family():
    assert issubclass(ContentNotFound, TagFSError)
    assert issubclass(ContentCorrupted, TagFSError)
