import pytest

from tagfs.model import InvalidTag, normalize_tag, normalize_tags


@pytest.mark.parametrize("tags", [["Foto", "2024"], ("FOTO", "2024"), {"foto", "2024", "Foto"}])
def test_any_collection_of_tags_is_normalized(tags):
    assert normalize_tags(tags) == {"foto", "2024"}


def test_no_tags_is_valid():
    assert normalize_tags(()) == frozenset()


@pytest.mark.parametrize("text", ["foto", "f", ""])
def test_a_string_is_not_a_collection_of_tags(text):
    # Un texto se puede recorrer letra a letra: sin esta regla, "foto" guardaría f, o y t.
    with pytest.raises(InvalidTag):
        normalize_tags(text)


@pytest.mark.parametrize("tag", [None, 2024, b"foto", ("foto",)])
def test_a_tag_must_be_text(tag):
    with pytest.raises(InvalidTag):
        normalize_tag(tag)
    with pytest.raises(InvalidTag):
        normalize_tags(["valida", tag])


def test_bytes_are_not_a_collection_of_tags():
    with pytest.raises(InvalidTag):
        normalize_tags(b"foto")
