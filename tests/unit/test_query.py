import pytest

from tagfs.model import InvalidQuery, InvalidTag, normalize_tag
from tagfs.query import And, Not, Or, Tag, candidates, index_tags, matches, parse_query

A, B, C = Tag("a"), Tag("b"), Tag("c")


@pytest.mark.parametrize(
    "text, expected",
    [
        ("a", A),
        ("a AND b AND c", And((A, B, C))),
        ("a OR b OR c", Or((A, B, C))),
        ("a OR b AND c", Or((A, And((B, C))))),
        ("(a OR b) AND c", And((Or((A, B)), C))),
        ("a AND NOT b", And((A, Not(B)))),
        ("NOT b AND a", And((Not(B), A))),
        ("a AND NOT (b OR c)", And((A, Not(Or((B, C)))))),
        ("((a))", A),
        ("a and not b", And((A, Not(B)))),
        ("(a)AND(b)", And((A, B))),
    ],
)
def test_parse_builds_the_expected_tree(text, expected):
    assert parse_query(text) == expected


@pytest.mark.parametrize(
    "text",
    ["", "   ", "(", ")", "()", "a b", "a AND", "AND a", "a OR OR b", "(a", "a)", "NOT"],
)
def test_parse_rejects_syntax_errors(text):
    with pytest.raises(InvalidQuery):
        parse_query(text)


@pytest.mark.parametrize(
    "text",
    ["NOT a", "NOT (a OR b)", "NOT a AND NOT b", "a OR NOT b", "NOT NOT a"],
)
def test_parse_rejects_queries_without_positive_part(text):
    with pytest.raises(InvalidQuery):
        parse_query(text)


@pytest.mark.parametrize(
    "text",
    ["(" * 5000 + "a" + ")" * 5000, "NOT " * 5000 + "a"],
    ids=["parentheses", "negations"],
)
def test_parse_rejects_excessive_nesting(text):
    with pytest.raises(InvalidQuery):
        parse_query(text)


@pytest.mark.parametrize("tag", ["foto", "año-2024", "c++", "notas", "android", "or_2", "FOTO"])
def test_every_valid_tag_can_be_queried(tag):
    assert parse_query(tag) == Tag(normalize_tag(tag))


@pytest.mark.parametrize("tag", ["", "a b", "a\tb", "a(b", "a)b", "and", "OR", "Not"])
def test_tags_that_could_not_be_queried_are_rejected(tag):
    with pytest.raises(InvalidTag):
        normalize_tag(tag)


@pytest.mark.parametrize(
    "text, tags, expected",
    [
        ("a", {"a"}, True),
        ("a", {"b"}, False),
        ("a AND b", {"a", "b"}, True),
        ("a AND b", {"a"}, False),
        ("a OR b", {"b"}, True),
        ("a OR b", {"c"}, False),
        ("a AND NOT b", {"a"}, True),
        ("a AND NOT b", {"a", "b"}, False),
        ("a AND (b OR NOT c)", {"a"}, True),
        ("a AND (b OR NOT c)", {"a", "c"}, False),
    ],
)
def test_matches_evaluates_against_real_tags(text, tags, expected):
    assert matches(parse_query(text), tags) is expected


INDEX = {"a": {"f1", "f2"}, "b": {"f2", "f3"}, "c": {"f2", "f4"}}


def lookup(tag):
    return INDEX.get(tag, set())


@pytest.mark.parametrize(
    "text, expected",
    [
        ("a", {"f1", "f2"}),
        ("a AND b", {"f2"}),
        ("a OR b", {"f1", "f2", "f3"}),
        ("a AND (b OR c)", {"f2"}),
        ("a OR b AND c", {"f1", "f2"}),
        ("desconocida", set()),
    ],
)
def test_candidates_combine_the_index(text, expected):
    assert candidates(parse_query(text), lookup) == expected


@pytest.mark.parametrize("text", ["a AND NOT b", "a AND (b OR NOT c)"])
def test_candidates_never_subtract_from_the_index(text):
    assert candidates(parse_query(text), lookup) == {"f1", "f2"}


@pytest.mark.parametrize(
    "text, expected",
    [
        ("a", {"a"}),
        ("a AND a", {"a"}),
        ("a OR b AND c", {"a", "b", "c"}),
        ("(a OR b) AND c AND NOT d", {"a", "b", "c"}),
        ("a AND NOT b", {"a"}),
        ("a AND (b OR NOT c)", {"a"}),
    ],
)
def test_index_tags_are_exactly_the_tags_candidates_asks_for(text, expected):
    asked = set()

    def recording_lookup(tag):
        asked.add(tag)
        return lookup(tag)

    candidates(parse_query(text), recording_lookup)

    assert index_tags(parse_query(text)) == expected == asked
