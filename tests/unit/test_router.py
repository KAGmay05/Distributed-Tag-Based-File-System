from collections import Counter

import pytest

from tagfs.router import shard_for


@pytest.mark.parametrize(
    "key, n, expected",
    [
        ("foto", 4, 3),
        ("foto", 1048576, 671147),
        ("viaje", 1048576, 672987),
        ("file-1", 1048576, 133034),
    ],
)
def test_values_are_pinned_so_every_process_agrees(key, n, expected):
    assert shard_for(key, n) == expected


@pytest.mark.parametrize("n", [1, 2, 7])
def test_result_stays_in_range(n):
    assert all(0 <= shard_for(f"key-{i}", n) < n for i in range(1000))


def test_keys_spread_evenly():
    counts = Counter(shard_for(f"key-{i}", 8) for i in range(8000))
    assert len(counts) == 8
    assert all(800 <= count <= 1200 for count in counts.values())


@pytest.mark.parametrize("n", [0, -1])
def test_rejects_non_positive_shard_count(n):
    with pytest.raises(ValueError):
        shard_for("foto", n)
