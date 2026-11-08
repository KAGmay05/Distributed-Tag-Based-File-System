from collections import Counter

import pytest

from tagfs.ring import HashRing

KEYS = [f"key-{i}" for i in range(3000)]


def make_ring(*node_ids):
    ring = HashRing()
    for node_id in node_ids:
        ring.add_node(node_id)
    return ring


def test_same_key_always_gets_the_same_nodes():
    assert make_ring("A", "B", "C").nodes_for("9b2e", 2) == make_ring("C", "B", "A").nodes_for("9b2e", 2)


def test_replicas_are_on_different_nodes():
    ring = make_ring("A", "B", "C")
    assert all(len(set(ring.nodes_for(key, 2))) == 2 for key in KEYS)


def test_asking_for_too_many_replicas_returns_every_node():
    assert set(make_ring("A", "B").nodes_for("9b2e", 5)) == {"A", "B"}


def test_empty_ring_has_no_nodes():
    assert HashRing().nodes_for("9b2e", 2) == []


def test_rejects_non_positive_replica_count():
    with pytest.raises(ValueError):
        make_ring("A").nodes_for("9b2e", 0)


def test_removed_node_is_never_chosen():
    ring = make_ring("A", "B", "C")
    ring.remove_node("B")
    assert all("B" not in ring.nodes_for(key, 2) for key in KEYS)


def test_adding_a_node_moves_few_keys():
    before = make_ring("A", "B", "C")
    after = make_ring("A", "B", "C", "D")
    moved = sum(before.nodes_for(key, 1) != after.nodes_for(key, 1) for key in KEYS)
    assert moved < len(KEYS) * 0.40


def test_keys_spread_evenly():
    ring = make_ring("A", "B", "C")
    counts = Counter(ring.nodes_for(key, 1)[0] for key in KEYS)
    assert all(700 <= count <= 1300 for count in counts.values())
