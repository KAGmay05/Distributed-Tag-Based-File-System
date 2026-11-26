import pytest

from tagfs.ray_runtime import start_cluster


@pytest.mark.parametrize(
    "options",
    [
        {"metadata_shards": 0},
        {"tag_shards": 0},
        {"storage_nodes": 0},
        {"replication_factor": 0},
        {"storage_nodes": 1, "replication_factor": 2},
    ],
)
def test_invalid_cluster_sizes_fail_before_starting_ray(monkeypatch, options):
    def unexpected_start():
        pytest.fail("No se debe arrancar Ray para una configuración inválida")

    monkeypatch.setattr("tagfs.ray_runtime.ray.is_initialized", unexpected_start)

    with pytest.raises(ValueError):
        start_cluster(**options)
