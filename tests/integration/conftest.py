import pytest
import ray

from tagfs.gateway import start_ray


@pytest.fixture(scope="session", autouse=True)
def ray_cluster():
    start_ray(num_cpus=2, include_dashboard=False, log_to_driver=False)
    yield
    ray.shutdown()
