"""Ejecuta el contrato del gateway y las garantías del mantenimiento sobre actores de Ray reales.

Los dos también se ejecutan sin Ray en tests/unit/test_gateway.py: las dos
formas deben cumplirlos igual. Cada test recibe un sistema nuevo y vacío, así
que se usan pocos actores.
"""

import pytest
from gateway_contract import GatewayContract
from maintenance_checks import MaintenanceChecks

from tagfs.ray_runtime import start_cluster


def small_cluster_gateway():
    return start_cluster(metadata_shards=2, tag_shards=2, storage_nodes=2).gateway()


class TestClusterClient(GatewayContract):
    @pytest.fixture
    def gateway(self):
        return small_cluster_gateway()


class TestClusterMaintenance(MaintenanceChecks):
    @pytest.fixture
    def gateway(self):
        return small_cluster_gateway()
