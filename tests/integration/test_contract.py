"""Ejecuta el contrato del gateway sobre actores de Ray reales.

El contrato también se ejecuta sin Ray en tests/unit/test_gateway.py: las dos
formas deben cumplirlo igual. Cada test recibe un sistema nuevo y vacío, así
que se usan pocos actores.
"""

import pytest
from gateway_contract import GatewayContract

from tagfs.ray_runtime import start_cluster


class TestClusterClient(GatewayContract):
    @pytest.fixture
    def gateway(self):
        return start_cluster(metadata_shards=2, tag_shards=2, storage_nodes=2).gateway()
