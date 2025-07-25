"""Pytest configuration and common fixtures"""

from collections.abc import Generator
from unittest.mock import Mock

import pytest


@pytest.fixture
def sample_launch_record() -> dict:
    """Sample launch record for testing"""
    return {
        "id": "test_id_123",
        "name": "Test Launch",
        "details": "This is a test launch for unit testing",
        "flight_number": 100,
        "launchpad": "test_launchpad",
        "date_utc": "2021-03-15T10:00:00Z",
        "rocket": "test_rocket_id",
        "payloads": ["payload1", "payload2"],
        "ships": ["ship1"],
        "cores": ["core1"],
        "success": True,
        "extra_field": "should_be_filtered_out",
    }


@pytest.fixture
def mock_dlt_pipeline() -> Mock:
    """Mock dlt pipeline for testing"""
    mock_pipeline = Mock()
    mock_pipeline.run.return_value = Mock()
    return mock_pipeline


@pytest.fixture
def mock_rest_api_resources() -> Generator[Mock, None, None]:
    """Mock rest_api_resources for testing"""
    with pytest.MonkeyPatch().context() as m:
        mock_resources = Mock()
        mock_resources.return_value = []
        m.setattr("dlt_dbt_dagster.dlt.spacex_pipeline.rest_api_resources", mock_resources)
        yield mock_resources
