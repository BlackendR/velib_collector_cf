import pytest
from unittest.mock import MagicMock

@pytest.fixture
def mock_bigquery_client():
    """Mock pour le client BigQuery"""
    mock_client = MagicMock()
    mock_client.project = "test-project"
    return mock_client

@pytest.fixture
def mock_station_info():
    """Données de test pour les informations des stations"""
    return [
        {
            "station_id": "1",
            "stationCode": "1",
            "name": "Test Station",
            "lat": 48.8566,
            "lon": 2.3522,
            "capacity": 20
        }
    ]

@pytest.fixture
def mock_station_status():
    """Données de test pour le status des stations"""
    return [
        {
            "station_id": "1",
            "stationCode": "1",
            "num_bikes_available": 10,
            "num_docks_available": 10,
            "num_bikes_available_types": [{"mechanical": 5, "ebike": 5}],
            "is_installed": True,
            "is_renting": True,
            "is_returning": True,
            "last_reported": 1625097600
        }
    ] 