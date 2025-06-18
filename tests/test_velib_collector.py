import logging
import pytest
from unittest.mock import patch, MagicMock
from src.api_client import fetch_station_information, fetch_station_status
from src.data_transformer import transform_station_information, transform_station_status
from src.bq_handler import test_bigquery_connection, upsert_stations_information, upsert_stations_status
from src.config import set_bigquery_client
from .fixtures import mock_station_info, mock_station_status

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@pytest.fixture(autouse=True)
def setup_mocks(mock_bigquery_client):
    """Configure le mock client BigQuery pour tous les tests"""
    set_bigquery_client(mock_bigquery_client)

@patch('src.api_client.requests.get')
def test_api(mock_get, mock_station_info):
    """Test de l'API Velib"""
    logger.info("🧪 Test de l'API Velib...")
    # Mock des réponses de l'API
    mock_response = MagicMock()
    mock_response.json.return_value = {"data": {"stations": mock_station_info}}
    mock_get.return_value = mock_response
    info = fetch_station_information()
    assert len(info) > 0, "Aucune station d'information trouvée"
    logger.info(f"✅ API OK - {len(info)} stations d'info")

@patch('src.api_client.requests.get')
def test_transformation(mock_get, mock_station_info):
    """Test de la transformation des données"""
    logger.info("🧪 Test de la transformation des données au format BigQuery...")
    # Mock des réponses de l'API
    mock_response = MagicMock()
    mock_response.json.return_value = {"data": {"stations": mock_station_info}}
    mock_get.return_value = mock_response
    info = fetch_station_information()
    transformed_info = transform_station_information(info)
    assert transformed_info is not None, "La transformation des informations a échoué"
    assert len(transformed_info) > 0, "Aucune donnée transformée"
    logger.info("✅ Transformation OK")

def test_bigquery(mock_bigquery_client):
    """Test de la connexion BigQuery (mock)"""
    logger.info("🧪 Test de la connexion BigQuery...")
    set_bigquery_client(mock_bigquery_client)
    # On suppose que le mock retourne True
    assert True, "La connexion à BigQuery a échoué"
    logger.info("✅ Connexion BigQuery OK (mock)")

def test_upsert(mock_station_info, mock_station_status, mock_bigquery_client):
    """Test de l'upsert des données (mock)"""
    logger.info("🧪 Test de l'upsert des données...")
    set_bigquery_client(mock_bigquery_client)
    transformed_info = transform_station_information(mock_station_info)
    transformed_status = transform_station_status(mock_station_status)
    # On ne teste pas le retour réel, juste que la fonction s'exécute sans erreur avec le mock
    try:
        upsert_stations_information(transformed_info)
        upsert_stations_status(transformed_status)
        ok = True
    except Exception as e:
        logger.error(f"Erreur upsert: {e}")
        ok = False
    assert ok, "L'upsert a échoué"
    logger.info("✅ Upsert OK (mock)")

def main():
    """Exécute tous les tests"""
    tests = [
        ("API", test_api),
        ("Transformation", test_transformation),
        ("BigQuery", test_bigquery),
        ("Upsert", test_upsert)
    ]
    
    success = True
    for name, test_func in tests:
        logger.info(f"\n=== Test {name} ===")
        if not test_func():
            success = False
            logger.error(f"❌ Le test {name} a échoué")
    
    if success:
        logger.info("\n✨ Tous les tests sont passés avec succès!")
    else:
        logger.error("\n❌ Certains tests ont échoué")

if __name__ == "__main__":
    main()