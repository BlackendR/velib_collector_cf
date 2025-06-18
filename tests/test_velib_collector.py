import logging
import pytest
from src.api_client import fetch_station_information, fetch_station_status
from src.data_transformer import transform_station_information, transform_station_status
from src.bq_handler import test_bigquery_connection, upsert_stations_information, upsert_stations_status

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_api():
    """Test de l'API Velib"""
    logger.info("🧪 Test de l'API Velib...")
    info = fetch_station_information()
    status = fetch_station_status()
    assert len(info) > 0, "Aucune station d'information trouvée"
    assert len(status) > 0, "Aucune station de status trouvée"
    logger.info(f"✅ API OK - {len(info)} stations d'info et {len(status)} stations de status")
    
def test_transformation():
    """Test de la transformation des données"""
    logger.info("🧪 Test de la transformation des données au format BigQuery...")
    info = fetch_station_information()
    status = fetch_station_status()

    transformed_info = transform_station_information(info)
    transformed_status = transform_station_status(status)
    
    assert transformed_info is not None, "La transformation des informations a échoué"
    assert transformed_status is not None, "La transformation des status a échoué"
    logger.info("✅ Transformation OK")

def test_bigquery():
    """Test de la connexion BigQuery"""
    logger.info("🧪 Test de la connexion BigQuery...")
    assert test_bigquery_connection(), "La connexion à BigQuery a échoué"
    logger.info("✅ Connexion BigQuery OK")

def test_upsert():
    """Test de l'upsert des données"""
    logger.info("🧪 Test de l'upsert des données...")
    info = fetch_station_information()
    transformed_info = transform_station_information(info)

    status = fetch_station_status()
    transformed_status = transform_station_status(status)

    assert upsert_stations_information(transformed_info), "L'upsert des informations a échoué"
    assert upsert_stations_status(transformed_status), "L'upsert des status a échoué"
    logger.info("✅ Upsert OK")

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