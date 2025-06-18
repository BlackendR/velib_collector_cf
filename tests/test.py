import logging
from src.api_client import fetch_station_information, fetch_station_status
from src.data_transformer import transform_station_information, transform_station_status
from src.bq_handler import test_bigquery_connection, upsert_stations_information, upsert_stations_status

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_api():
    """Test de l'API Velib"""
    logger.info("🧪 Test de l'API Velib...")
    try:
        info = fetch_station_information()
        status = fetch_station_status()
        logger.info(f"✅ API OK - {len(info)} stations d'info et {len(status)} stations de status")
        return True
    except Exception as e:
        logger.error(f"❌ Erreur API: {e}")
        return False

def test_bigquery():
    """Test de la connexion BigQuery"""
    logger.info("🧪 Test de la connexion BigQuery...")
    try:
        if test_bigquery_connection():
            logger.info("✅ Connexion BigQuery OK")
            return True
        return False
    except Exception as e:
        logger.error(f"❌ Erreur BigQuery: {e}")
        return False

def test_upsert():
    """Test de l'upsert des données"""
    logger.info("🧪 Test de l'upsert des données STATIONS INFO...")
    try:
        info = fetch_station_information()
        transformed_info = transform_station_information(info)

        status = fetch_station_status()
        transformed_status = transform_station_status(status)

        if upsert_stations_information(transformed_info) and upsert_stations_status(transformed_status) :
            logger.info("✅ Upsert OK")
            return True
        return False
    except Exception as e:
        logger.error(f"❌ Erreur upsert: {e}")
        return False

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