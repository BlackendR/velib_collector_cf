import logging
from .api_client import fetch_station_information, fetch_station_status
from .data_transformer import transform_station_information, transform_station_status
from .bq_handler import upsert_stations_information, upsert_stations_status

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def collect_velib_data():
    """
    Fonction d'orchestration
    """

    try:
        # 1. Récupération des données via l'API Velib

        logger.info("📡 Récupération des données via l'API Velib...")
        info_data = fetch_station_information()
        status_data = fetch_station_status()

        # 2. Transformation des données reçues
        logger.info("⚙️ Transformation des données...")
        transformed_info_data = transform_station_information(info_data)
        transformed_status_data = transform_station_status(status_data)

        # 3. Insertion des données dans BigQuery
        logger.info("🛫 Insertion des données dans BigQuery...")
        success_info = upsert_stations_information(transformed_info_data)
        success_status = upsert_stations_status(transformed_status_data)
        success = success_info and success_status

        if success:
            logger.info("✅ Collecte terminée avec succès")
            return "Success", 200
        else:
            logger.error("❌ Échec de la collecte")
            return "Error", 500
            
    except Exception as e:
        logger.error(f"❌ Erreur: {e}")
        return f"Error: {e}", 500

def main(request):
    """Point d'entrée pour la Cloud Function"""
    return collect_velib_data()

if __name__ == "__main__":
    collect_velib_data()