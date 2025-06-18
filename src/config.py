import os
import logging
from dotenv import load_dotenv
from google.cloud import bigquery
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Charger les variables d'environnement depuis le fichier .env à la racine
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

# Google Cloud
PROJECT_ID = os.getenv('GOOGLE_CLOUD_PROJECT', 'test-project')
DATASET_ID = os.getenv('BIGQUERY_DATASET', 'test_dataset')
LOCATION = os.getenv('BIGQUERY_LOCATION', 'europe-west1')
CREDENTIALS = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

logger.info(f"PROJECT_ID: {PROJECT_ID}")
logger.info(f"DATASET_ID: {DATASET_ID}")
logger.info(f"CREDENTIALS: {CREDENTIALS}")

# API URLs
STATION_INFORMATION_URL = os.getenv('VELIB_STATION_INFORMATION_URL', 'https://api-test.com/stations/information')
STATION_STATUS_URL = os.getenv('VELIB_STATION_STATUS_URL', 'https://api-test.com/stations/status')

# BigQuery Tables
STATION_INFORMATION_TABLE = os.getenv('STATIONS_INFO_TABLE_NAME', 'stations_info')
STATION_STATUS_TABLE = os.getenv('STATIONS_STATUS_TABLE_NAME', 'stations_status')

# API Config
REQUEST_TIMEOUT = 10
MAX_RETRIES = 3

# Client BigQuery - initialisé seulement si nécessaire
_client = None

def get_bigquery_client():
    """Retourne le client BigQuery, en le créant si nécessaire"""
    global _client
    if _client is None:
        _client = bigquery.Client(project=PROJECT_ID)
    return _client

def set_bigquery_client(client):
    """Permet d'injecter un client BigQuery (utile pour les tests)"""
    global _client
    _client = client
