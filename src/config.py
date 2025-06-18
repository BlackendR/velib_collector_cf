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
PROJECT_ID = os.getenv('GOOGLE_CLOUD_PROJECT')
DATASET_ID = os.getenv('BIGQUERY_DATASET')
LOCATION = os.getenv('BIGQUERY_LOCATION')
CREDENTIALS = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

client = bigquery.Client(project=PROJECT_ID)

# API URLs
STATION_INFORMATION_URL = os.getenv('VELIB_STATION_INFORMATION_URL')
STATION_STATUS_URL = os.getenv('VELIB_STATION_STATUS_URL')

# BigQuery Tables
STATION_INFORMATION_TABLE = os.getenv('STATIONS_INFO_TABLE_NAME')
STATION_STATUS_TABLE = os.getenv('STATIONS_STATUS_TABLE_NAME')

# API Config
REQUEST_TIMEOUT = 10
MAX_RETRIES = 3
