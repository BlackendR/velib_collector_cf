from google.cloud import bigquery
from .config import PROJECT_ID, DATASET_ID, STATION_INFORMATION_TABLE, STATION_STATUS_TABLE, client
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def upsert_stations_information(information_data):
    """
    Met à jour ou insère les données d'informations des stations
    """
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{STATION_INFORMATION_TABLE}"
    current_time = datetime.now()

    try:
        temp_table_id = f"{table_id}_temp_{int(current_time.timestamp())}"
        
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        
        job = client.load_table_from_json(information_data, temp_table_id, job_config=job_config)
        job.result()

        merge_query = f"""
        MERGE `{table_id}` AS target
        USING `{temp_table_id}` AS source
        ON CAST(target.station_id AS INT64) = CAST(source.station_id AS INT64)
        WHEN MATCHED THEN
          UPDATE SET
            stationCode = CAST(source.stationCode AS STRING),
            name = CAST(source.name AS STRING),
            geopoint = ST_GEOGPOINT(CAST(source.longitude AS FLOAT64), CAST(source.latitude AS FLOAT64)),
            capacity = CAST(source.capacity AS INT64)
        WHEN NOT MATCHED THEN
          INSERT (station_id, stationCode, name, geopoint, capacity)
          VALUES (
            CAST(source.station_id AS INT64),
            CAST(source.stationCode AS STRING),
            CAST(source.name AS STRING),
            ST_GEOGPOINT(CAST(source.longitude AS FLOAT64), CAST(source.latitude AS FLOAT64)),
            CAST(source.capacity AS INT64)
          )
        """

        query_job = client.query(merge_query)
        query_job.result()

        client.delete_table(temp_table_id, not_found_ok=True)
        logger.info(f"UPSERT stations_info: {len(information_data)} rows inserted")
        return True
    
    except Exception as e:
        logger.error(f"Erreur UPSERT stations_info: {e}")
        try:
            client.delete_table(temp_table_id, not_found_ok=True)
        except:
            pass
        return False

def upsert_stations_status(status_data):
    """
    Met à jour ou insère les données des status des stations
    """
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{STATION_STATUS_TABLE}"
    current_time = datetime.now()

    try:
        temp_table_id = f"{table_id}_temp_{int(current_time.timestamp())}"
        
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        
        job = client.load_table_from_json(status_data, temp_table_id, job_config=job_config)
        job.result()

        merge_query = f"""
        MERGE `{table_id}` AS target
        USING `{temp_table_id}` AS source
        ON CAST(target.station_id AS INT64) = CAST(source.station_id AS INT64)
        WHEN MATCHED THEN
          UPDATE SET
            stationCode = CAST(source.stationCode AS STRING),
            num_bikes_available = CAST(source.num_bikes_available AS INT64),
            num_docks_available = CAST(source.num_docks_available AS INT64),
            bikes_types_available = source.bikes_types_available,
            is_installed = CAST(source.is_installed AS BOOLEAN),
            is_returning = CAST(source.is_returning AS BOOLEAN),
            is_renting = CAST(source.is_renting AS BOOLEAN),
            last_reported = CAST(source.last_reported AS TIMESTAMP),
            last_collected = CAST(source.last_collected AS TIMESTAMP)
        WHEN NOT MATCHED THEN
          INSERT (station_id, stationCode, num_bikes_available, num_docks_available, bikes_types_available, is_installed, is_returning, is_renting, last_reported, last_collected)
          VALUES (
            CAST(source.station_id AS INT64),
            CAST(source.stationCode AS STRING),
            CAST(source.num_bikes_available AS INT64),
            CAST(source.num_docks_available AS INT64),
            source.bikes_types_available,
            CAST(source.is_installed AS BOOLEAN),
            CAST(source.is_returning AS BOOLEAN),
            CAST(source.is_renting AS BOOLEAN),
            CAST(source.last_reported AS TIMESTAMP),
            CAST(source.last_collected AS TIMESTAMP)
          )
        """

        query_job = client.query(merge_query)
        query_job.result()

        client.delete_table(temp_table_id, not_found_ok=True)
        logger.info(f"UPSERT stations_status: {len(status_data)} rows inserted")
        return True
    
    except Exception as e:
        logger.error(f"Erreur UPSERT stations_status: {e}")
        try:
            client.delete_table(temp_table_id, not_found_ok=True)
        except:
            pass
        return False

def test_bigquery_connection():
    """
    Teste la connexion à BigQuery et l'existence des tables
    
    Returns:
        bool: True si la connexion fonctionne
    """
    try:
        datasets = list(client.list_datasets())
        logger.info(f'Datasets disponibles: {[d.dataset_id for d in datasets]}')

        dataset_ref = client.dataset(DATASET_ID)
        tables = list(client.list_tables(dataset_ref))
        table_names = [table.table_id for table in tables]

        required_tables = [STATION_INFORMATION_TABLE, STATION_STATUS_TABLE]
        missing_tables = [table for table in required_tables if table not in table_names]

        if missing_tables:
            logger.warning(f'Tables manquantes: {missing_tables}')
            return False
        
        for table_name in required_tables:
            table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
            table = client.get_table(table_id)
            logger.info(f"Schéma de {table_name}: {[field.name for field in table.schema]}")
        
        logger.info("Connexion BigQuery OK - Toutes les tables requises existent")
        return True
    
    except Exception as e:
        logger.error(f'Erreur de connexion à BigQuery: {e}')
        return False