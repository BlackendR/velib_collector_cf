import time
from .api_client import fetch_station_status, fetch_station_information
from datetime import datetime

def transform_station_status(stations_data=None):
    """Transforme les données de station_status en un format compatible avec BigQuery.
    
    Parameters
    ----------
    stations_data : `list`, `optional`
        Liste des données des stations. Si `None`, les données seront récupérées via l'API.
        
    Returns
    -------
    Liste des stations transformées avec les champs suivants :
    - station_id: Identifiant unique de la station
    - stationCode: Code de la station
    - num_bikes_available: Nombre de vélos disponibles
    - num_docks_available: Nombre de places disponibles
    - bikes_types_available: Dictionnaire des types de vélos disponibles
    - is_installed: État d'installation
    - is_renting: État de location
    - is_returning: État de retour
    - last_reported: Dernière mise à jour
    - last_collected: Date de collecte
    """
    if stations_data is None:
        stations_data = fetch_station_status()
        
    status = []
    for station in stations_data:
        bikes_types = station['num_bikes_available_types']
        status.append({
            "station_id": station['station_id'],
            "stationCode": str(station['stationCode']),
            "num_bikes_available": station['num_bikes_available'],
            "num_docks_available": station['num_docks_available'],
            "bikes_types_available": {
                "mechanical": bikes_types[0].get('mechanical', 0),
                "ebike": bikes_types[0].get('ebike', 0)
            },
            "is_installed": station['is_installed'],
            "is_renting": station['is_renting'],
            "is_returning": station['is_returning'],
            "last_reported": datetime.fromtimestamp(station['last_reported']).strftime('%Y-%m-%d %H:%M:%S'),
            "last_collected": datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        })
    return status

def transform_station_information(stations_data=None):
    """Transforme les données de station_information en un format compatible avec BigQuery.
    
    Parameters
    ----------
    stations_data : `list`, `optional`
        Liste des données des stations. Si `None`, les données seront récupérées via l'API.
        
    Returns
    -------
    Liste des stations transformées avec les champs suivants :
    - station_id: Identifiant unique de la station
    - stationCode: Code de la station
    - name: Nom de la station
    - latitude: Latitude de la station
    - longitude: Longitude de la station
    - capacity: Capacité totale de la station
    """
    if stations_data is None:
        stations_data = fetch_station_information()
        
    information = []
    for station in stations_data:
        information.append({
            "station_id": station['station_id'],
            "stationCode": str(station['stationCode']),
            "name": str(station['name']),
            "latitude": station['lat'],
            "longitude": station['lon'],
            "capacity": station['capacity'],
        })
    return information
