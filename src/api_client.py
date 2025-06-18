import requests
from .config import STATION_INFORMATION_URL, STATION_STATUS_URL

payload = {}
headers = {}

def fetch_station_information():
    """Effectue une requête GET sur l'URL de l'information des stations Velib et retourne le résultat au format JSON"""
    response_information = requests.request("GET", STATION_INFORMATION_URL, headers=headers, data=payload)
    return response_information.json()['data']['stations']

def fetch_station_status():
    """Effectue une requête GET sur l'URL du status des stations Velib et retourne le résultat au format JSON"""
    response_status = requests.request("GET", STATION_STATUS_URL, headers=headers, data=payload)
    return response_status.json()['data']['stations']