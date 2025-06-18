from src.main import main

def collect_velib_data(request):
    """Point d'entrée pour la Google Cloud Function"""
    return main(request)