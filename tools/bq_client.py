# a
import os
import json
from google.cloud import bigquery
from google.oauth2 import service_account

# Lecture des variables d'environnement
PROJECT_ID = os.environ.get("BIGQUERY_PROJECT_ID", "yago-data-pipeline-prod")
GOOGLE_CREDENTIALS_JSON = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON", "")


def get_bq_client() -> bigquery.Client:
    """
    Crée et retourne un client BigQuery authentifié.
    
    En production (Railway) : utilise le JSON du Service Account
    stocké dans la variable d'environnement GOOGLE_APPLICATION_CREDENTIALS_JSON.
    
    En local : utilise les credentials par défaut du système
    (Application Default Credentials via `gcloud auth login`).
    """
    if GOOGLE_CREDENTIALS_JSON:
        # Parse le contenu JSON de la variable d'environnement
        credentials_info = json.loads(GOOGLE_CREDENTIALS_JSON)

        # Crée les credentials avec uniquement le scope lecture BigQuery
        credentials = service_account.Credentials.from_service_account_info(
            credentials_info,
            scopes=["https://www.googleapis.com/auth/bigquery.readonly"]
        )

        return bigquery.Client(project=PROJECT_ID, credentials=credentials)
    else:
        # Fallback pour le développement local
        return bigquery.Client(project=PROJECT_ID)