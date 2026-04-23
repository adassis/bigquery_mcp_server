import os
from .bq_client import get_bq_client

PROJECT_ID = os.environ.get("BIGQUERY_PROJECT_ID", "yago-data-pipeline-prod")


def list_datasets() -> str:
    """
    Liste tous les datasets disponibles dans le projet BigQuery yago-data-pipeline-prod.

    Un dataset est un conteneur de tables (équivalent d'un schéma en SQL classique).
    Utilise cet outil EN PREMIER pour découvrir la structure du projet
    avant toute autre action.

    Retourne : La liste des datasets avec leur ID.
    """
    try:
        client = get_bq_client()

        # Appel API BigQuery : GET /v2/projects/{projectId}/datasets
        datasets = list(client.list_datasets(project=PROJECT_ID))

        if not datasets:
            return f"Aucun dataset trouvé dans le projet {PROJECT_ID}."

        result_lines = [
            f"📦 Datasets disponibles dans '{PROJECT_ID}' ({len(datasets)} trouvés) :\n"
        ]
        for ds in datasets:
            result_lines.append(f"  • {ds.dataset_id}")

        result_lines.append(
            "\n➡️ Utilise list_tables(dataset_id) pour explorer un dataset."
        )

        return "\n".join(result_lines)

    except Exception as e:
        return f"❌ Erreur lors de la récupération des datasets : {str(e)}"