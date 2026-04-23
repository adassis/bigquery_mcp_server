import os
from .bq_client import get_bq_client

PROJECT_ID = os.environ.get("BIGQUERY_PROJECT_ID", "yago-data-pipeline-prod")


def list_tables(dataset_id: str) -> str:
    """
    Liste toutes les tables contenues dans un dataset BigQuery spécifique.

    Utilise cet outil après list_datasets pour explorer le contenu d'un dataset.

    Args:
        dataset_id: L'identifiant du dataset à explorer (ex: "analytics", "sales")

    Retourne : La liste des tables avec leur nom et leur type (TABLE, VIEW, etc.)
    """
    try:
        client = get_bq_client()

        # Référence complète : projet.dataset
        dataset_ref = f"{PROJECT_ID}.{dataset_id}"

        # Appel API BigQuery : GET /v2/projects/{projectId}/datasets/{datasetId}/tables
        tables = list(client.list_tables(dataset_ref))

        if not tables:
            return f"Aucune table trouvée dans le dataset '{dataset_id}'."

        result_lines = [
            f"📋 Tables dans '{dataset_ref}' ({len(tables)} trouvées) :\n"
        ]

        # Icône selon le type de table
        type_emoji = {
            "TABLE": "🗄️",
            "VIEW": "👁️",
            "EXTERNAL": "🔗",
            "MATERIALIZED_VIEW": "⚡",
        }

        for table in tables:
            emoji = type_emoji.get(table.table_type, "📄")
            result_lines.append(
                f"  {emoji} {table.table_id}  [{table.table_type}]"
            )

        result_lines.append(
            "\n➡️ Utilise get_table_schema(dataset_id, table_id) "
            "pour voir le schéma d'une table."
        )

        return "\n".join(result_lines)

    except Exception as e:
        return (
            f"❌ Erreur lors de la récupération des tables "
            f"du dataset '{dataset_id}' : {str(e)}"
        )