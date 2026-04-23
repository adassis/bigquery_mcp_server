import os
from .bq_client import get_bq_client

PROJECT_ID = os.environ.get("BIGQUERY_PROJECT_ID", "yago-data-pipeline-prod")


def get_table_schema(dataset_id: str, table_id: str) -> str:
    """
    Retourne le schéma complet d'une table BigQuery :
    colonnes, types de données, descriptions, et statistiques.

    C'est l'outil le plus important pour comprendre la structure d'une table
    avant d'écrire une requête SQL. Il expose également les descriptions
    de la table et de chaque colonne si elles sont documentées dans BigQuery.

    Args:
        dataset_id: L'identifiant du dataset (ex: "analytics")
        table_id:   L'identifiant de la table   (ex: "customers")

    Retourne : Le schéma complet avec colonnes, types et descriptions.
    """
    try:
        client = get_bq_client()

        # Référence complète : projet.dataset.table
        table_ref = f"{PROJECT_ID}.{dataset_id}.{table_id}"

        # Appel API BigQuery :
        # GET /v2/projects/{projectId}/datasets/{datasetId}/tables/{tableId}
        table = client.get_table(table_ref)

        result_lines = [
            f"🗄️  Table      : `{table_ref}`",
            f"📝 Description : {table.description or '(aucune description renseignée)'}",
        ]

        # Statistiques de stockage (disponibles si la table est non vide)
        if table.num_rows is not None:
            result_lines.append(f"📊 Lignes      : {table.num_rows:,}")
        if table.num_bytes is not None:
            size_mb = table.num_bytes / (1024 * 1024)
            result_lines.append(f"💾 Taille      : {size_mb:.1f} MB")

        result_lines += ["", f"📐 Colonnes ({len(table.schema)}) :", ""]

        # Indicateur de mode pour chaque colonne
        mode_indicator = {
            "REQUIRED": "✱",   # champ obligatoire, jamais NULL
            "REPEATED": "[]",  # champ tableau/liste
            "NULLABLE": "○",   # champ optionnel (valeur par défaut)
        }

        for field in table.schema:
            indicator = mode_indicator.get(field.mode, "○")
            desc = (
                f"\n      └─ {field.description}"
                if field.description
                else ""
            )
            result_lines.append(
                f"  {indicator} {field.name:<30} {field.field_type}{desc}"
            )

        result_lines += [
            "",
            f"➡️  Référence complète pour le SQL : "
            f"`{PROJECT_ID}.{dataset_id}.{table_id}`",
        ]

        return "\n".join(result_lines)

    except Exception as e:
        return (
            f"❌ Erreur lors de la récupération du schéma "
            f"de '{dataset_id}.{table_id}' : {str(e)}"
        )