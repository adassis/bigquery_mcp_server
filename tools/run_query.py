import os
from .bq_client import get_bq_client
from google.cloud import bigquery

PROJECT_ID = os.environ.get("BIGQUERY_PROJECT_ID", "yago-data-pipeline-prod")

# Mots-clés SQL interdits : ce serveur est en LECTURE SEULE
FORBIDDEN_KEYWORDS = [
    "INSERT", "UPDATE", "DELETE", "DROP",
    "CREATE", "ALTER", "TRUNCATE", "MERGE"
]


def run_query(sql_query: str, location: str = "EU") -> str:
    """
    Exécute une requête SQL sur BigQuery et retourne les résultats.

    IMPORTANT :
    - Utilise get_table_schema() AVANT pour connaître les colonnes disponibles.
    - Uniquement des requêtes SELECT (lecture seule).
    - Toujours préfixer les tables : `yago-data-pipeline-prod.dataset.table`
    - Standard SQL BigQuery uniquement (pas Legacy SQL).

    Args:
        sql_query : La requête SQL à exécuter en Standard SQL BigQuery.
        location  : La région géographique des données (défaut: "EU").

    Retourne : Les résultats formatés en tableau lisible.
    """
    try:
        # ── Sécurité : bloquer les requêtes d'écriture ───────────────────────
        sql_upper = sql_query.upper().strip()
        for keyword in FORBIDDEN_KEYWORDS:
            if sql_upper.startswith(keyword) or f" {keyword} " in sql_upper:
                return (
                    f"❌ Requête refusée : le mot-clé '{keyword}' n'est pas "
                    f"autorisé. Ce serveur est en lecture seule (SELECT uniquement)."
                )

        client = get_bq_client()

        # ── Configuration du job BigQuery ────────────────────────────────────
        job_config = bigquery.QueryJobConfig(
            use_query_cache=True,    # Réutilise le cache si la requête a déjà été faite
            use_legacy_sql=False,    # Standard SQL obligatoire
            maximum_bytes_billed=10 * 1024 * 1024 * 1024  # Plafond 10 GB (protection coût)
        )

        # ── Exécution de la requête (synchrone) ──────────────────────────────
        # Appel API BigQuery : POST /v2/projects/{projectId}/queries
        query_job = client.query(sql_query, job_config=job_config, location=location)

        # Bloque jusqu'à la fin de l'exécution (timeout 2 min)
        rows = query_job.result(timeout=120)

        # ── Lecture des résultats ─────────────────────────────────────────────
        column_names = [field.name for field in rows.schema]
        all_rows = []

        for row in rows:
            all_rows.append(
                [str(v) if v is not None else "NULL" for v in row.values()]
            )

        # Cas : aucun résultat
        if not all_rows:
            return "✅ Requête exécutée avec succès. Aucun résultat retourné."

        # ── Formatage en tableau aligné ───────────────────────────────────────
        col_widths = [len(name) for name in column_names]
        for row in all_rows:
            for i, val in enumerate(row):
                col_widths[i] = max(col_widths[i], len(val))

        header    = " | ".join(name.ljust(col_widths[i]) for i, name in enumerate(column_names))
        separator = "-+-".join("-" * w for w in col_widths)

        result_lines = [
            f"✅ Résultats ({len(all_rows)} ligne{'s' if len(all_rows) > 1 else ''}) :\n",
            header,
            separator,
        ]
        for row in all_rows:
            result_lines.append(
                " | ".join(str(val).ljust(col_widths[i]) for i, val in enumerate(row))
            )

        # Octets traités (indicateur de coût)
        if query_job.total_bytes_processed:
            gb = query_job.total_bytes_processed / (1024 ** 3)
            result_lines.append(f"\n💡 Données traitées : {gb:.3f} GB")

        return "\n".join(result_lines)

    except Exception as e:
        return f"❌ Erreur lors de l'exécution de la requête : {str(e)}"