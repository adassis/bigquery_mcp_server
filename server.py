import os
from fastmcp import FastMCP
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse
import uvicorn

# Import des 4 outils depuis le dossier tools/
from tools import list_datasets, list_tables, get_table_schema, run_query

# ── Variables d'environnement ─────────────────────────────────────────────────
PROJECT_ID   = os.environ.get("BIGQUERY_PROJECT_ID", "yago-data-pipeline-prod")
BEARER_TOKEN = os.environ.get("MCP_BEARER_TOKEN", "")


# ── Middleware d'authentification ─────────────────────────────────────────────
class BearerTokenMiddleware(BaseHTTPMiddleware):
    """
    Vérifie que chaque requête entrante contient le bon Bearer Token.
    Dust enverra automatiquement ce token dans le header Authorization.
    """
    async def dispatch(self, request, call_next):
        if request.url.path.startswith("/sse") or request.url.path.startswith("/message"):
            auth_header = request.headers.get("Authorization", "")
            if BEARER_TOKEN and auth_header != f"Bearer {BEARER_TOKEN}":
                return JSONResponse(
                    {"error": "Unauthorized — Invalid or missing Bearer token"},
                    status_code=401
                )
        return await call_next(request)


# ── Création du serveur MCP ───────────────────────────────────────────────────
mcp = FastMCP(
    name="BigQuery MCP — yago-data-pipeline-prod",
    instructions="""
    Ce serveur MCP permet d'interroger le projet BigQuery yago-data-pipeline-prod.
    Workflow recommandé :
    1. list_datasets       → découvrir les datasets disponibles
    2. list_tables         → lister les tables d'un dataset
    3. get_table_schema    → lire le schéma et la doc d'une table
    4. run_query           → exécuter une requête SQL et obtenir les résultats
    Utiliser uniquement Standard SQL. Ne jamais écrire de requêtes INSERT/UPDATE/DELETE.
    """
)

# ── Enregistrement des 4 outils ───────────────────────────────────────────────
mcp.tool()(list_datasets)
mcp.tool()(list_tables)
mcp.tool()(get_table_schema)
mcp.tool()(run_query)


# ── Démarrage ─────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))

    print(f"🚀 Serveur MCP BigQuery démarré sur le port {port}")
    print(f"📦 Projet : {PROJECT_ID}")
    print(f"🔐 Bearer Token : {'✅ Activé' if BEARER_TOKEN else '⚠️  Désactivé'}")

    app = mcp.streamable_http_app()
    app.add_middleware(BearerTokenMiddleware)

    uvicorn.run(app, host="0.0.0.0", port=port)