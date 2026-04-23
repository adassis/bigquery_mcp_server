from dotenv import load_dotenv
load_dotenv()

import os
import uvicorn
from mcp.server.fastmcp import FastMCP
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

from tools import list_datasets, list_tables, get_table_schema, run_query

PROJECT_ID   = os.environ.get("BIGQUERY_PROJECT_ID", "yago-data-pipeline-prod")
BEARER_TOKEN = os.environ.get("MCP_BEARER_TOKEN", "")
PORT         = int(os.environ.get("PORT", 8000))

mcp = FastMCP(
    name="bigquery-server",
    host="0.0.0.0",
    port=PORT,
    instructions=(
        "Serveur MCP BigQuery pour le projet yago-data-pipeline-prod. "
        "Workflow : list_datasets -> list_tables -> get_table_schema -> run_query. "
        "Utiliser uniquement Standard SQL. Ne jamais ecrire INSERT/UPDATE/DELETE."
    )
)

mcp.tool()(list_datasets)
mcp.tool()(list_tables)
mcp.tool()(get_table_schema)
mcp.tool()(run_query)

class BearerAuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        if BEARER_TOKEN:
            auth = request.headers.get("Authorization", "")
            if not auth.startswith("Bearer ") or auth[7:].strip() != BEARER_TOKEN:
                return JSONResponse({"error": "Non autorise"}, status_code=401)
        return await call_next(request)

if __name__ == "__main__":
    print(f"Serveur MCP BigQuery demarre sur le port {PORT}")
    print(f"Projet : {PROJECT_ID}")
    print(f"Auth : {'Activee' if BEARER_TOKEN else 'DESACTIVEE'}")

    app = mcp.streamable_http_app()
    app.add_middleware(BearerAuthMiddleware)
    uvicorn.run(app, host="0.0.0.0", port=PORT)