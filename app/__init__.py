from flask import Flask

from db import get_db_client
from ingestion.repository import CircularRepository


def create_app() -> Flask:
    app = Flask(__name__)
    app.config.from_object("config.Config")

    @app.get("/")
    def health_check():
        return {"message": "Flask project initialized successfully."}

    @app.get("/api/circulars/counts")
    def get_circular_counts():
        db_client = get_db_client()
        repository = CircularRepository(db_pool=db_client.get_pool())
        counts = repository.get_source_counts(("NSE", "SEBI"))
        nse_count = counts.get("NSE", 0)
        sebi_count = counts.get("SEBI", 0)
        return {
            "nse": nse_count,
            "sebi": sebi_count,
            "total": nse_count + sebi_count,
        }

    return app
