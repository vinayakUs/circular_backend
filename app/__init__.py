"""
Flask app factory.

The actual route handlers live in app/routes/* — this module wires them up.
Health check stays here because it's a single line that doesn't belong to
any one domain.
"""
from __future__ import annotations

import logging

from flask import Flask
from flask_cors import CORS

from config import Config
from services.rag.answer_generator import RAGAnswerGenerator

logger = logging.getLogger(__name__)


def create_app() -> Flask:
    logging.basicConfig(
        level=Config.LOG_LEVEL,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger("numba.core").setLevel(logging.WARNING)

    app = Flask(__name__)
    app.config.from_object("config.Config")
    CORS(app, origins="*")

    rag_generator = RAGAnswerGenerator()

    @app.get("/")
    def health_check():
        return {"message": "Flask project initialized successfully."}

    # Wire up domain route modules.
    from app.routes import auth, circulars, experts, mentions, properties, admin

    auth.register_routes(app)
    circulars.register_routes(app, rag_generator=rag_generator)
    experts.register_routes(app)
    mentions.register_routes(app)
    properties.register_routes(app)
    admin.register_routes(app)

    return app
