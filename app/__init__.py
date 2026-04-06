from flask import Flask


def create_app() -> Flask:
    app = Flask(__name__)
    app.config.from_object("config.Config")

    @app.get("/")
    def health_check():
        return {"message": "Flask project initialized successfully."}

    return app
