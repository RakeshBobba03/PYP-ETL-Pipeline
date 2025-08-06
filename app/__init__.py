# app/__init__.py

import os
from datetime import datetime

from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from dotenv import load_dotenv

load_dotenv()

db = SQLAlchemy()

def create_app():
    app = Flask(__name__)

    # ─── Secret key for flash/session ─────────────────────
    # In production, make this a strong random value!
    app.secret_key = os.getenv(
        'FLASK_SECRET_KEY',
        'change-me-to-a-secure-random-value'
    )
    # ────────────────────────────────────────────────────────

    # ─── Config ────────────────────────────────────────────────
    db_user = os.getenv('DB_USER', 'flask_user')
    db_pass = os.getenv('DB_PASSWORD', 'flask_password')
    db_host = os.getenv('DB_HOST', 'db')
    db_port = os.getenv('DB_PORT', '5432')
    db_name = os.getenv('DB_NAME', 'flask_db')

    app.config['SQLALCHEMY_DATABASE_URI'] = (
        os.getenv('DATABASE_URL') or
        f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
    )
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

    app.config['DGRAPH_URL']       = os.getenv('DGRAPH_URL')
    app.config['DGRAPH_API_TOKEN'] = os.getenv('DGRAPH_API_TOKEN')
    # ────────────────────────────────────────────────────────────

    db.init_app(app)

    from app.routes import main_bp
    app.register_blueprint(main_bp)

    @app.context_processor
    def inject_now():
        return { 'now': datetime.utcnow }

    return app
