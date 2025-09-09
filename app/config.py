import os
import logging
from datetime import timedelta

class Config:
    """Base configuration class"""
    # Flask settings
    SECRET_KEY = os.environ.get('FLASK_SECRET_KEY')
    if not SECRET_KEY:
        if os.environ.get('FLASK_DEBUG') == 'False':  # Production mode
            raise ValueError("FLASK_SECRET_KEY environment variable must be set in production")
        import secrets
        SECRET_KEY = secrets.token_hex(32)
    
    # Database settings
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL') or \
        f"postgresql://{os.environ.get('DB_USER', 'flask_user')}:{os.environ.get('DB_PASSWORD', 'flask_password')}@{os.environ.get('DB_HOST', 'db')}:{os.environ.get('DB_PORT', '5432')}/{os.environ.get('DB_NAME', 'flask_db')}"
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    
    # Dgraph settings
    DGRAPH_URL = os.environ.get('DGRAPH_URL')
    DGRAPH_API_TOKEN = os.environ.get('DGRAPH_API_TOKEN')
    
    # ETL settings
    FUZZY_MATCH_THRESHOLD = float(os.environ.get('FUZZY_MATCH_THRESHOLD', '80.0'))
    AUTO_RESOLVE_THRESHOLD = float(os.environ.get('AUTO_RESOLVE_THRESHOLD', '95.0'))
    BATCH_SIZE = int(os.environ.get('BATCH_SIZE', '1000'))
    
    # Security settings
    SESSION_COOKIE_SECURE = os.environ.get('FLASK_DEBUG') == 'False'  # Production mode
    SESSION_COOKIE_HTTPONLY = True
    SESSION_COOKIE_SAMESITE = 'Lax'
    PERMANENT_SESSION_LIFETIME = timedelta(hours=24)
    
    # File upload settings
    MAX_CONTENT_LENGTH = 16 * 1024 * 1024  # 16MB max file size
    UPLOAD_FOLDER = '/app/uploads'  # Use absolute path in Docker container
    ALLOWED_EXTENSIONS = {'xlsx', 'xls', 'csv'}
    
    # Request timeout settings
    DGRAPH_TIMEOUT = int(os.environ.get('DGRAPH_TIMEOUT', '30'))
    DGRAPH_MAX_RETRIES = int(os.environ.get('DGRAPH_MAX_RETRIES', '3'))
    DGRAPH_RETRY_DELAY = int(os.environ.get('DGRAPH_RETRY_DELAY', '1'))
    
    # Logging settings
    LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO').upper()

class DevelopmentConfig(Config):
    """Development configuration"""
    DEBUG = True
    FLASK_DEBUG = True

class ProductionConfig(Config):
    """Production configuration"""
    DEBUG = False
    FLASK_DEBUG = False
    
    # Enforce HTTPS in production
    SESSION_COOKIE_SECURE = True
    
    # Stricter security in production
    SESSION_COOKIE_SAMESITE = 'Strict'

class TestingConfig(Config):
    """Testing configuration"""
    TESTING = True
    SQLALCHEMY_DATABASE_URI = 'sqlite:///:memory:'
    WTF_CSRF_ENABLED = False

# Configuration mapping
config = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'testing': TestingConfig,
    'default': DevelopmentConfig
}