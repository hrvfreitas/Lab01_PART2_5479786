# =============================================================================
# superset_config.py — Apache Superset
# Cache simples em memória
# =============================================================================
import os

SECRET_KEY = os.environ.get(
    'SUPERSET_SECRET_KEY',
    'pncp_lab_secret_5479786_change_me'
)

SQLALCHEMY_DATABASE_URI = os.environ.get(
    'SQLALCHEMY_DATABASE_URI',
    'postgresql://postgres:postgres@pncp_postgres:5432/superset_meta'
)

# Cache em memória
CACHE_CONFIG = {
    'CACHE_TYPE':            'SimpleCache',
    'CACHE_DEFAULT_TIMEOUT': 300,
}
DATA_CACHE_CONFIG = {
    'CACHE_TYPE':            'SimpleCache',
    'CACHE_DEFAULT_TIMEOUT': 600,
}

FEATURE_FLAGS = {
    'ENABLE_TEMPLATE_PROCESSING': True,
    'DASHBOARD_NATIVE_FILTERS':   True,
    'DASHBOARD_CROSS_FILTERS':    True,
    'DRILL_TO_DETAIL':            True,
    'EMBEDDABLE_CHARTS':          True,
}

PREVENT_UNSAFE_DB_CONNECTIONS = False
SQLLAB_TIMEOUT               = 300
SUPERSET_WEBSERVER_TIMEOUT   = 300
SUPERSET_LOAD_EXAMPLES       = False
BABEL_DEFAULT_LOCALE         = 'pt'
BABEL_DEFAULT_TIMEZONE       = 'America/Sao_Paulo'
APP_NAME                     = 'PNCP Analytics'
LOG_LEVEL                    = 'INFO'
