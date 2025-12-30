"""
Configuración del proyecto
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Cargar .env
load_dotenv()

# Directorios
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / 'data'
RAW_DIR = DATA_DIR / 'raw'
PROCESSED_DIR = DATA_DIR / 'processed'
ANALYTICS_DIR = DATA_DIR / 'analytics'

# Crear directorios si no existen
for dir_path in [RAW_DIR, PROCESSED_DIR, ANALYTICS_DIR]:
    dir_path.mkdir(parents=True, exist_ok=True)

# PostgreSQL
DB_HOST = os.getenv('DB_HOST', '172.20.0.3')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'iadb_data')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'postgres')

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# API del IADB
IADB_API_URL = "https://data.iadb.org/api/3/action"
IADB_PACKAGE_ID = "social-indicators-of-latin-america-and-the-caribbean"

# Países a procesar
COUNTRIES = ['ARG', 'BOL', 'BRA', 'CHL', 'COL', 'CRI', 'ECU', 'MEX', 'PER', 'URY']

# Mapeo de países
COUNTRIES_MAP = {
    'ARG': 'Argentina',
    'BOL': 'Bolivia',
    'BRA': 'Brasil',
    'CHL': 'Chile',
    'COL': 'Colombia',
    'CRI': 'Costa Rica',
    'ECU': 'Ecuador',
    'MEX': 'México',
    'PER': 'Perú',
    'URY': 'Uruguay'
}

ALLOWED_NAMES = [
    "Percentage of the population in poverty",
    "Early school dropout rate",
    "Gross attendance rate primary education",
    "Gross attendance rate secondary education",
    "Gross attendance rate tertiary education",
    "Mean score in science (PISA)",
    "Mean score in reading (PISA)",
    "Mean score in mathematics (PISA)",
    "Percentage with access to schools with internet"
]

YEARS = [
    "2019",
    "2020",
    "2021",
    "2022"
]

FIELDS = ['year', 'isoalpha3', 'area', 'value', 'sex']

print("✓ Configuración cargada")
print(f"  Base de datos: {DB_NAME} @ {DB_HOST}")
print(f"  Directorios creados en: {DATA_DIR}")