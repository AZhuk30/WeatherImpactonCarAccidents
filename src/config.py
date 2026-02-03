"""
Configuration management for the ETL pipeline
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Project paths
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
LOGS_DIR = DATA_DIR / "logs"

# Create directories
for directory in [RAW_DATA_DIR, PROCESSED_DATA_DIR, LOGS_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

# Database configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 3306)),
    'database': os.getenv('DB_NAME', 'nyc_traffic_safety'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
}

# API configuration
WEATHER_API_URL = os.getenv('WEATHER_API_URL', 'https://api.open-meteo.com/v1/forecast')
NYC_COLLISIONS_API = os.getenv('NYC_COLLISIONS_API', 'https://data.cityofnewyork.us/resource/h9gi-nx95.csv')

# NYC Borough coordinates (center points)
BOROUGHS = {
    'MANHATTAN': {'lat': 40.7834, 'lon': -73.9663},
    'BROOKLYN': {'lat': 40.6501, 'lon': -73.9496},
    'QUEENS': {'lat': 40.6815, 'lon': -73.8365},
    'BRONX': {'lat': 40.8499, 'lon': -73.8664},
    'STATEN ISLAND': {'lat': 40.5623, 'lon': -74.1399},
}

# Weather parameters to extract
WEATHER_PARAMS = [
    "temperature_2m",
    "precipitation",
    "visibility",
    "rain",
    "showers",
    "snowfall",
    "wind_speed_10m",
]

# Alert thresholds
ALERT_CONFIG = {
    'collision_threshold': int(os.getenv('ALERT_THRESHOLD_COLLISIONS', 50)),
    'visibility_threshold': int(os.getenv('ALERT_THRESHOLD_VISIBILITY', 1000)),
    'email': os.getenv('ALERT_EMAIL'),
}

# Pipeline configuration
PIPELINE_CONFIG = {
    'lookback_days': 7,  # How many days back to extract
    'batch_size': 1000,
    'timezone': 'America/New_York',
    'default_start_date': '2024-01-01',  # Start of your analysis
}