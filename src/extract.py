"""
Data extraction from APIs
Weather (Open-Meteo) + NYC Motor Vehicle Collisions
STANDALONE VERSION - No imports from src.config
"""

import logging
import os
import sys
from datetime import datetime, timedelta
from io import StringIO  # <-- Add this import

import pandas as pd
import requests
import requests_cache
from retry_requests import retry
import openmeteo_requests
import pytz

logger = logging.getLogger(__name__)

# =========================
# CONFIGURATION - Hardcoded values
# =========================

BOROUGHS = ["MANHATTAN", "BROOKLYN", "QUEENS", "BRONX", "STATEN ISLAND"]
WEATHER_API_URL = "https://api.open-meteo.com/v1/forecast"
NYC_COLLISIONS_API = "https://data.cityofnewyork.us/resource/h9gi-nx95.csv"
RAW_DATA_DIR = "data/raw"

# =========================
# WEATHER EXTRACTOR
# =========================

class WeatherExtractor:
    """Extract hourly weather data for NYC boroughs using Open-Meteo"""
    
    def __init__(self):
        # EXACTLY like your working code
        cache_session = requests_cache.CachedSession(".cache", expire_after=3600)
        retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
        self.client = openmeteo_requests.Client(session=retry_session)
        
        # Coordinates EXACTLY like your working code
        self.latitudes = [40.7834, 40.6501, 40.6815, 40.8499, 40.5623]
        self.longitudes = [-73.9663, -73.9496, -73.8365, -73.8664, -74.1399]

    def extract(self, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        logger.info("📡 Extracting weather data from Open-Meteo")

        # EXACTLY like your working code - no timezone conversions!
        if end_date is None:
            end = datetime.now()
            end_date = end.strftime("%Y-%m-%d")
        else:
            end = datetime.strptime(end_date, "%Y-%m-%d")
            
        if start_date is None:
            start = end - timedelta(days=7)
            start_date = start.strftime("%Y-%m-%d")
        else:
            start = datetime.strptime(start_date, "%Y-%m-%d")
        
        # Calculate past_days EXACTLY like your working code
        past_days = (end - start).days
        
        logger.info(f"Date range: {start_date} to {end_date}, past_days: {past_days}")

        # Parameters EXACTLY like your working code
        params = {
            "latitude": self.latitudes,
            "longitude": self.longitudes,
            "hourly": [
                "temperature_2m",
                "precipitation",
                "visibility",
                "rain",
                "showers",
                "snowfall",
                "wind_speed_10m",
            ],
            "past_days": past_days,
            "forecast_days": 0,
            "timezone": "America/New_York",
        }

        try:
            logger.debug(f"Making API call with params: {params}")
            responses = self.client.weather_api(WEATHER_API_URL, params=params)
            logger.info(f"✅ API call successful, got {len(responses)} responses")
        except Exception as e:
            logger.error(f"❌ API call failed: {str(e)}")
            raise

        dfs = []

        for i, response in enumerate(responses):
            borough = BOROUGHS[i]
            hourly = response.Hourly()

            # Create timestamps EXACTLY like your working code
            datetimes = pd.date_range(
                start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=hourly.Interval()),
                inclusive="left",
            )

            # Build DataFrame EXACTLY like your working code
            df = pd.DataFrame({
                "borough": borough,
                "datetime": datetimes,
                "temperature_2m": hourly.Variables(0).ValuesAsNumpy(),
                "precipitation": hourly.Variables(1).ValuesAsNumpy(),
                "visibility": hourly.Variables(2).ValuesAsNumpy(),
                "rain": hourly.Variables(3).ValuesAsNumpy(),
                "showers": hourly.Variables(4).ValuesAsNumpy(),
                "snowfall": hourly.Variables(5).ValuesAsNumpy(),
                "wind_speed_10m": hourly.Variables(6).ValuesAsNumpy(),
            })

            # Convert datetime and add date column EXACTLY like your working code
            df["datetime"] = pd.to_datetime(df["datetime"])
            df["date"] = df["datetime"].dt.date
            dfs.append(df)

        if not dfs:
            raise Exception("No weather data was extracted")

        weather_df = pd.concat(dfs, ignore_index=True)

        # Save
        os.makedirs(RAW_DATA_DIR, exist_ok=True)
        filename = f"{RAW_DATA_DIR}/nyc_borough_weather_hourly_{start_date}_to_{end_date}.csv"
        weather_df.to_csv(filename, index=False)

        logger.info(f"✅ Weather data saved: {filename}")
        logger.info(f"Total weather records: {len(weather_df)}")
        
        if 'borough' in weather_df.columns:
            logger.info(f"Weather rows per borough:\n{weather_df['borough'].value_counts()}")
        else:
            logger.warning("No borough column in weather data")

        return weather_df


# =========================
# COLLISIONS EXTRACTOR - FIXED VERSION
# =========================

class CollisionsExtractor:
    """Extract NYC motor vehicle collisions"""
    
    def extract(self, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        logger.info("📡 Extracting NYC collisions data")

        # EXACTLY like your working code
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")
        if start_date is None:
            start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

        logger.info(f"Fetching collisions from {start_date} to {end_date}")

        # EXACTLY like your working code
        params = {
            '$limit': 50000,
            '$where': f"crash_date between '{start_date}' and '{end_date}'"
        }

        try:
            response = requests.get(NYC_COLLISIONS_API, params=params, timeout=30)
            response.raise_for_status()
            
            # FIXED: Use StringIO from io module instead of pd.compat
            df = pd.read_csv(StringIO(response.text))
            logger.info(f"✅ Retrieved {len(df)} collision records")
            
            os.makedirs(RAW_DATA_DIR, exist_ok=True)
            filename = f"{RAW_DATA_DIR}/collisions_{start_date}_to_{end_date}.csv"
            df.to_csv(filename, index=False)

            logger.info(f"✅ Collisions data saved: {filename}")
            
            if 'borough' in df.columns:
                logger.info(f"Collisions per borough:\n{df['borough'].value_counts()}")
            else:
                logger.warning("No borough column in collisions data")
            
            return df
            
        except Exception as e:
            logger.error(f"❌ Failed to extract collisions: {str(e)}")
            raise


# =========================
# PIPELINE ENTRY POINT
# =========================

def run_extraction(start_date: str = None, end_date: str = None):
    logger.info("🚀 Starting data extraction pipeline")
    
    try:
        weather = WeatherExtractor().extract(start_date, end_date)
        collisions = CollisionsExtractor().extract(start_date, end_date)

        logger.info(
            f"🎉 Extraction complete: "
            f"{len(weather)} weather rows, {len(collisions)} collisions"
        )

        return weather, collisions
        
    except Exception as e:
        logger.error(f"❌ Extraction failed: {str(e)}")
        raise


# =========================
# DIRECT TEST
# =========================

if __name__ == "__main__":
    # Simple logging for testing
    logging.basicConfig(level=logging.INFO, 
                       format='%(asctime)s - %(levelname)s - %(message)s')
    
    print("="*60)
    print("TESTING EXTRACTION MODULE")
    print("="*60)
    
    # Test with FIXED dates to avoid timezone issues
    test_start = "2024-01-01"
    test_end = "2024-01-02"
    
    print(f"\nTesting with dates: {test_start} to {test_end}")
    
    try:
        print("\n1. Testing Weather Extraction...")
        weather_df = WeatherExtractor().extract(test_start, test_end)
        print(f"   ✓ Weather: {len(weather_df)} records")
        print(f"   Sample:\n{weather_df.head()}")
        
        print("\n2. Testing Collisions Extraction...")
        collisions_df = CollisionsExtractor().extract(test_start, test_end)
        print(f"   ✓ Collisions: {len(collisions_df)} records")
        print(f"   Sample:\n{collisions_df.head()}")
        
        print("\n" + "="*60)
        print("✅ ALL TESTS PASSED!")
        print("="*60)
        
    except Exception as e:
        print(f"\n❌ TEST FAILED: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()