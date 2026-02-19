"""
Data extraction from APIs
Weather (Open-Meteo) + NYC Motor Vehicle Collisions
Uses src.config for URLs, paths, settings
"""

import logging
from datetime import datetime, timedelta
from io import StringIO 

import pandas as pd
import requests
import requests_cache
from retry_requests import retry
import openmeteo_requests

# =========================
# CONFIGURATION
# =========================

from src.config import (
    BOROUGH_LIST,
    BOROUGHS,
    WEATHER_API_URL,
    NYC_COLLISIONS_API,
    RAW_DATA_DIR,
    WEATHER_PARAMS,
    PIPELINE_CONFIG,
)

logger = logging.getLogger(__name__)

# =========================
# WEATHER EXTRACTOR
# =========================

class WeatherExtractor:
    """Extract hourly weather data for NYC boroughs using Open-Meteo"""
    
    def __init__(self):
        
        cache_session = requests_cache.CachedSession(".cache", expire_after=3600)
        retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
        self.client = openmeteo_requests.Client(session=retry_session)
        
      # Pull borough coordinates from config
        self.latitudes = [BOROUGHS[b]["lat"] for b in BOROUGH_LIST]
        self.longitudes = [BOROUGHS[b]["lon"] for b in BOROUGH_LIST]

    def extract(self, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        logger.info("Extracting weather data from Open-Meteo")

        # Data range
        if end_date is None:
            end = datetime.now()
            end_date = end.strftime("%Y-%m-%d")
        else:
            end = datetime.strptime(end_date, "%Y-%m-%d")
            
        if start_date is None:
            start = end - timedelta(days=PIPELINE_CONFIG["lookback_days"])
            start_date = start.strftime("%Y-%m-%d")
        else:
            start = datetime.strptime(start_date, "%Y-%m-%d")
        
        past_days = (end - start).days
        
        logger.info(f"Date range: {start_date} to {end_date}, past_days: {past_days}")

        # Parameters
        params = {
            "latitude": self.latitudes,
            "longitude": self.longitudes,
            "hourly": WEATHER_PARAMS,
            "past_days": past_days,
            "forecast_days": 0,
            "timezone": PIPELINE_CONFIG["timezone"],
        }

        responses = self.client.weather_api(WEATHER_API_URL, params=params)
        logger.info(f"API call successful, got {len(responses)} responses")

        dfs = []

        for i, response in enumerate(responses):
            borough = BOROUGH_LIST[i]
            hourly = response.Hourly()

            # Create timestamps
            datetimes = pd.date_range(
                start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=hourly.Interval()),
                inclusive="left",
            )

            # Build DataFrame
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

            # Convert datetime and add date column
            df["datetime"] = pd.to_datetime(df["datetime"])
            df["date"] = df["datetime"].dt.date
            dfs.append(df)

        if not dfs:
            raise Exception("No weather data was extracted")

        weather_df = pd.concat(dfs, ignore_index=True)

        # Save
        RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
        filename = RAW_DATA_DIR / f"nyc_borough_weather_hourly_{start_date}_to_{end_date}.csv"
        weather_df.to_csv(filename, index=False)

        logger.info(f"Weather data saved: {filename}")
        logger.info(f"Total weather records: {len(weather_df)}")

        if "borough" in weather_df.columns:
            logger.info(f"Weather rows per borough:\n{weather_df['borough'].value_counts()}")

        return weather_df


# =========================
# COLLISIONS EXTRACTOR - FIXED VERSION
# =========================

class CollisionsExtractor:
    """Extract NYC motor vehicle collisions"""
    
    def extract(self, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        logger.info("Extracting NYC collisions data")

        # EXACTLY like your working code
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")
        if start_date is None:
            start_date = (datetime.now() - timedelta(days=PIPELINE_CONFIG["lookback_days"])).strftime("%Y-%m-%d")

        logger.info(f"Fetching collisions from {start_date} to {end_date}")

        
        params = {
            '$limit': 50000,
            '$where': f"crash_date between '{start_date}' and '{end_date}'"
        }

        response = requests.get(NYC_COLLISIONS_API, params=params, timeout=30)
        response.raise_for_status()

        df = pd.read_csv(StringIO(response.text))
        logger.info(f"Retrieved {len(df)} collision records")

        RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
        filename = RAW_DATA_DIR / f"collisions_{start_date}_to_{end_date}.csv"
        df.to_csv(filename, index=False)

        logger.info(f"Collisions data saved: {filename}")

        if "borough" in df.columns:
            logger.info(f"Collisions per borough:\n{df['borough'].value_counts()}")

        return df


# =========================
# PIPELINE ENTRY POINT
# =========================

def run_extraction(start_date: str = None, end_date: str = None):
    logger.info("Starting data extraction pipeline")
    weather = WeatherExtractor().extract(start_date, end_date)
    collisions = CollisionsExtractor().extract(start_date, end_date)

    logger.info(f"Extraction complete: {len(weather)} weather rows, {len(collisions)} collisions")
    return weather, collisions


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
        print("ALL TESTS PASSED!")
        print("="*60)
        
    except Exception as e:
        print(f"\n TEST FAILED: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()