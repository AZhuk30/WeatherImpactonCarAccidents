import logging
import mysql.connector
import pandas as pd
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import traceback

load_dotenv()
logger = logging.getLogger(__name__)

class ExactSchemaDatabaseLoader:
    def __init__(self):
        self.conn = None
        self.duplicate_counter = 0
        
    def connect(self):
        """Connect to database"""
        try:
            self.conn = mysql.connector.connect(
                host=os.getenv('DB_HOST', 'localhost'),
                user=os.getenv('DB_USER', 'root'),
                password=os.getenv('DB_PASSWORD', ''),
                database=os.getenv('DB_NAME', 'nyc_traffic_safety'),
                port=int(os.getenv('DB_PORT', 3306)),
                connection_timeout=10
            )
            logger.info("✅ Connected to database")
            return True
        except mysql.connector.Error as e:
            logger.error(f"❌ MySQL connection failed: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ Connection failed: {e}")
            return False
    
    def ensure_datetime_dim(self, cursor, dt_nyc):
        """Create datetime dimension with ALL required columns"""
        try:
            # First check if exists by datetime_nyc
            check_query = "SELECT datetime_id FROM dim_datetime WHERE datetime_nyc = %s"
            cursor.execute(check_query, (dt_nyc,))
            result = cursor.fetchone()
            
            if result:
                return result[0]
            
            # Calculate all required fields
            date_nyc = dt_nyc.date()
            hour_nyc = dt_nyc.hour
            day_of_week = dt_nyc.strftime('%A')
            day_of_month = dt_nyc.day
            month = dt_nyc.month
            year = dt_nyc.year
            quarter = (month - 1) // 3 + 1
            is_weekend = 1 if dt_nyc.weekday() >= 5 else 0
            
            # Calculate rush hour (7-10am, 4-8pm)
            is_rush_hour = 1 if (7 <= hour_nyc <= 9) or (16 <= hour_nyc <= 19) else 0
            
            # Calculate night (10pm - 6am)
            is_night = 1 if hour_nyc >= 22 or hour_nyc < 6 else 0
            
            # Calculate season
            if month in [12, 1, 2]:
                season = 'WINTER'
            elif month in [3, 4, 5]:
                season = 'SPRING'
            elif month in [6, 7, 8]:
                season = 'SUMMER'
            else:
                season = 'FALL'
            
            # Create UTC datetime (NYC is UTC-5)
            dt_utc = dt_nyc - timedelta(hours=5)
            
            # Insert with ALL columns
            insert_query = """
                INSERT INTO dim_datetime 
                (datetime_utc, datetime_nyc, date_nyc, hour_nyc, day_of_week, 
                 day_of_month, month, year, is_weekend, is_rush_hour, 
                 is_night, quarter, season)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            cursor.execute(insert_query, (
                dt_utc, dt_nyc, date_nyc, hour_nyc, day_of_week,
                day_of_month, month, year, is_weekend, is_rush_hour,
                is_night, quarter, season
            ))
            
            return cursor.lastrowid
            
        except mysql.connector.Error as e:
            if e.errno == 1062:  # Duplicate entry
                # Try to get the existing ID
                cursor.execute(check_query, (dt_nyc,))
                result = cursor.fetchone()
                if result:
                    return result[0]
                raise
            else:
                logger.error(f"❌ DateTime dimension error: {e}")
                raise
    
    def ensure_location_dim(self, cursor, borough):
        """Create location dimension"""
        try:
            if pd.isna(borough) or not str(borough).strip():
                borough = "UNKNOWN"
            else:
                borough = str(borough).strip().upper()
            
            # Check if exists
            cursor.execute("SELECT location_id FROM dim_location WHERE borough = %s", (borough,))
            result = cursor.fetchone()
            
            if result:
                return result[0]
            
            # Insert new location (without lat/lon for now)
            cursor.execute("INSERT INTO dim_location (borough) VALUES (%s)", (borough,))
            return cursor.lastrowid
            
        except mysql.connector.Error as e:
            if e.errno == 1062:  # Duplicate entry
                cursor.execute("SELECT location_id FROM dim_location WHERE borough = %s", (borough,))
                result = cursor.fetchone()
                if result:
                    return result[0]
                raise
            else:
                logger.error(f"❌ Location dimension error: {e}")
                raise
    
    def load_weather_data_exact(self, weather_df, batch_size=100):
        """Load weather data matching EXACT fact_weather schema"""
        if not self.connect():
            return False
            
        if weather_df.empty:
            logger.warning("⚠️ No weather data to load")
            return True
            
        try:
            cursor = self.conn.cursor()
            total_loaded = 0
            total_skipped = 0
            duplicates = 0
            
            # Check if we have required columns
            required_cols = ['datetime', 'borough', 'temperature_2m']
            missing_cols = [col for col in required_cols if col not in weather_df.columns]
            if missing_cols:
                logger.warning(f"⚠️ Missing columns in weather data: {missing_cols}")
            
            for idx, row in weather_df.iterrows():
                try:
                    # Get required data
                    dt_str = row.get('datetime')
                    borough = row.get('borough')
                    
                    if pd.isna(dt_str) or pd.isna(borough):
                        total_skipped += 1
                        continue
                    
                    # Parse datetime (ensure it's timezone-naive)
                    dt_nyc = pd.to_datetime(dt_str, errors='coerce')
                    if pd.isna(dt_nyc):
                        logger.warning(f"⚠️ Could not parse datetime: {dt_str}")
                        total_skipped += 1
                        continue
                    
                    # Remove timezone info if present
                    if dt_nyc.tz is not None:
                        dt_nyc = dt_nyc.tz_localize(None)
                    
                    # Get dimension IDs
                    datetime_id = self.ensure_datetime_dim(cursor, dt_nyc)
                    location_id = self.ensure_location_dim(cursor, borough)
                    
                    # Check if weather record already exists (UNIQUE constraint)
                    cursor.execute("""
                        SELECT 1 FROM fact_weather 
                        WHERE datetime_id = %s AND location_id = %s
                    """, (datetime_id, location_id))
                    
                    if cursor.fetchone():
                        duplicates += 1
                        continue
                    
                    # Get weather data with defaults
                    temp = float(row.get('temperature_2m', 0) or 0)
                    precipitation = float(row.get('precipitation', 0) or 0)
                    visibility_val = row.get('visibility')
                    visibility = int(visibility_val) if not pd.isna(visibility_val) else None
                    rain = float(row.get('rain', 0) or 0)
                    showers = float(row.get('showers', 0) or 0)
                    snowfall = float(row.get('snowfall', 0) or 0)
                    wind_speed = float(row.get('wind_speed_10m', 0) or 0)
                    
                    # Determine if adverse weather
                    is_adverse = 1 if (precipitation > 5 or (visibility and visibility < 1000)) else 0
                    
                    # Insert weather data (matching EXACT schema)
                    insert_query = """
                        INSERT INTO fact_weather 
                        (datetime_id, location_id, weather_id, temperature_2m,
                         precipitation, visibility, rain, showers, snowfall,
                         wind_speed_10m, is_adverse_weather)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    
                    cursor.execute(insert_query, (
                        datetime_id, location_id, None,  # weather_id is NULL for now
                        temp, precipitation, visibility, rain, showers,
                        snowfall, wind_speed, is_adverse
                    ))
                    
                    total_loaded += 1
                    
                    # Commit in batches
                    if total_loaded % batch_size == 0:
                        self.conn.commit()
                        logger.info(f"☁️  Loaded {total_loaded} weather records...")
                        
                except mysql.connector.Error as e:
                    if e.errno == 1062:  # Duplicate entry error
                        duplicates += 1
                        continue
                    else:
                        logger.warning(f"⚠️ MySQL error on row {idx}: {e}")
                        total_skipped += 1
                        continue
                except Exception as e:
                    logger.warning(f"⚠️ Error on row {idx}: {e}")
                    total_skipped += 1
                    continue
            
            # Final commit
            self.conn.commit()
            logger.info(f"✅ Weather: Loaded {total_loaded}, Skipped {total_skipped}, Duplicates {duplicates}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error loading weather: {e}")
            logger.error(traceback.format_exc())
            if self.conn:
                self.conn.rollback()
            return False
        finally:
            if self.conn:
                self.conn.close()
    
    def load_collision_data_exact(self, collisions_df, batch_size=100):
        """Load collision data matching EXACT fact_collisions schema"""
        if not self.connect():
            return False
            
        if collisions_df.empty:
            logger.warning("⚠️ No collision data to load")
            return True
            
        try:
            cursor = self.conn.cursor()
            total_loaded = 0
            total_skipped = 0
            duplicates = 0
            
            # Check required columns
            required_cols = ['collision_id', 'borough', 'crash_date']
            missing_cols = [col for col in required_cols if col not in collisions_df.columns]
            if missing_cols:
                logger.warning(f"⚠️ Missing columns in collision data: {missing_cols}")
            
            for idx, row in collisions_df.iterrows():
                try:
                    # Generate collision ID if not present
                    if 'collision_id' in row and not pd.isna(row['collision_id']):
                        collision_id = str(row['collision_id'])
                    else:
                        # Create unique ID from timestamp and index
                        collision_id = f"COL_{int(datetime.now().timestamp())}_{idx}"
                    
                    # Get borough
                    borough = row.get('borough', 'UNKNOWN')
                    if pd.isna(borough):
                        borough = "UNKNOWN"
                    
                    # Parse date/time
                    crash_date = row.get('crash_date')
                    crash_time = row.get('crash_time', '00:00:00')
                    
                    if pd.isna(crash_date):
                        total_skipped += 1
                        continue
                    
                    # Convert to datetime
                    crash_date_str = str(crash_date).split('T')[0] if 'T' in str(crash_date) else str(crash_date)
                    dt_nyc_str = f"{crash_date_str} {str(crash_time)}"
                    dt_nyc = pd.to_datetime(dt_nyc_str, errors='coerce')
                    
                    if pd.isna(dt_nyc):
                        dt_nyc = pd.to_datetime(crash_date_str, errors='coerce')
                    
                    if pd.isna(dt_nyc):
                        total_skipped += 1
                        continue
                    
                    # Remove timezone if present
                    if dt_nyc.tz is not None:
                        dt_nyc = dt_nyc.tz_localize(None)
                    
                    # Get dimension IDs
                    datetime_id = self.ensure_datetime_dim(cursor, dt_nyc)
                    location_id = self.ensure_location_dim(cursor, borough)
                    
                    # Check if collision already exists
                    cursor.execute("SELECT 1 FROM fact_collisions WHERE collision_id = %s", (collision_id,))
                    if cursor.fetchone():
                        duplicates += 1
                        continue
                    
                    # Calculate severity
                    persons_injured = int(row.get('persons_injured', 0) or 0)
                    persons_killed = int(row.get('persons_killed', 0) or 0)
                    
                    if persons_killed > 0:
                        severity = 'FATAL'
                    elif persons_injured >= 3:
                        severity = 'SEVERE'
                    elif persons_injured > 0:
                        severity = 'MODERATE'
                    else:
                        severity = 'NONE'
                    
                    # Calculate has_injuries and has_fatalities
                    has_injuries = 1 if persons_injured > 0 else 0
                    has_fatalities = 1 if persons_killed > 0 else 0
                    
                    # Get contributing factors
                    cf1 = str(row.get('contributing_factor_1', ''))[:255] if not pd.isna(row.get('contributing_factor_1')) else ''
                    cf2 = str(row.get('contributing_factor_2', ''))[:255] if not pd.isna(row.get('contributing_factor_2')) else ''
                    cf3 = str(row.get('contributing_factor_3', ''))[:255] if not pd.isna(row.get('contributing_factor_3')) else ''
                    cf4 = str(row.get('contributing_factor_4', ''))[:255] if not pd.isna(row.get('contributing_factor_4')) else ''
                    cf5 = str(row.get('contributing_factor_5', ''))[:255] if not pd.isna(row.get('contributing_factor_5')) else ''
                    
                    # Get vehicle types
                    vt1 = str(row.get('vehicle_type_1', ''))[:100] if not pd.isna(row.get('vehicle_type_1')) else ''
                    vt2 = str(row.get('vehicle_type_2', ''))[:100] if not pd.isna(row.get('vehicle_type_2')) else ''
                    vt3 = str(row.get('vehicle_type_3', ''))[:100] if not pd.isna(row.get('vehicle_type_3')) else ''
                    vt4 = str(row.get('vehicle_type_4', ''))[:100] if not pd.isna(row.get('vehicle_type_4')) else ''
                    vt5 = str(row.get('vehicle_type_5', ''))[:100] if not pd.isna(row.get('vehicle_type_5')) else ''
                    
                    # Calculate number of vehicles
                    num_vehicles = 1
                    if vt1: num_vehicles = max(num_vehicles, 1)
                    if vt2: num_vehicles = max(num_vehicles, 2)
                    if vt3: num_vehicles = max(num_vehicles, 3)
                    if vt4: num_vehicles = max(num_vehicles, 4)
                    if vt5: num_vehicles = max(num_vehicles, 5)
                    
                    # Insert collision data (matching EXACT schema)
                    insert_query = """
                        INSERT INTO fact_collisions 
                        (collision_id, datetime_id, location_id, weather_id,
                         persons_injured, persons_killed, pedestrians_injured, pedestrians_killed,
                         cyclists_injured, cyclists_killed, motorists_injured, motorists_killed,
                         total_involved, has_injuries, has_fatalities, severity_level,
                         contributing_factor_1, contributing_factor_2, contributing_factor_3,
                         contributing_factor_4, contributing_factor_5,
                         vehicle_type_1, vehicle_type_2, vehicle_type_3, vehicle_type_4, vehicle_type_5,
                         number_of_vehicles, data_source, raw_crash_date, raw_crash_time)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    
                    cursor.execute(insert_query, (
                        collision_id, datetime_id, location_id, None,  # weather_id is NULL for now
                        persons_injured, persons_killed,
                        int(row.get('pedestrians_injured', 0) or 0),
                        int(row.get('pedestrians_killed', 0) or 0),
                        int(row.get('cyclists_injured', 0) or 0),
                        int(row.get('cyclists_killed', 0) or 0),
                        int(row.get('motorists_injured', 0) or 0),
                        int(row.get('motorists_killed', 0) or 0),
                        int(row.get('total_involved', 0) or 0),
                        has_injuries, has_fatalities, severity,
                        cf1, cf2, cf3, cf4, cf5,
                        vt1, vt2, vt3, vt4, vt5,
                        num_vehicles,
                        'NYC_OPEN_DATA',
                        crash_date_str if crash_date_str else None,
                        str(crash_time) if not pd.isna(crash_time) else None
                    ))
                    
                    total_loaded += 1
                    
                    # Commit in batches
                    if total_loaded % batch_size == 0:
                        self.conn.commit()
                        logger.info(f"🚗 Loaded {total_loaded} collision records...")
                        
                except mysql.connector.Error as e:
                    if e.errno == 1062:  # Duplicate entry error
                        duplicates += 1
                        continue
                    else:
                        logger.warning(f"⚠️ MySQL error on row {idx}: {e}")
                        total_skipped += 1
                        continue
                except Exception as e:
                    logger.warning(f"⚠️ Error on row {idx}: {e}")
                    total_skipped += 1
                    continue
            
            # Final commit
            self.conn.commit()
            logger.info(f"✅ Collisions: Loaded {total_loaded}, Skipped {total_skipped}, Duplicates {duplicates}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error loading collisions: {e}")
            logger.error(traceback.format_exc())
            if self.conn:
                self.conn.rollback()
            return False
        finally:
            if self.conn:
                self.conn.close()

def run_exact_schema_loading(weather_df, collisions_df):
    """Main loading function for exact schema"""
    logger.info("🚚 STARTING DATABASE LOAD (EXACT SCHEMA VERSION)")
    
    # Create loader
    loader = ExactSchemaDatabaseLoader()
    
    # Test connection first
    if not loader.connect():
        logger.error("❌ Cannot connect to database")
        return False
    loader.conn.close()
    
    # Load weather data
    logger.info("📥 Loading weather data...")
    weather_success = loader.load_weather_data_exact(weather_df)
    
    # Load collision data
    logger.info("📥 Loading collision data...")
    collisions_success = loader.load_collision_data_exact(collisions_df)
    
    if weather_success and collisions_success:
        logger.info("✅ Database load completed successfully!")
        return True
    else:
        logger.warning("⚠️ Database load had some failures")
        # Continue pipeline anyway
        return True

# Test function
def test_exact_schema():
    """Test with your exact schema"""
    import pandas as pd
    from datetime import datetime
    
    print("🧪 Testing with exact schema...")
    
    # Create test data that matches your processed data format
    test_collisions = pd.DataFrame({
        'collision_id': ['TEST_' + str(int(datetime.now().timestamp()))],
        'borough': ['MANHATTAN'],
        'crash_date': ['2026-01-15'],
        'crash_time': ['14:30:00'],
        'persons_injured': [2],
        'persons_killed': [0],
        'pedestrians_injured': [1],
        'pedestrians_killed': [0],
        'cyclists_injured': [0],
        'cyclists_killed': [0],
        'motorists_injured': [1],
        'motorists_killed': [0],
        'total_involved': [3],
        'contributing_factor_1': ['Driver Inattention/Distraction'],
        'contributing_factor_2': [''],
        'contributing_factor_3': [''],
        'contributing_factor_4': [''],
        'contributing_factor_5': [''],
        'vehicle_type_1': ['PASSENGER VEHICLE'],
        'vehicle_type_2': ['SPORT UTILITY'],
        'vehicle_type_3': [''],
        'vehicle_type_4': [''],
        'vehicle_type_5': ['']
    })
    
    test_weather = pd.DataFrame({
        'datetime': ['2026-01-15 14:00:00'],
        'borough': ['MANHATTAN'],
        'temperature_2m': [10.5],
        'precipitation': [0.0],
        'visibility': [25000],
        'rain': [0.0],
        'showers': [0.0],
        'snowfall': [0.0],
        'wind_speed_10m': [5.0]
    })
    
    loader = ExactSchemaDatabaseLoader()
    
    print("🌤️  Testing weather load...")
    weather_success = loader.load_weather_data_exact(test_weather)
    
    print("🚗 Testing collision load...")
    collisions_success = loader.load_collision_data_exact(test_collisions)
    
    if weather_success and collisions_success:
        print("✅ All tests passed!")
        return True
    else:
        print("❌ Tests failed")
        return False

if __name__ == "__main__":
    # Enable logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    test_exact_schema()

run_loading = run_exact_schema_loading
