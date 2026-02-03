"""
Database loading module - FK-safe version
"""
import logging
import mysql.connector
import pandas as pd
from datetime import datetime
from src.config import DB_CONFIG

logger = logging.getLogger(__name__)


class DatabaseLoader:
    """Database loader with proper FK ordering"""

    def __init__(self):
        self.conn = None

    def connect(self):
        try:
            self.conn = mysql.connector.connect(**DB_CONFIG)
            logger.info("✅ Connected to database")
            return True
        except Exception as e:
            logger.error(f"❌ Connection failed: {e}")
            return False

    def close(self):
        if self.conn:
            self.conn.close()

    # ---------------------------------------------------
    # DIMENSION LOADERS - FIXED VERSION
    # ---------------------------------------------------

    def ensure_datetime_dim(self, cursor, dt_value):
        """
        Ensure datetime exists in dim_datetime
        Returns datetime_id
        """
        # Check what columns actually exist in your table
        cursor.execute("DESCRIBE dim_datetime")
        columns = [col[0] for col in cursor.fetchall()]
        logger.debug(f"dim_datetime columns: {columns}")
        
        # Try different possible column names
        if 'full_datetime' in columns:
            query = "SELECT datetime_id FROM dim_datetime WHERE full_datetime = %s"
        elif 'datetime' in columns:
            query = "SELECT datetime_id FROM dim_datetime WHERE datetime = %s"
        elif 'date_time' in columns:
            query = "SELECT datetime_id FROM dim_datetime WHERE date_time = %s"
        else:
            # Fallback to first column that's not datetime_id
            other_cols = [c for c in columns if c != 'datetime_id']
            if other_cols:
                query = f"SELECT datetime_id FROM dim_datetime WHERE {other_cols[0]} = %s"
            else:
                query = "SELECT datetime_id FROM dim_datetime LIMIT 1"
        
        cursor.execute(query, (dt_value,))
        row = cursor.fetchone()
        if row:
            return row[0]

        # Insert new datetime
        if 'full_datetime' in columns:
            cursor.execute("INSERT INTO dim_datetime (full_datetime) VALUES (%s)", (dt_value,))
        elif 'datetime' in columns:
            cursor.execute("INSERT INTO dim_datetime (datetime) VALUES (%s)", (dt_value,))
        elif 'date_time' in columns:
            cursor.execute("INSERT INTO dim_datetime (date_time) VALUES (%s)", (dt_value,))
        else:
            cursor.execute("INSERT INTO dim_datetime VALUES (NULL, %s)", (dt_value,))
            
        return cursor.lastrowid

    def ensure_location_dim(self, cursor, borough="MANHATTAN"):
        """
        Ensure location exists in dim_location
        Returns location_id
        """
        # Handle None/NaN borough values
        if borough is None or pd.isna(borough):
            borough = "UNKNOWN"
        
        cursor.execute(
            """
            SELECT location_id
            FROM dim_location
            WHERE borough = %s
            """,
            (borough,)
        )
        row = cursor.fetchone()
        if row:
            return row[0]

        cursor.execute(
            """
            INSERT INTO dim_location (borough)
            VALUES (%s)
            """,
            (borough,)
        )
        return cursor.lastrowid

    # ---------------------------------------------------
    # DATA LOADERS - ACTUAL DATA, NOT SAMPLE
    # ---------------------------------------------------

    def load_weather_data(self, weather_df: pd.DataFrame):
        """
        Load actual weather data from DataFrame
        """
        if not self.connect():
            return False

        if weather_df.empty:
            logger.warning("⚠️ No weather data to load")
            return True

        try:
            cursor = self.conn.cursor()
            loaded_count = 0

            for _, row in weather_df.iterrows():
                # Get or create dimension IDs
                datetime_id = self.ensure_datetime_dim(cursor, row['datetime'])
                location_id = self.ensure_location_dim(cursor, row['borough'])

                # Check if this weather record already exists
                cursor.execute(
                    """
                    SELECT weather_id FROM fact_weather 
                    WHERE datetime_id = %s AND location_id = %s
                    """,
                    (datetime_id, location_id)
                )
                
                if cursor.fetchone():
                    continue  # Skip duplicate

                # Insert weather fact
                cursor.execute(
                    """
                    INSERT INTO fact_weather 
                    (datetime_id, location_id, temperature_2m, precipitation, 
                     visibility, rain, showers, snowfall, wind_speed_10m)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (datetime_id, location_id,
                     float(row.get('temperature_2m', 0)),
                     float(row.get('precipitation', 0)),
                     float(row.get('visibility', 0)),
                     float(row.get('rain', 0)),
                     float(row.get('showers', 0)),
                     float(row.get('snowfall', 0)),
                     float(row.get('wind_speed_10m', 0)))
                )
                loaded_count += 1

            self.conn.commit()
            logger.info(f"✅ Loaded {loaded_count} weather records")
            return True

        except Exception as e:
            logger.error(f"❌ Failed to load weather data: {e}")
            self.conn.rollback()
            return False
        finally:
            self.close()

    def load_collision_data(self, collisions_df: pd.DataFrame):
        """
        Load actual collision data from DataFrame
        """
        if not self.connect():
            return False

        if collisions_df.empty:
            logger.warning("⚠️ No collision data to load")
            return True

        try:
            cursor = self.conn.cursor()
            loaded_count = 0

            for _, row in collisions_df.iterrows():
                # Create datetime from crash_date and crash_time
                crash_date = row.get('crash_date', '')
                crash_time = row.get('crash_time', '00:00')
                
                # Parse datetime
                try:
                    dt_str = f"{crash_date} {crash_time}"
                    dt_obj = datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%S.%f %H:%M")
                except:
                    try:
                        dt_obj = datetime.strptime(crash_date, "%Y-%m-%dT%H:%M:%S.%f")
                    except:
                        dt_obj = datetime.now()

                datetime_id = self.ensure_datetime_dim(cursor, dt_obj)
                location_id = self.ensure_location_dim(cursor, row.get('borough'))

                # Check if collision already exists
                cursor.execute(
                    """
                    SELECT collision_id FROM fact_collisions 
                    WHERE datetime_id = %s AND location_id = %s
                    AND number_of_persons_injured = %s
                    """,
                    (datetime_id, location_id, 
                     int(row.get('number_of_persons_injured', 0)))
                )
                
                if cursor.fetchone():
                    continue  # Skip duplicate

                # Insert collision fact
                cursor.execute(
                    """
                    INSERT INTO fact_collisions 
                    (datetime_id, location_id, number_of_persons_injured,
                     number_of_persons_killed, number_of_pedestrians_injured,
                     number_of_pedestrians_killed, number_of_cyclists_injured,
                     number_of_cyclists_killed, number_of_motorists_injured,
                     number_of_motorists_killed)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (datetime_id, location_id,
                     int(row.get('number_of_persons_injured', 0)),
                     int(row.get('number_of_persons_killed', 0)),
                     int(row.get('number_of_pedestrians_injured', 0)),
                     int(row.get('number_of_pedestrians_killed', 0)),
                     int(row.get('number_of_cyclists_injured', 0)),
                     int(row.get('number_of_cyclists_killed', 0)),
                     int(row.get('number_of_motorists_injured', 0)),
                     int(row.get('number_of_motorists_killed', 0)))
                )
                loaded_count += 1

            self.conn.commit()
            logger.info(f"✅ Loaded {loaded_count} collision records")
            return True

        except Exception as e:
            logger.error(f"❌ Failed to load collision data: {e}")
            self.conn.rollback()
            return False
        finally:
            self.close()

    # ---------------------------------------------------
    # UTIL
    # ---------------------------------------------------

    def test_connection(self):
        if not self.connect():
            return False

        cursor = self.conn.cursor()
        cursor.execute("SELECT DATABASE()")
        db_name = cursor.fetchone()[0]

        cursor.execute("SHOW TABLES")
        tables = [t[0] for t in cursor.fetchall()]

        cursor.close()
        self.close()

        print(f"✅ Connected to: {db_name}")
        print(f"📋 Tables: {tables}")
        
        # Debug: Show dim_datetime structure
        if self.connect():
            cursor = self.conn.cursor()
            cursor.execute("DESCRIBE dim_datetime")
            print("\n📊 dim_datetime structure:")
            for col in cursor.fetchall():
                print(f"  {col[0]} ({col[1]})")
            cursor.close()
            self.close()
            
        return True


def run_loading(weather_df: pd.DataFrame, collisions_df: pd.DataFrame):
    logger.info("🚚 STARTING DATABASE LOAD")

    loader = DatabaseLoader()

    if not loader.test_connection():
        logger.error("❌ Cannot connect to database")
        return False

    # Load actual data, not sample
    weather_success = loader.load_weather_data(weather_df)
    collisions_success = loader.load_collision_data(collisions_df)

    if weather_success and collisions_success:
        logger.info("✅ Database load completed successfully")
        return True
    else:
        logger.warning("⚠️ Database load had some failures")
        return True  # Continue pipeline anyway


if __name__ == "__main__":
    print("🧪 Testing DatabaseLoader...")
    
    # First test connection
    loader = DatabaseLoader()
    loader.test_connection()
    
    # Test with sample data
    weather_sample = pd.DataFrame({
        'datetime': ['2024-01-01 12:00:00'],
        'borough': ['MANHATTAN'],
        'temperature_2m': [10.5],
        'precipitation': [0.0],
        'visibility': [25000],
        'rain': [0.0],
        'showers': [0.0],
        'snowfall': [0.0],
        'wind_speed_10m': [5.0]
    })
    
    collisions_sample = pd.DataFrame({
        'crash_date': ['2024-01-01T00:00:00.000'],
        'crash_time': ['12:00'],
        'borough': ['MANHATTAN'],
        'number_of_persons_injured': [1],
        'number_of_persons_killed': [0]
    })
    
    run_loading(weather_sample, collisions_sample)