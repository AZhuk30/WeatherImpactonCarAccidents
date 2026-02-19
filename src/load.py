"""
Database loading for NYC Traffic Safety ETL pipeline.

Expects DataFrames that have already been cleaned by transform.py:
  - Column names are normalised (lowercase, underscored)
  - Injury columns guaranteed to exist as ints
  - severity_level, total_involved pre-computed
  - borough is uppercase and stripped
  - crash_datetime is tz-naive
"""

import logging
from datetime import datetime
from typing import Optional

import mysql.connector
import pandas as pd
from zoneinfo import ZoneInfo

from src.config import DB_CONFIG, SKIP_DATABASE

logger = logging.getLogger(__name__)

NY_TZ  = ZoneInfo("America/New_York")
UTC_TZ = ZoneInfo("UTC")


# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------

def _str_or_none(row: pd.Series, col: str) -> Optional[str]:
    v = row.get(col)
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = str(v).strip()
    return s if s else None


def _int(row: pd.Series, col: str, default: int = 0) -> int:
    v = row.get(col, default)
    try:
        return int(v) if v is not None and not pd.isna(v) else default
    except (ValueError, TypeError):
        return default


def _to_nyc_naive(dt_value) -> Optional[datetime]:
    """Convert any datetime-like value to tz-naive NYC local datetime."""
    dt = pd.to_datetime(dt_value, errors="coerce")
    if pd.isna(dt):
        return None
    if getattr(dt, "tzinfo", None) is not None:
        return dt.astimezone(NY_TZ).replace(tzinfo=None)
    return dt.to_pydatetime()


def _nyc_naive_to_utc(dt: datetime) -> datetime:
    """Interpret a tz-naive datetime as NYC local and return UTC."""
    return dt.replace(tzinfo=NY_TZ).astimezone(UTC_TZ).replace(tzinfo=None)


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------

class DatabaseLoader:
    """
    Loads cleaned weather and collision DataFrames into MySQL.

    Schema:
      dim_datetime, dim_location, fact_weather, fact_collisions
    """

    def __init__(self):
        self.conn = None

    # ── Connection ────────────────────────────────────────────────────────

    def connect(self) -> bool:
        user     = DB_CONFIG.get("user")
        password = DB_CONFIG.get("password")

        if not user or not password:
            logger.error("DB_USER and DB_PASSWORD must be set in environment — refusing to connect")
            return False

        try:
            self.conn = mysql.connector.connect(
                host=DB_CONFIG["host"],
                port=DB_CONFIG.get("port", 3306),
                database=DB_CONFIG["database"],
                user=user,
                password=password,
                connection_timeout=15,
            )
            logger.info("Connected to database")
            return True
        except mysql.connector.Error as e:
            logger.error(f"MySQL connection failed: {e}")
            return False

    def close(self):
        if self.conn:
            try:
                self.conn.close()
            except Exception:
                pass
            self.conn = None

    # ── Dimension upserts ─────────────────────────────────────────────────

    def _upsert_datetime(self, cursor, dt_nyc: datetime) -> int:
        dt_utc   = _nyc_naive_to_utc(dt_nyc)
        month    = dt_nyc.month
        hour     = dt_nyc.hour

        if month in (12, 1, 2):   season = "WINTER"
        elif month in (3, 4, 5):  season = "SPRING"
        elif month in (6, 7, 8):  season = "SUMMER"
        else:                     season = "FALL"

        cursor.execute(
            """
            INSERT INTO dim_datetime
                (datetime_utc, datetime_nyc, date_nyc, hour_nyc, day_of_week,
                 day_of_month, month, year, quarter, season,
                 is_weekend, is_rush_hour, is_night)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE datetime_id = LAST_INSERT_ID(datetime_id)
            """,
            (
                dt_utc,
                dt_nyc,
                dt_nyc.date(),
                hour,
                dt_nyc.strftime("%A"),
                dt_nyc.day,
                month,
                dt_nyc.year,
                (month - 1) // 3 + 1,
                season,
                1 if dt_nyc.weekday() >= 5 else 0,
                1 if hour in (7, 8, 9, 16, 17, 18, 19) else 0,
                1 if hour >= 22 or hour < 6 else 0,
            ),
        )
        return int(cursor.lastrowid)

    def _get_or_create_location(self, cursor, borough: str) -> int:
        b = (borough or "UNKNOWN").upper().strip()
        cursor.execute(
            "SELECT location_id FROM dim_location WHERE borough = %s LIMIT 1", (b,)
        )
        row = cursor.fetchone()
        if row:
            return int(row[0])
        cursor.execute("INSERT INTO dim_location (borough) VALUES (%s)", (b,))
        return int(cursor.lastrowid)

    # ── Shared load scaffolding ───────────────────────────────────────────

    def _run_load(self, df: pd.DataFrame, name: str, insert_fn) -> bool:
        """
        Guards, connection, commit loop, rollback, and teardown in one place.
        insert_fn(cursor, datetime_cache, location_cache) iterates the DataFrame.
        """
        if SKIP_DATABASE:
            logger.info(f"SKIP_DATABASE=True — skipping {name} load")
            return True
        if df is None or df.empty:
            logger.warning(f"No {name} data to load")
            return True
        if not self.connect():
            return False

        datetime_cache: dict  = {}
        location_cache: dict  = {}

        try:
            cur = self.conn.cursor()
            loaded = insert_fn(cur, datetime_cache, location_cache)
            self.conn.commit()
            logger.info(f"{name} load complete — {loaded} rows inserted")
            return True
        except Exception as e:
            logger.error(f"{name} load failed: {e}", exc_info=True)
            try:
                self.conn.rollback()
            except Exception:
                pass
            return False
        finally:
            self.close()

    # ── Weather load ──────────────────────────────────────────────────────

    def load_weather(self, df: pd.DataFrame, batch_commit: int = 500) -> bool:

        def _insert(cur, dt_cache, loc_cache):
            sql = """
                INSERT IGNORE INTO fact_weather
                    (datetime_id, location_id,
                     temperature_2m, precipitation, visibility,
                     rain, showers, snowfall, wind_speed_10m,
                     weather_category, weather_severity, is_adverse_weather)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """
            loaded = 0
            for _, row in df.iterrows():
                dt_nyc  = _to_nyc_naive(row.get("datetime"))
                borough = _str_or_none(row, "borough")

                if dt_nyc is None or borough is None:
                    continue

                if dt_nyc not in dt_cache:
                    dt_cache[dt_nyc] = self._upsert_datetime(cur, dt_nyc)
                if borough not in loc_cache:
                    loc_cache[borough] = self._get_or_create_location(cur, borough)

                vis_raw = row.get("visibility")
                visibility = int(vis_raw) if vis_raw is not None and not pd.isna(vis_raw) else None
                precipitation = float(row.get("precipitation", 0) or 0)
                is_adverse = 1 if (precipitation > 5 or (visibility is not None and visibility < 1000)) else 0

                cur.execute(sql, (
                    dt_cache[dt_nyc],
                    loc_cache[borough],
                    float(row.get("temperature_2m", 0) or 0),
                    precipitation,
                    visibility,
                    float(row.get("rain", 0) or 0),
                    float(row.get("showers", 0) or 0),
                    float(row.get("snowfall", 0) or 0),
                    float(row.get("wind_speed_10m", 0) or 0),
                    _str_or_none(row, "weather_category"),
                    _str_or_none(row, "weather_severity"),
                    is_adverse,
                ))

                loaded += 1
                if loaded % batch_commit == 0:
                    self.conn.commit()
                    logger.info(f"  {loaded} weather rows committed...")

            return loaded

        return self._run_load(df, "weather", _insert)

    # ── Collisions load ───────────────────────────────────────────────────

    def load_collisions(self, df: pd.DataFrame, batch_commit: int = 500) -> bool:

        def _insert(cur, dt_cache, loc_cache):
            sql = """
                INSERT IGNORE INTO fact_collisions
                    (collision_id, datetime_id, location_id,
                     persons_injured, persons_killed,
                     pedestrians_injured, pedestrians_killed,
                     cyclists_injured, cyclists_killed,
                     motorists_injured, motorists_killed,
                     total_involved, has_injuries, has_fatalities, severity_level,
                     contributing_factor_vehicle_1, contributing_factor_vehicle_2,
                     contributing_factor_vehicle_3, contributing_factor_vehicle_4,
                     contributing_factor_vehicle_5,
                     vehicle_type_code1, vehicle_type_code2, vehicle_type_code_3,
                     vehicle_type_code_4, vehicle_type_code_5,
                     number_of_vehicles, raw_crash_date, raw_crash_time)
                VALUES
                    (%s,%s,%s,
                     %s,%s,%s,%s,%s,%s,%s,%s,
                     %s,%s,%s,%s,
                     %s,%s,%s,%s,%s,
                     %s,%s,%s,%s,%s,
                     %s,%s,%s)
            """
            loaded = 0
            for idx, row in df.iterrows():
                collision_id = _str_or_none(row, "collision_id")
                if not collision_id:
                    logger.debug(f"Row {idx} skipped — no collision_id")
                    continue

                dt_nyc  = _to_nyc_naive(row.get("crash_datetime"))
                borough = _str_or_none(row, "borough") or "UNKNOWN"

                if dt_nyc is None:
                    continue

                if dt_nyc not in dt_cache:
                    dt_cache[dt_nyc] = self._upsert_datetime(cur, dt_nyc)
                if borough not in loc_cache:
                    loc_cache[borough] = self._get_or_create_location(cur, borough)

                persons_injured      = _int(row, "persons_injured")
                persons_killed       = _int(row, "persons_killed")
                pedestrians_injured  = _int(row, "pedestrians_injured")
                pedestrians_killed   = _int(row, "pedestrians_killed")
                cyclists_injured     = _int(row, "cyclists_injured")
                cyclists_killed      = _int(row, "cyclists_killed")
                motorists_injured    = _int(row, "motorists_injured")
                motorists_killed     = _int(row, "motorists_killed")

                # total_involved pre-computed by transform; fall back just in case
                total_involved = _int(row, "total_involved") or (persons_injured + persons_killed)

                vt1 = _str_or_none(row, "vehicle_type_code1")
                vt2 = _str_or_none(row, "vehicle_type_code2")
                vt3 = _str_or_none(row, "vehicle_type_code_3")
                vt4 = _str_or_none(row, "vehicle_type_code_4")
                vt5 = _str_or_none(row, "vehicle_type_code_5")
                num_vehicles = sum(1 for v in (vt1, vt2, vt3, vt4, vt5) if v) or 0

                crash_date_str = _str_or_none(row, "crash_date")
                if crash_date_str:
                    crash_date_str = crash_date_str.split("T")[0]
                crash_time_str = _str_or_none(row, "crash_time") or "00:00:00"

                cur.execute(sql, (
                    collision_id,
                    dt_cache[dt_nyc],
                    loc_cache[borough],
                    persons_injured,
                    persons_killed,
                    pedestrians_injured,
                    pedestrians_killed,
                    cyclists_injured,
                    cyclists_killed,
                    motorists_injured,
                    motorists_killed,
                    total_involved,
                    1 if persons_injured > 0 else 0,
                    1 if persons_killed > 0 else 0,
                    _str_or_none(row, "severity_level") or "NONE",
                    _str_or_none(row, "contributing_factor_vehicle_1"),
                    _str_or_none(row, "contributing_factor_vehicle_2"),
                    _str_or_none(row, "contributing_factor_vehicle_3"),
                    _str_or_none(row, "contributing_factor_vehicle_4"),
                    _str_or_none(row, "contributing_factor_vehicle_5"),
                    vt1, vt2, vt3, vt4, vt5,
                    num_vehicles,
                    crash_date_str,
                    crash_time_str,
                ))

                loaded += 1
                if loaded % batch_commit == 0:
                    self.conn.commit()
                    logger.info(f"  {loaded} collision rows committed...")

            return loaded

        return self._run_load(df, "collisions", _insert)


# ---------------------------------------------------------------------------
# Pipeline entry point
# ---------------------------------------------------------------------------

def run_load(weather_df: pd.DataFrame, collisions_df: pd.DataFrame) -> bool:
    loader = DatabaseLoader()

    ok_weather    = loader.load_weather(weather_df)
    ok_collisions = loader.load_collisions(collisions_df)

    if ok_weather and ok_collisions:
        logger.info("Database load finished successfully")
        return True

    logger.warning("Database load finished with errors")
    return False