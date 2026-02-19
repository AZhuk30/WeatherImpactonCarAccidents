"""
Data transformation and cleaning
Weather + NYC Collisions
"""

import logging
from datetime import datetime

import pandas as pd

from src.config import BOROUGHS, PROCESSED_DATA_DIR

logger = logging.getLogger(__name__)

INJURY_COLS = [
    "persons_injured", "persons_killed",
    "pedestrians_injured", "pedestrians_killed",
    "cyclists_injured", "cyclists_killed",
    "motorists_injured", "motorists_killed",
]

CONTRIBUTING_FACTOR_COLS = [
    "contributing_factor_vehicle_1",
    "contributing_factor_vehicle_2",
    "contributing_factor_vehicle_3",
    "contributing_factor_vehicle_4",
    "contributing_factor_vehicle_5",
]

FACTOR_EXCLUDE = {"", "nan", "none", "unspecified", "not specified", "unknown"}


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def make_tz_naive(s: pd.Series) -> pd.Series:
    """Convert a datetime Series to tz-naive regardless of input tz."""
    s = pd.to_datetime(s, errors="coerce")
    try:
        if hasattr(s.dt, "tz") and s.dt.tz is not None:
            s = s.dt.tz_convert(None)
    except Exception:
        pass
    return s


def top_contributing_factors(df: pd.DataFrame, n: int = 15) -> pd.DataFrame:
    """
    Aggregate all five contributing_factor columns into a ranked count table.
    Returns DataFrame with columns ['factor', 'count'].
    Filters out blank/NaN/Unspecified values.
    """
    present = [c for c in CONTRIBUTING_FACTOR_COLS if c in df.columns]
    if not present:
        return pd.DataFrame(columns=["factor", "count"])

    s = (
        df[present]
        .astype("string")
        .stack(dropna=True)
        .str.strip()
    )
    s = s[~s.str.lower().isin(FACTOR_EXCLUDE)]

    out = s.value_counts().head(n).reset_index()
    out.columns = ["factor", "count"]
    return out


# ---------------------------------------------------------------------------
# Weather transformer
# ---------------------------------------------------------------------------

class WeatherTransformer:

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info(f"Transforming {len(df)} weather records")
        df = df.copy()

        df.columns = df.columns.str.lower().str.strip().str.replace(" ", "_")

        # Datetime — convert to NYC local then strip tz so comparisons are simple
        df["datetime"] = make_tz_naive(
            pd.to_datetime(df["datetime"], utc=True, errors="coerce")
            .dt.tz_convert("America/New_York")
        )

        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")

        # Numerics
        numeric_cols = [
            "temperature_2m", "precipitation", "visibility",
            "rain", "showers", "snowfall", "wind_speed_10m",
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        if "temperature_2m" in df.columns:
            df["temperature_2m"] = df["temperature_2m"].round(2)
        if "wind_speed_10m" in df.columns:
            df["wind_speed_10m"] = df["wind_speed_10m"].round(2)
        if "visibility" in df.columns:
            df["visibility"] = (df["visibility"] / 100).round() * 100

        # Time features
        df["hour_nyc"]     = df["datetime"].dt.hour
        df["day_of_week"]  = df["datetime"].dt.day_name()
        df["is_weekend"]   = df["datetime"].dt.dayofweek >= 5
        df["is_rush_hour"] = df["hour_nyc"].isin([7, 8, 9, 16, 17, 18, 19])
        df["is_night"]     = (df["hour_nyc"] >= 20) | (df["hour_nyc"] < 6)
        df["month"]        = df["datetime"].dt.month
        df["season"]       = df["month"].map(self._season)

        # Weather labels
        df["weather_category"] = df.apply(self._categorize, axis=1)
        df["weather_severity"]  = df.apply(self._severity, axis=1)

        if "borough" in df.columns:
            df["borough"] = df["borough"].str.upper().str.strip()

        df = df.drop_duplicates(subset=["borough", "datetime"])
        logger.info(f"Weather transformation complete: {len(df)} records")
        return df

    @staticmethod
    def _season(month: int) -> str:
        if month in (12, 1, 2): return "WINTER"
        if month in (3, 4, 5):  return "SPRING"
        if month in (6, 7, 8):  return "SUMMER"
        return "FALL"

    @staticmethod
    def _categorize(row) -> str:
        if row.get("snowfall", 0) > 0:
            return "SNOW"
        if row.get("rain", 0) + row.get("showers", 0) + row.get("precipitation", 0) > 0:
            return "RAIN"
        if row.get("visibility", 10000) < 5000:
            return "FOG"
        if row.get("wind_speed_10m", 0) > 30:
            return "WIND"
        return "CLEAR"

    @staticmethod
    def _severity(row) -> str:
        if row.get("snowfall", 0) > 5:
            return "HEAVY"
        rain = row.get("rain", 0) + row.get("showers", 0) + row.get("precipitation", 0)
        if rain > 10:                            return "HEAVY"
        if rain > 5:                             return "MODERATE"
        if row.get("visibility", 10000) < 1000: return "SEVERE"
        if row.get("visibility", 10000) < 3000: return "MODERATE"
        if row.get("wind_speed_10m", 0) > 50:   return "SEVERE"
        if row.get("wind_speed_10m", 0) > 30:   return "MODERATE"
        return "LIGHT"


# ---------------------------------------------------------------------------
# Collisions transformer
# ---------------------------------------------------------------------------

class CollisionsTransformer:

    # API column aliases → canonical names used everywhere downstream
    _RENAME = {
        "number_of_persons_injured":     "persons_injured",
        "number_of_persons_killed":      "persons_killed",
        "number_of_pedestrians_injured": "pedestrians_injured",
        "number_of_pedestrians_killed":  "pedestrians_killed",
        "number_of_cyclist_injured":     "cyclists_injured",
        "number_of_cyclist_killed":      "cyclists_killed",
        "number_of_motorist_injured":    "motorists_injured",
        "number_of_motorist_killed":     "motorists_killed",
    }

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info(f"Transforming {len(df)} collision records")
        df = df.copy()

        df.columns = df.columns.str.lower().str.strip().str.replace(" ", "_")
        df = df.rename(columns={k: v for k, v in self._RENAME.items() if k in df.columns})

        # Datetime
        df["crash_datetime"] = self._parse_crash_datetime(df)

        # Borough — normalise to uppercase, drop rows with unrecognised values
        if "borough" in df.columns:
            df["borough"] = df["borough"].fillna("UNKNOWN").str.upper().str.strip()
            valid = set(BOROUGHS.keys()) | {"UNKNOWN"}
            df = df[df["borough"].isin(valid)]

        # Ensure all injury columns exist and are int
        for col in INJURY_COLS:
            if col not in df.columns:
                df[col] = 0
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

        for col in ("latitude", "longitude"):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").round(9)

        df = df.dropna(subset=["collision_id", "crash_datetime"])
        df = df.drop_duplicates(subset=["collision_id"])

        # Derived columns
        df["has_injuries"]   = df["persons_injured"] > 0
        df["has_fatalities"] = df["persons_killed"] > 0

        # persons_injured/killed are already totals across all road user types
        # so use only those to avoid double-counting
        df["total_involved"] = df["persons_injured"] + df["persons_killed"]

        df["severity_level"] = df.apply(self._severity, axis=1)

        logger.info(f"Collisions transformation complete: {len(df)} records")
        return df

    @staticmethod
    def _parse_crash_datetime(df: pd.DataFrame) -> pd.Series:
        results = []
        for _, row in df.iterrows():
            try:
                date_str = str(row.get("crash_date", "")).split("T")[0]
                time_str = str(row.get("crash_time", "00:00")).strip()
                results.append(pd.to_datetime(f"{date_str} {time_str}", errors="coerce"))
            except Exception:
                results.append(pd.NaT)
        return pd.Series(results, index=df.index)

    @staticmethod
    def _severity(row) -> str:
        if row["persons_killed"] > 0:   return "FATAL"
        if row["persons_injured"] >= 3: return "SEVERE"
        if row["persons_injured"] > 0:  return "MODERATE"
        if row["total_involved"] > 0:   return "MINOR"
        return "NONE"


# ---------------------------------------------------------------------------
# Pipeline entry point
# ---------------------------------------------------------------------------

def run_transformation(
    weather_df: pd.DataFrame,
    collisions_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:

    logger.info("STARTING DATA TRANSFORMATION")

    weather_clean    = WeatherTransformer().transform(weather_df)
    collisions_clean = CollisionsTransformer().transform(collisions_df)

    PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    weather_clean.to_csv(
        PROCESSED_DATA_DIR / f"weather_processed_{timestamp}.csv", index=False
    )
    collisions_clean.to_csv(
        PROCESSED_DATA_DIR / f"collisions_processed_{timestamp}.csv", index=False
    )

    logger.info("TRANSFORMATION COMPLETE")
    return weather_clean, collisions_clean
