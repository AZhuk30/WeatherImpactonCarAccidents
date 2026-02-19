"""
NYC Traffic Safety ETL Pipeline

Modes:
  local   - Extract → Transform → Load to SQL + export CSVs
  actions - Extract → Transform → export CSVs only (no DB)
"""

import argparse
import logging
import sys
import traceback
from datetime import datetime, timedelta

from src.config import LOGS_DIR, PROCESSED_DATA_DIR, SKIP_DATABASE, PIPELINE_CONFIG

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def setup_logging() -> logging.Logger:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(LOGS_DIR / "pipeline.log", encoding="utf-8"),
        ],
    )
    return logging.getLogger(__name__)


logger = setup_logging()

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def resolve_date_range(days: int) -> tuple[str, str]:
    end = datetime.now()
    start = end - timedelta(days=days)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def export_master_csvs(weather_df, collisions_df) -> None:
    PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)

    weather_path = PROCESSED_DATA_DIR / "weather_master.csv"
    collisions_path = PROCESSED_DATA_DIR / "collisions_master.csv"

    weather_df.to_csv(weather_path, index=False)
    collisions_df.to_csv(collisions_path, index=False)

    logger.info(f"Exported weather CSV:    {weather_path} ({len(weather_df):,} rows)")
    logger.info(f"Exported collisions CSV: {collisions_path} ({len(collisions_df):,} rows)")

    # Sanity-check files exist on disk
    if not (weather_path.exists() and collisions_path.exists()):
        raise RuntimeError("CSV export failed — files not found after write")


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def run_pipeline(mode: str, days: int) -> bool:
    start_date, end_date = resolve_date_range(days)

    logger.info("=" * 60)
    logger.info("STARTING NYC TRAFFIC SAFETY ETL PIPELINE")
    logger.info(f"Mode: {mode.upper()} | {start_date} → {end_date} ({days} days)")
    logger.info("=" * 60)

    try:
        # STEP 1: Extract
        logger.info("STEP 1: EXTRACTING")
        from src.extract import run_extraction
        weather_df, collisions_df = run_extraction(start_date, end_date)
        logger.info(f"  Weather:    {len(weather_df):,} records")
        logger.info(f"  Collisions: {len(collisions_df):,} records")

        if weather_df.empty or collisions_df.empty:
            logger.error("Extraction returned empty data — aborting.")
            return False

        # STEP 2: Transform
        logger.info("STEP 2: TRANSFORMING")
        from src.transform import run_transformation
        weather_clean, collisions_clean = run_transformation(weather_df, collisions_df)
        logger.info(f"  Weather:    {len(weather_clean):,} records")
        logger.info(f"  Collisions: {len(collisions_clean):,} records")

        # STEP 3: Load
        logger.info(f"STEP 3: LOADING ({mode} mode)")
        if mode == "local":
            try:
                from src.load import run_load
                run_load(weather_clean, collisions_clean)
                logger.info("  SQL load complete")
            except Exception as e:
                logger.error(f"  SQL load failed: {e} — falling back to CSV-only")

        export_master_csvs(weather_clean, collisions_clean)

        logger.info("=" * 60)
        logger.info("PIPELINE COMPLETE — run: streamlit run app.py")
        logger.info("=" * 60)
        return True

    except Exception:
        logger.error(f"PIPELINE FAILED:\n{traceback.format_exc()}")
        return False


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="NYC Traffic Safety ETL Pipeline")

    parser.add_argument(
        "--mode",
        choices=["local", "actions"],
        default="local",
        help="local = SQL + CSV; actions = CSV only",
    )

    group = parser.add_mutually_exclusive_group()
    group.add_argument("--days", type=int, default=None, help="Explicit number of days to fetch")
    group.add_argument("--recent", action="store_true", help="Fetch last 30 days")
    group.add_argument("--test", action="store_true", help="Fetch last 2 days (quick test)")
    group.add_argument("--historical", action="store_true", help="Fetch from DEFAULT_START_DATE to today")

    return parser.parse_args()


def resolve_days(args: argparse.Namespace) -> int:
    if args.test:
        return 2
    if args.recent:
        return 30
    if args.historical:
        default_start = PIPELINE_CONFIG.get("default_start_date", "2024-01-01")
        try:
            return max(1, (datetime.now() - datetime.strptime(default_start, "%Y-%m-%d")).days)
        except ValueError:
            logger.warning(f"Invalid default_start_date '{default_start}', defaulting to 365 days")
            return 365
    return args.days if args.days is not None else 30


if __name__ == "__main__":
    args = parse_args()
    days = resolve_days(args)
    
    mode = "actions" if SKIP_DATABASE else args.mode

    success = run_pipeline(mode=mode, days=days)
    sys.exit(0 if success else 1)

