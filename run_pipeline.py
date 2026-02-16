"""
Enhanced ETL Pipeline - NYC Traffic Safety Analysis
Supports BOTH modes:
1. Local mode: Full SQL database pipeline
2. GitHub Actions mode: CSV-only for dashboard deployment
"""

import logging
import sys
import argparse
from datetime import datetime, timedelta
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('data/logs/pipeline.log')
    ]
)

logger = logging.getLogger(__name__)


def run_pipeline(mode='local', days=7):
    """
    Run the ETL pipeline in specified mode
    
    Args:
        mode: 'local' for SQL database, 'actions' for GitHub Actions CSV-only
        days: Number of days to fetch
    """
    
    logger.info("="*60)
    logger.info(f"STARTING NYC TRAFFIC SAFETY ETL PIPELINE")
    logger.info(f"Mode: {mode.upper()}")
    logger.info(f"Days to fetch: {days}")
    logger.info("="*60)
    
    # Calculate date range
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    
    try:
        # STEP 1: EXTRACT
        logger.info("\n📡 STEP 1: EXTRACTING DATA")
        logger.info("-" * 60)
        
        from src.extract import run_extraction
        weather_df, collisions_df = run_extraction(start_date, end_date)
        
        logger.info(f"✅ Extracted {len(weather_df)} weather records")
        logger.info(f"✅ Extracted {len(collisions_df)} collision records")
        
        if weather_df.empty or collisions_df.empty:
            logger.error("❌ No data extracted. Aborting pipeline.")
            return False
        
        # STEP 2: TRANSFORM
        logger.info("\n🔄 STEP 2: TRANSFORMING DATA")
        logger.info("-" * 60)
        
        from src.transform import run_transformation
        weather_clean, collisions_clean = run_transformation(weather_df, collisions_df)
        
        logger.info(f"✅ Transformed {len(weather_clean)} weather records")
        logger.info(f"✅ Transformed {len(collisions_clean)} collision records")
        
        # STEP 3: LOAD (mode-dependent)
        logger.info(f"\n💾 STEP 3: LOADING DATA ({mode} mode)")
        logger.info("-" * 60)
        
        if mode == 'local':
            # LOCAL MODE: Load to SQL database
            try:
                from src.load import run_load
                run_load(weather_clean, collisions_clean)
                logger.info("✅ Data loaded to SQL database")
            except Exception as e:
                logger.error(f"⚠️ SQL load failed: {e}")
                logger.info("💡 Falling back to CSV export for dashboard")
                mode = 'actions'  # Fallback to CSV mode
        
        if mode == 'actions' or mode == 'local':
            # ALWAYS create CSV exports for Streamlit dashboard
            # (GitHub Actions needs these, local mode can use them as backup)
            
            import os
            os.makedirs('data/processed', exist_ok=True)
            
            # Save master files for dashboard
            weather_clean.to_csv('data/processed/weather_master.csv', index=False)
            collisions_clean.to_csv('data/processed/collisions_master.csv', index=False)
            
            logger.info("✅ CSV master files created for dashboard")
            logger.info(f"   📁 weather_master.csv: {len(weather_clean):,} records")
            logger.info(f"   📁 collisions_master.csv: {len(collisions_clean):,} records")
        
        # STEP 4: VERIFY OUTPUT
        logger.info("\n🔍 STEP 4: VERIFYING OUTPUT")
        logger.info("-" * 60)
        
        # Check CSV files exist and have data
        from pathlib import Path
        weather_file = Path('data/processed/weather_master.csv')
        collision_file = Path('data/processed/collisions_master.csv')
        
        if weather_file.exists() and collision_file.exists():
            import pandas as pd
            w_df = pd.read_csv(weather_file)
            c_df = pd.read_csv(collision_file)
            
            logger.info(f"✅ Weather CSV verified: {len(w_df):,} rows")
            logger.info(f"✅ Collision CSV verified: {len(c_df):,} rows")
            
            # Show data summary
            if 'date' in c_df.columns:
                logger.info(f"📅 Collision date range: {c_df['date'].min()} to {c_df['date'].max()}")
            if 'borough' in c_df.columns:
                logger.info(f"🏙️ Boroughs in data: {c_df['borough'].nunique()}")
        else:
            logger.error("❌ CSV files not created properly")
            return False
        
        # SUCCESS
        logger.info("\n" + "="*60)
        logger.info("🎉 PIPELINE COMPLETED SUCCESSFULLY!")
        logger.info("="*60)
        logger.info(f"\n📊 Ready to run dashboard:")
        logger.info(f"   streamlit run app.py")
        
        return True
        
    except Exception as e:
        logger.error(f"\n❌ PIPELINE FAILED: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='NYC Traffic Safety ETL Pipeline')
    parser.add_argument('--mode', 
                       choices=['local', 'actions'], 
                       default='local',
                       help='Pipeline mode: local (SQL+CSV) or actions (CSV only)')
    parser.add_argument('--days', 
                       type=int, 
                       default=30,
                       help='Number of days to fetch (default: 30)')
    
    args = parser.parse_args()
    
    success = run_pipeline(mode=args.mode, days=args.days)
    sys.exit(0 if success else 1)
