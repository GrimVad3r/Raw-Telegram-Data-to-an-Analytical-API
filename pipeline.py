"""
Dagster Pipeline for Medical Telegram Data Warehouse
Orchestrates the complete ELT pipeline: Scrape -> Load -> Transform (dbt) -> Enrich (YOLO)
"""

import asyncio
import logging
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from dagster import (
    Config,
    Definitions,
    In,
    OpExecutionContext,
    Out,
    RunConfig,
    ScheduleDefinition,
    file_relative_path,
    job,
    op,
)
from dotenv import load_dotenv
from pydantic import Field

# 1. Environment and Path Setup
load_dotenv()
# Add src to sys.path to ensure modules are discoverable
SRC_PATH = Path(__file__).parent / "src"
sys.path.insert(0, str(SRC_PATH))

# Create an __init__.py if it doesn't exist to treat src as a package
if SRC_PATH.exists() and not (SRC_PATH / "__init__.py").exists():
    (SRC_PATH / "__init__.py").touch()

# Import custom modules
try:
    from scrapper import TelegramScraper
    from load_raw_to_postgres import RawDataLoader
    from yolo_detect import ImageDetector
    from load_yolo_to_postgres import load_yolo_results
except ImportError as e:
    print(f"Import Error: Ensure your files are in the 'src' directory. {e}")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 2. Configuration Schema
class PipelineConfig(Config):
    """Configuration for the pipeline using Pydantic Field"""
    channels: list = Field(
        default_factory=lambda: [
            'https://t.me/CheMed123',
            'https://t.me/lobelia4cosmetics',
            'https://t.me/tikvahpharma'
        ],
        description="List of Telegram channels to scrape"
    )
    message_limit: int = Field(
        default=1000,
        description="Maximum number of messages to scrape per channel"
    )

# 3. Operations (Ops)
@op(description="Scrape messages and images from Telegram channels")
def scrape_telegram_data(context: OpExecutionContext, config: PipelineConfig) -> str:
    context.log.info("TASK 1: Starting Telegram Data Scraping")
    
    try:
        async def run_scraper():
            # Standardize paths relative to the execution directory
            Path('logs').mkdir(exist_ok=True)
            Path('data/raw/telegram_messages').mkdir(parents=True, exist_ok=True)
            Path('data/raw/images').mkdir(parents=True, exist_ok=True)
            
            scraper = TelegramScraper()
            await scraper.run(config.channels)
        
        asyncio.run(run_scraper())
        context.log.info(f"✓ Scraped {len(config.channels)} channels successfully")
        return "scraping_complete"
    except Exception as e:
        context.log.error(f"✗ Scraping failed: {str(e)}")
        raise

@op(ins={"scraping_result": In(str)})
def load_raw_to_postgres(context: OpExecutionContext, scraping_result: str) -> str:
    context.log.info("TASK 2a: Loading Raw Data to PostgreSQL")
    try:
        loader = RawDataLoader()
        loader.create_raw_schema() # Ensure your SQL here uses 'CASCADE' or 'TRUNCATE'
        loader.load_json_files()
        loader.close()
        context.log.info("✓ Raw data loaded successfully")
        return "loading_complete"
    except Exception as e:
        context.log.error(f"✗ Data loading failed: {str(e)}")
        raise

@op(ins={"loading_result": In(str)})
def run_dbt_transformations(context: OpExecutionContext, loading_result: str) -> str:
    context.log.info("TASK 2b: Running dbt Transformations")
    try:
        # Standardize path handling
        dbt_dir = Path(file_relative_path(__file__, "medical_warehouse"))
        profiles_dir = dbt_dir # Assuming profiles.yml is inside the dbt project folder
        
        if not dbt_dir.exists():
            raise FileNotFoundError(f"dbt project directory not found at: {dbt_dir}")

        # Helper to run dbt commands with proper logging
        def run_dbt_cmd(args):
            full_cmd = ["dbt"] + args + ["--project-dir", str(dbt_dir), "--profiles-dir", str(profiles_dir)]
            res = subprocess.run(full_cmd, capture_output=True, text=True, check=True)
            if res.stdout: context.log.info(res.stdout)
            return res

        context.log.info("Running dbt deps...")
        run_dbt_cmd(["deps"])
        
        context.log.info("Running dbt run...")
        run_dbt_cmd(["run"])
        
        context.log.info("Running dbt test...")
        run_dbt_cmd(["test"])
        
        return "dbt_complete"
    except subprocess.CalledProcessError as e:
        context.log.error(f"✗ dbt failed! Output: {e.stdout}\nError: {e.stderr}")
        raise
    except Exception as e:
        context.log.error(f"✗ Unexpected dbt error: {str(e)}")
        raise

@op(ins={"dbt_result": In(str)})
def run_yolo_enrichment(context: OpExecutionContext, dbt_result: str) -> str:
    context.log.info("TASK 3: Running YOLO Object Detection")
    try:
        BASE_DIR = Path(__file__).resolve().parent
        image_dir = BASE_DIR / 'data/raw/images'
        images = list(image_dir.rglob('*.jpg'))
        
        if not images:
            context.log.warning("⚠ No images found; skipping YOLO enrichment")
            return "yolo_skipped"
        
        detector = ImageDetector()
        results_df = detector.process_images()
        load_yolo_results()
        
        # Trigger the specific mart for image detections
        context.log.info("Creating fct_image_detections mart...")
        dbt_dir = Path(file_relative_path(__file__, "medical_warehouse"))
        profiles_dir = dbt_dir
        subprocess.run(
            [
                "dbt", "run", 
                "--project-dir", str(dbt_dir), 
                "--profiles-dir", str(profiles_dir), # <--- ADDED THIS
                "--select", "fct_image_detections"
            ],
            check=True, 
            capture_output=True, 
            text=True
        )
        
        context.log.info(f"✓ Processed {len(results_df)} images with YOLO")
        return "yolo_complete"
    
    except subprocess.CalledProcessError as e:
        # Crucial for debugging: This prints WHY dbt failed (e.g., "Table not found")
        context.log.error(f"✗ dbt select failed: {e.stderr}")
        raise
    except Exception as e:
        context.log.error(f"✗ YOLO enrichment failed: {str(e)}")
        raise

@op(ins={"yolo_result": In(str)})
def verify_pipeline(context: OpExecutionContext, yolo_result: str) -> str:
    context.log.info("Final Pipeline Verification")
    try:
        import psycopg2
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST', 'localhost'),
            database=os.getenv('DB_NAME', 'medical_warehouse'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASS')
        )
        cur = conn.cursor()

        tables = [('raw.telegram_messages', 'Raw'), ('raw_marts.fct_messages', 'Facts')]
        for table, label in tables:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            count = cur.fetchone()[0]
            context.log.info(f"  {label} table row count: {count}")
            
        cur.close()
        conn.close()
        return "pipeline_complete"
    except Exception as e:
        context.log.error(f"✗ Verification failed: {str(e)}")
        raise

# 4. Job and Schedule Definitions
@job(
    description="Medical Telegram ELT Pipeline",
    config=RunConfig(
        ops={"scrape_telegram_data": PipelineConfig()}
    )
)
def medical_telegram_pipeline():
    scraping = scrape_telegram_data()
    loading = load_raw_to_postgres(scraping)
    dbt = run_dbt_transformations(loading)
    yolo = run_yolo_enrichment(dbt)
    verify_pipeline(yolo)

daily_schedule = ScheduleDefinition(
    job=medical_telegram_pipeline,
    cron_schedule="0 2 * * *",
    name="daily_medical_telegram_sync"
)

defs = Definitions(
    jobs=[medical_telegram_pipeline],
    schedules=[daily_schedule]
)

if __name__ == "__main__":
    print("Run with: dagster dev -f pipeline.py")