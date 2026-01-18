import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import os
import logging

# --- 1. PRODUCTION LOGGING ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("YOLOLoader")

load_dotenv()

def load_yolo_results(csv_path='data/processed/yolo_detections.csv'):
    """Loads YOLO results from CSV to Postgres using an Upsert strategy."""
    
    if not os.path.exists(csv_path):
        logger.error(f"Source file not found: {csv_path}")
        return

    conn = None
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST', 'localhost'),
            database=os.getenv('DB_NAME', 'medical_warehouse'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASS')
        )
        cur = conn.cursor()
        
        # 2. SYSTEMATIC ERROR HANDLING: Constraint-based Schema
        cur.execute("""
            CREATE SCHEMA IF NOT EXISTS raw;
            CREATE TABLE IF NOT EXISTS raw.yolo_detections (
                message_id BIGINT,
                channel_name VARCHAR(255),
                image_path TEXT,
                detected_class VARCHAR(100),
                confidence_score FLOAT,
                image_category VARCHAR(50),
                all_detections TEXT,
                detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                -- Primary Key ensures no duplicate detections per image
                PRIMARY KEY (message_id, channel_name)
            );
        """)
        
        # 3. PERFORMANCE: Efficient DataFrame Loading
        df = pd.read_csv(csv_path)
        # Handle potential NaNs in the CSV to prevent SQL errors
        df = df.where(pd.notnull(df), None)
        
        # Convert DataFrame to list of tuples for batch processing
        data_values = [tuple(x) for x in df.values]
        
        upsert_query = """
            INSERT INTO raw.yolo_detections 
            (message_id, channel_name, image_path, detected_class, confidence_score, image_category, all_detections)
            VALUES %s
            ON CONFLICT (message_id, channel_name) DO UPDATE SET
                confidence_score = EXCLUDED.confidence_score,
                image_category = EXCLUDED.image_category,
                all_detections = EXCLUDED.all_detections,
                detected_at = CURRENT_TIMESTAMP;
        """
        
        execute_values(cur, upsert_query, data_values)
        conn.commit()
        logger.info(f"Successfully Upserted {len(df)} YOLO detections to raw.yolo_detections")

    except Exception as e:
        logger.error(f"Database Load Error: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cur.close()
            conn.close()
            logger.info("Database connection closed.")

if __name__ == '__main__':
    load_yolo_results()