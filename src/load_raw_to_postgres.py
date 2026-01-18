import json
import psycopg2
from psycopg2.extras import execute_values
from pathlib import Path
from dotenv import load_dotenv
import os
import logging

# --- 1. PRODUCTION LOGGING ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("RawDataLoader")

load_dotenv()

class RawDataLoader:
    def __init__(self):
        self.db_url = os.getenv('DATABASE_URL') or (
            f"host={os.getenv('DB_HOST', 'localhost')} "
            f"dbname={os.getenv('DB_NAME', 'medical_warehouse')} "
            f"user={os.getenv('DB_USER', 'postgres')} "
            f"password={os.getenv('DB_PASS')}"
        )

    def init_database(self):
        """Prepares the schema without destructive CASCADE drops."""
        with psycopg2.connect(self.db_url) as conn:
            with conn.cursor() as cur:
                cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")
                
                # Systematic Error Handling: Use Unique Constraint for Upsert logic
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS raw.telegram_messages (
                        message_id BIGINT,
                        channel_name VARCHAR(255),
                        message_date TIMESTAMP,
                        message_text TEXT,
                        has_media BOOLEAN,
                        image_path TEXT,
                        views INTEGER,
                        forwards INTEGER,
                        loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (message_id, channel_name)
                    );
                """)
                conn.commit()
        logger.info("Database schema initialized and ready for Upsert.")

    def load_json_files(self, data_dir='data/raw/telegram_messages'):
        """Loads JSON data using an Upsert strategy for production resilience."""
        json_files = list(Path(data_dir).rglob('*.json'))
        
        if not json_files:
            logger.warning(f"No JSON files found in {data_dir}")
            return

        with psycopg2.connect(self.db_url) as conn:
            with conn.cursor() as cur:
                for json_file in json_files:
                    try:
                        with open(json_file, 'r', encoding='utf-8') as f:
                            messages = json.load(f)
                        
                        if not messages:
                            continue

                        # Prepare data for batch insert
                        data_to_insert = [
                            (
                                m['message_id'], m['channel_name'], m['message_date'],
                                m['message_text'], m['has_media'], m['image_path'],
                                m['views'], m['forwards']
                            ) for m in messages
                        ]

                        # 2. SYSTEMATIC UPSERT: Update views/forwards if record exists
                        insert_query = """
                            INSERT INTO raw.telegram_messages 
                            (message_id, channel_name, message_date, message_text, 
                             has_media, image_path, views, forwards)
                            VALUES %s
                            ON CONFLICT (message_id, channel_name) DO UPDATE SET
                                views = EXCLUDED.views,
                                forwards = EXCLUDED.forwards,
                                loaded_at = CURRENT_TIMESTAMP;
                        """
                        
                        execute_values(cur, insert_query, data_to_insert)
                        logger.info(f"Loaded/Updated {len(messages)} messages from {json_file.name}")
                        
                    except Exception as e:
                        logger.error(f"Failed to load {json_file}: {e}")
                        conn.rollback() # Rollback current file, move to next
                
                conn.commit()

if __name__ == '__main__':
    loader = RawDataLoader()
    loader.init_database()
    loader.load_json_files()