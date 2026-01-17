import json
import psycopg2
from pathlib import Path
from dotenv import load_dotenv
import os
import logging

load_dotenv()
logger = logging.getLogger(__name__)

class RawDataLoader:
    def __init__(self):
        self.conn = psycopg2.connect(
            host=os.getenv('DB_HOST', 'localhost'),
            database=os.getenv('DB_NAME', 'medical_warehouse'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASS')
        )
        self.cur = self.conn.cursor()
    
    def create_raw_schema(self):
        """Create raw schema and table"""
        self.cur.execute("""
            CREATE SCHEMA IF NOT EXISTS raw;
            
            DROP TABLE IF EXISTS raw.telegram_messages;
            
            CREATE TABLE raw.telegram_messages (
                message_id BIGINT,
                channel_name VARCHAR(255),
                message_date TIMESTAMP,
                message_text TEXT,
                has_media BOOLEAN,
                image_path TEXT,
                views INTEGER,
                forwards INTEGER,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        self.conn.commit()
        logger.info("Created raw schema and table")
    
    def load_json_files(self, data_dir='data/raw/telegram_messages'):
        """Load all JSON files from data lake"""
        json_files = list(Path(data_dir).rglob('*.json'))
        
        for json_file in json_files:
            with open(json_file, 'r', encoding='utf-8') as f:
                messages = json.load(f)
            
            for message in messages:
                self.cur.execute("""
                    INSERT INTO raw.telegram_messages 
                    (message_id, channel_name, message_date, message_text, 
                     has_media, image_path, views, forwards)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    message['message_id'],
                    message['channel_name'],
                    message['message_date'],
                    message['message_text'],
                    message['has_media'],
                    message['image_path'],
                    message['views'],
                    message['forwards']
                ))
            
            self.conn.commit()
            logger.info(f"Loaded {len(messages)} messages from {json_file}")
    
    def close(self):
        self.cur.close()
        self.conn.close()

if __name__ == '__main__':
    loader = RawDataLoader()
    loader.create_raw_schema()
    loader.load_json_files()
    loader.close()