import os
import logging
import psycopg2
import pandas as pd
from pathlib import Path
from ultralytics import YOLO
from dotenv import load_dotenv
from psycopg2.extras import execute_values

# --- 1. CONFIGURATION & LOGGING ---
load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("YOLODetector")

class MedicalImageDetector:
    def __init__(self, model_name='yolov8n.pt'):
        self.model = YOLO(model_name)
        self.db_url = os.getenv('DATABASE_URL') or (
            f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} "
            f"user={os.getenv('DB_USER')} password={os.getenv('DB_PASS')}"
        )

    def _get_already_processed(self):
        """Production Resilience: Query DB to see which message_ids are already done."""
        processed = set()
        try:
            with psycopg2.connect(self.db_url) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT DISTINCT message_id FROM raw.yolo_detections")
                    processed = {row[0] for row in cur.fetchall()}
        except Exception:
            logger.info("No existing detections found. Starting fresh.")
        return processed

    def categorize_image(self, class_names):
        """Business Logic: Categorize based on medical/commercial context."""
        # Medical objects in COCO: bottle (medicine), person (doctor/patient)
        has_person = 'person' in class_names
        has_product = any(item in class_names for item in ['bottle', 'cup', 'bowl'])

        if has_person and has_product: return 'promotional'
        if has_product: return 'product_display'
        if has_person: return 'lifestyle'
        return 'other'

    def process_and_load(self, image_dir='data/raw/images'):
        processed_ids = self._get_already_processed()
        image_paths = [p for p in Path(image_dir).rglob('*.jpg') if int(p.stem) not in processed_ids]
        
        if not image_paths:
            logger.info("No new images to process.")
            return

        results_to_load = []
        logger.info(f"Processing {len(image_paths)} new images...")

        for path in image_paths:
            try:
                results = self.model(str(path), verbose=False)
                detections = []
                for r in results:
                    for box in r.boxes:
                        detections.append({
                            'name': self.model.names[int(box.cls[0])],
                            'conf': float(box.conf[0])
                        })

                # Determine top category and confidence
                top_class = max(detections, key=lambda x: x['conf'])['name'] if detections else 'none'
                top_conf = max(detections, key=lambda x: x['conf'])['conf'] if detections else 0.0
                category = self.categorize_image([d['name'] for d in detections])

                results_to_load.append((
                    int(path.stem),  # message_id
                    path.parts[-2],  # channel_name
                    str(path),
                    top_class,
                    top_conf,
                    category
                ))
            except Exception as e:
                logger.error(f"Failed to process {path.name}: {e}")

        # 2. SYSTEMATIC LOADING: Batch insert into Postgres
        if results_to_load:
            self._load_to_db(results_to_load)

    def _load_to_db(self, data):
        query = """
            INSERT INTO raw.yolo_detections 
            (message_id, channel_name, image_path, detected_class, confidence_score, image_category)
            VALUES %s
        """
        with psycopg2.connect(self.db_url) as conn:
            with conn.cursor() as cur:
                # Ensure the table exists for Script 9 dbt source
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS raw.yolo_detections (
                        message_id BIGINT, channel_name TEXT, image_path TEXT,
                        detected_class TEXT, confidence_score FLOAT, image_category TEXT,
                        detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """)
                execute_values(cur, query, data)
                conn.commit()
        logger.info(f"Successfully loaded {len(data)} detections to database.")

if __name__ == '__main__':
    detector = MedicalImageDetector()
    detector.process_and_load()