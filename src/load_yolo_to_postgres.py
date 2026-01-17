# src/load_yolo_to_postgres.py
import pandas as pd
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

def load_yolo_results():
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        database=os.getenv('DB_NAME', 'medical_warehouse'),
        user=os.getenv('DB_USER', 'postgres'),
        password=os.getenv('DB_PASSWORD')
    )
    cur = conn.cursor()
    
    # Create table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw.yolo_detections (
            message_id BIGINT,
            channel_name VARCHAR(255),
            image_path TEXT,
            detected_class VARCHAR(100),
            confidence_score FLOAT,
            image_category VARCHAR(50),
            all_detections TEXT
        );
    """)
    
    # Load CSV
    df = pd.read_csv('data/processed/yolo_detections.csv')
    
    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO raw.yolo_detections 
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, tuple(row))
    
    conn.commit()
    cur.close()
    conn.close()

if __name__ == '__main__':
    load_yolo_results()