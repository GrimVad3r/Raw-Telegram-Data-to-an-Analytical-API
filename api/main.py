import logging
import uvicorn
from fastapi import FastAPI, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from typing import List, Dict, Any

import api.schemas as schemas
from api.database import get_db

# --- 1. PRODUCTION LOGGING CONFIGURATION ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("medical_api")

app = FastAPI(
    title="Medical Telegram Analytics API",
    description="API for analyzing Ethiopian medical business data from Telegram",
    version="1.0.0"
)

# --- 2. SYSTEMATIC ERROR HANDLING (Global) ---
@app.exception_handler(SQLAlchemyError)
async def database_exception_handler(request: Request, exc: SQLAlchemyError):
    """Catches all database-related errors for production resilience."""
    logger.critical(f"Database Connectivity Issue: {str(exc)}")
    return JSONResponse(
        status_code=503,
        content={"detail": "Database service temporarily unavailable. Please try again later."}
    )

# --- 3. UTILITY ENDPOINTS ---
@app.get("/health")
def health_check():
    """Endpoint for orchestration tools to monitor API status."""
    return {"status": "healthy", "service": "medical-telegram-analytics"}

@app.get("/")
def read_root():
    return {"message": "Medical Telegram Analytics API", "version": "1.0.0"}

# --- 4. ANALYTICS ENDPOINTS ---

@app.get("/api/reports/top-products", response_model=List[schemas.TopProduct])
def get_top_products(
    limit: int = Query(10, ge=1, le=100),
    db: Session = Depends(get_db)
):
    """Get the most frequently mentioned medical terms using NLP-lite SQL."""
    query = text("""
        WITH words AS (
            SELECT LOWER(regexp_replace(
                unnest(regexp_split_to_array(message_text, '[\s\/\-\,\.\(\)]+')), 
                '[^a-z]', '', 'g'
            )) AS word
            FROM raw_marts.fct_messages
            WHERE message_text IS NOT NULL AND message_text != ''
        ),
        filtered_counts AS (
            SELECT word, COUNT(*) AS count
            FROM words
            WHERE LENGTH(word) > 3
              AND word NOT IN (
                  'birr', 'delivery', 'available', 'price', 'fixed', 'address', 'phone', 
                  'location', 'station', 'around', 'front', 'school', 'plaza', 'mall',
                  'please', 'message', 'thanks', 'contact', 'stock', 'store', 'inbox'
              )
              AND (
                  word LIKE '%ine' OR word LIKE '%ol' OR word LIKE '%acid' OR 
                  word LIKE '%derm%' OR word LIKE '%vit%' OR word LIKE '%gel' OR
                  word LIKE '%caine' OR word LIKE '%sone' OR word LIKE '%zole'
              )
            GROUP BY word
        )
        SELECT word AS product_term, count AS mention_count
        FROM filtered_counts
        WHERE word != ''
        ORDER BY count DESC
        LIMIT :limit
    """)
    
    result = db.execute(query, {"limit": limit}).mappings().all()
    return result

@app.get("/api/channels/{channel_name}/activity", response_model=List[schemas.ChannelActivity])
def get_channel_activity(channel_name: str, db: Session = Depends(get_db)):
    """Fetch 30-day activity trend for a specific channel."""
    query = text("""
        SELECT 
            d.full_date AS date,
            COUNT(f.message_id) AS message_count,
            COALESCE(SUM(f.view_count), 0) AS total_views
        FROM raw_marts.fct_messages f
        JOIN raw_marts.dim_channels c ON f.channel_key = c.channel_key
        JOIN raw_marts.dim_dates d ON f.date_key = d.date_key
        WHERE c.channel_name = :channel_name
        GROUP BY d.full_date
        ORDER BY d.full_date DESC
        LIMIT 30
    """)
    
    result = db.execute(query, {"channel_name": channel_name}).mappings().all()
    
    if not result:
        logger.info(f"Search for non-existent channel activity: {channel_name}")
        raise HTTPException(status_code=404, detail="Channel not found or no data available")
        
    return result

@app.get("/api/search/messages", response_model=List[schemas.MessageSearch])
def search_messages(
    query: str = Query(..., min_length=1),
    limit: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db)
):
    """Search messages by keyword with view-count ranking."""
    sql_query = text("""
        SELECT 
            f.message_id,
            c.channel_name,
            d.full_date AS message_date,
            f.message_text,
            f.view_count AS views
        FROM raw_marts.fct_messages f
        JOIN raw_marts.dim_channels c ON f.channel_key = c.channel_key
        JOIN raw_marts.dim_dates d ON f.date_key = d.date_key
        WHERE LOWER(f.message_text) LIKE LOWER(:search_term)
        ORDER BY f.view_count DESC
        LIMIT :limit
    """)
    
    result = db.execute(sql_query, {
        "search_term": f"%{query}%", 
        "limit": limit
    }).mappings().all()
    
    return result

@app.get("/api/reports/visual-content", response_model=List[schemas.VisualContentStats])
def get_visual_content_stats(db: Session = Depends(get_db)):
    """Aggregate YOLO detection results to show visual marketing trends."""
    query = text("""
        WITH channel_stats AS (
            SELECT 
                c.channel_name,
                COUNT(f.message_id) AS total_messages,
                COUNT(CASE WHEN f.has_image THEN 1 END) AS messages_with_images,
                COUNT(CASE WHEN i.image_category = 'promotional' THEN 1 END) AS promotional_count,
                COUNT(CASE WHEN i.image_category = 'product_display' THEN 1 END) AS product_display_count
            FROM raw_marts.fct_messages f
            JOIN raw_marts.dim_channels c ON f.channel_key = c.channel_key
            LEFT JOIN raw_marts.fct_image_detections i ON f.message_id = i.message_id
            GROUP BY c.channel_name
        )
        SELECT 
            channel_name,
            total_messages,
            messages_with_images,
            ROUND(100.0 * messages_with_images / NULLIF(total_messages, 0), 2) AS image_percentage,
            promotional_count,
            product_display_count
        FROM channel_stats
        ORDER BY image_percentage DESC
    """)
    
    rows = db.execute(query).mappings().all()
    
    # Ensuring type-safety for percentages during serialization
    return [dict(row, image_percentage=float(row['image_percentage'] or 0.0)) for row in rows]

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")