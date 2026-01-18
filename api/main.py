from fastapi import FastAPI, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List
import api.schemas as schemas
from api.database import get_db

app = FastAPI(
    title="Medical Telegram Analytics API",
    description="API for analyzing Ethiopian medical business data from Telegram",
    version="1.0.0"
)

@app.get("/")
def read_root():
    return {"message": "Medical Telegram Analytics API", "version": "1.0.0"}

@app.get("/api/reports/top-products", response_model=List[schemas.TopProduct])
def get_top_products(
    limit: int = Query(10, ge=1, le=100),
    db: Session = Depends(get_db)
):
    """
    Get the most frequently mentioned terms/products across all channels.
    Uses simple word frequency analysis on message text.
    """
    query = text("""
                 WITH words AS (
    SELECT 
        -- 1. Improved regex: split by spaces AND punctuation/slashes, 
        -- then remove anything not a letter to prevent "tubeheparine" clusters.
        LOWER(regexp_replace(
            unnest(regexp_split_to_array(message_text, '[\s\/\-\,\.\(\)]+')), 
            '[^a-z]', '', 'g'
        )) AS word
    FROM raw_marts.fct_messages
    WHERE message_text IS NOT NULL AND message_text != ''
),
filtered_counts AS (
    SELECT 
        word,
        COUNT(*) AS count
    FROM words
    WHERE LENGTH(word) > 3  -- Shortened slightly to catch 'zinc' or 'gel'
      -- 2. EXCLUDE "False Positives" (Words that match suffixes but aren't pharma)
      AND word NOT IN (
          'birr', 'delivery', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday',
          'available', 'price', 'fixed', 'address', 'phone', 'location', 'station', 'around', 'front',
          'school', 'plaza', 'mall', 'bole', 'medhanialem', 'cmc', 'ayat', 'gerji', 'legetafo',
          -- NEW: Negative filter for common suffix matches
          'machine', 'furniture', 'urine', 'uterine', 'online', 'outline', 'deadline', 'headline', 
          'timeline', 'routine', 'magazine', 'gasoline', 'engine', 'alcohol', 'school', 'control',
          'please', 'message', 'thanks', 'contact', 'stock', 'store', 'inbox'
      )
      -- 3. ENHANCED INCLUDE patterns
      AND (
          word LIKE '%ine' OR   -- e.g., Caffeine, Insulin
          word LIKE '%ol' OR    -- e.g., Retinol, Paracetamol, Menthol
          word LIKE '%acid' OR  -- e.g., Hyaluronic
          word LIKE '%derm%' OR -- e.g., Bioderma
          word LIKE '%vit%' OR  -- e.g., Vitamin
          word LIKE '%cream%' OR
          word LIKE '%serum%' OR
          word LIKE '%gel' OR   -- Added for cosmetics
          word LIKE '%caine' OR -- Added for numbing/pharma (Lidocaine)
          word LIKE '%sone' OR  -- Added for steroids (Dexamethasone)
          word LIKE '%zole'     -- Added for antifungals (Ketoconazole)
      )
    GROUP BY word
)
SELECT 
    word AS product_term,
    count AS mention_count
FROM filtered_counts
WHERE word != '' -- Final safety check
ORDER BY count DESC
LIMIT :limit
                 """)
    
    result = db.execute(query, {"limit": limit})
    return [{"product_term": row[0], "mention_count": row[1]} for row in result]

@app.get("/api/channels/{channel_name}/activity", response_model=List[schemas.ChannelActivity])
def get_channel_activity(
    channel_name: str,
    db: Session = Depends(get_db)
):
    """
    Get daily posting activity and engagement metrics for a specific channel.
    """
    query = text("""
        SELECT 
            d.full_date AS date,
            COUNT(f.message_id) AS message_count,
            SUM(f.view_count) AS total_views
        FROM raw_marts.fct_messages f
        JOIN raw_marts.dim_channels c ON f.channel_key = c.channel_key
        JOIN raw_marts.dim_dates d ON f.date_key = d.date_key
        WHERE c.channel_name = :channel_name
        GROUP BY d.full_date
        ORDER BY d.full_date DESC
        LIMIT 30
    """)
    
    result = db.execute(query, {"channel_name": channel_name})
    return [
        {
            "date": row[0],
            "message_count": row[1],
            "total_views": row[2] or 0
        } 
        for row in result
    ]

@app.get("/api/search/messages", response_model=List[schemas.MessageSearch])
def search_messages(
    query: str = Query(..., min_length=1),
    limit: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db)
):
    """
    Search for messages containing a specific keyword.
    """
    sql_query = text("""
        SELECT 
            f.message_id,
            c.channel_name,
            d.full_date AS message_date,
            f.message_text,
            f.view_count
        FROM raw_marts.fct_messages f
        JOIN raw_marts.dim_channels c ON f.channel_key = c.channel_key
        JOIN raw_marts.dim_dates d ON f.date_key = d.date_key
        WHERE LOWER(f.message_text) LIKE LOWER(:search_term)
        ORDER BY f.view_count DESC
        LIMIT :limit
    """)
    
    result = db.execute(sql_query, {"search_term": f"%{query}%", "limit": limit})
    return [
        {
            "message_id": row[0],
            "channel_name": row[1],
            "message_date": row[2],
            "message_text": row[3],
            "views": row[4]
        }
        for row in result
    ]

@app.get("/api/reports/visual-content", response_model=List[schemas.VisualContentStats])
def get_visual_content_stats(db: Session = Depends(get_db)):
    """
    Get statistics about image usage and categories across channels.
    """
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
    
    result = db.execute(query)
    return [
        {
            "channel_name": row[0],
            "total_messages": row[1],
            "messages_with_images": row[2],
            "image_percentage": float(row[3]) if row[3] else 0.0,
            "promotional_count": row[4],
            "product_display_count": row[5]
        }
        for row in result
    ]

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)