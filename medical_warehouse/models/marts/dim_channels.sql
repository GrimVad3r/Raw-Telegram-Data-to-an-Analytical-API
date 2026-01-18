{{ config(materialized='table') }}

-- 1. CLEANING LAYER: Filter out invalid data before aggregating
WITH valid_messages AS (
    SELECT *
    FROM {{ ref('stg_telegram_messages') }}
    WHERE channel_name IS NOT NULL -- Systematic error handling for scraper failures
),

-- 2. TRANSFORMATION LAYER: Calculate channel-specific metrics
channel_stats AS (
    SELECT
        channel_name,
        MIN(message_date) AS first_post_date,
        MAX(message_date) AS last_post_date,
        COUNT(message_id) AS total_posts,
        COALESCE(AVG(views), 0) AS avg_views, -- Resilience against null view counts
        
        -- Improved Classification Logic
        CASE 
            WHEN LOWER(channel_name) LIKE '%pharma%' THEN 'Pharmaceutical'
            WHEN LOWER(channel_name) LIKE '%cosmetic%' THEN 'Cosmetics'
            WHEN LOWER(channel_name) LIKE '%beauty%' THEN 'Cosmetics'
            ELSE 'Medical General'
        END AS channel_type
    FROM valid_messages
    GROUP BY 1
)

-- 3. FINAL LAYER: Assign surrogate keys and format outputs
SELECT
    -- Surrogate key generation is best practice for dim tables
    {{ dbt_utils.generate_surrogate_key(['channel_name']) }} AS channel_key,
    channel_name,
    channel_type,
    first_post_date,
    last_post_date,
    total_posts,
    ROUND(avg_views::NUMERIC, 2) AS avg_views,
    CURRENT_TIMESTAMP AS dbt_updated_at -- Audit column for production tracking
FROM channel_stats