{{ config(
    materialized='incremental',
    unique_key='message_id',
    on_schema_change='fail'
) }}

-- 1. SOURCE LAYER: Clean raw inputs and handle incrementality
WITH messages AS (
    SELECT * FROM {{ ref('stg_telegram_messages') }}
    
    {% if is_incremental() %}
    -- Systematic Resilience: Only process messages newer than our last load
    WHERE message_date > (SELECT MAX(message_date) FROM {{ this }})
    {% endif %}
),

-- 2. DIMENSION LOOKUPS
channels AS (
    SELECT channel_key, channel_name FROM {{ ref('dim_channels') }}
),

dates AS (
    SELECT date_key, full_date FROM {{ ref('dim_dates') }}
)

-- 3. FINAL JOIN: Implementing "Defensive" SQL for production stability
SELECT
    m.message_id,
    
    -- Systematic Error Handling: 
    -- If a channel join fails, assign -1 instead of NULL to prevent broken API filters
    COALESCE(c.channel_key, -1) AS channel_key,
    COALESCE(d.date_key, -1) AS date_key,
    
    m.message_text,
    m.message_length,
    
    -- Resilience: Ensure numeric fields are never NULL for the FastAPI analytics
    COALESCE(m.views, 0) AS view_count,
    COALESCE(m.forwards, 0) AS forward_count,
    
    m.has_image,
    m.image_path,
    
    -- Audit column for PR-based troubleshooting
    CURRENT_TIMESTAMP AS dbt_processed_at
FROM messages m
LEFT JOIN channels c ON m.channel_name = c.channel_name
LEFT JOIN dates d ON DATE(m.message_date) = d.full_date