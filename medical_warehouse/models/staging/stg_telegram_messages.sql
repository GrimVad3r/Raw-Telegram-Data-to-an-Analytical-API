{{ config(materialized='view') }}

-- 1. SOURCE LAYER
WITH source AS (
    SELECT * FROM {{ source('raw', 'telegram_messages') }}
),

-- 2. CLEANING & STANDARDIZATION
standardized AS (
    SELECT
        -- Basic cleanup
        message_id,
        channel_name,
        CAST(message_date AS TIMESTAMP) AS message_date,
        
        -- Systematic Text Cleaning for NLP resilience
        TRIM(REGEXP_REPLACE(COALESCE(message_text, ''), '\s+', ' ', 'g')) AS message_text,
        
        -- Derived Metrics
        LENGTH(COALESCE(message_text, '')) AS message_length,
        COALESCE(views, 0) AS views,
        COALESCE(forwards, 0) AS forwards,
        
        -- Media Handling logic
        COALESCE(has_media, FALSE) AS has_media,
        CASE 
            WHEN image_path IS NOT NULL AND image_path != '' AND image_path != 'None' THEN TRUE 
            ELSE FALSE 
        END AS has_image,
        image_path,
        
        -- Metadata for auditability
        loaded_at AS scraped_at,
        
        -- Deduplication Logic: Keep the record with the most recent load time per message
        ROW_NUMBER() OVER (
            PARTITION BY message_id, channel_name 
            ORDER BY loaded_at DESC
        ) AS row_num

    FROM source
    WHERE message_date IS NOT NULL 
      AND message_id IS NOT NULL
      AND channel_name IS NOT NULL
),

-- 3. FINAL LAYER: Filter out duplicates
SELECT
    -- Create a surrogate key to identify this specific message record
    {{ dbt_utils.generate_surrogate_key(['message_id', 'channel_name']) }} AS message_key,
    message_id,
    channel_name,
    message_date,
    message_text,
    message_length,
    has_media,
    has_image,
    image_path,
    views,
    forwards,
    scraped_at
FROM standardized
WHERE row_num = 1 -- Systematic deduplication