{{ config(
    materialized='incremental',
    unique_key='detection_id',
    on_schema_change='append_new_columns'
) }}

-- 1. SOURCE LAYER: Fetch raw ML output
WITH raw_detections AS (
    SELECT 
        *,
        -- Generate a unique ID for incremental tracking
        {{ dbt_utils.generate_surrogate_key(['message_id', 'detected_class', 'confidence_score']) }} AS detection_id
    FROM {{ source('raw', 'yolo_detections') }}
    
    {% if is_incremental() %}
    -- Only process new detections since the last run
    WHERE scraped_at > (SELECT MAX(scraped_at) FROM {{ this }})
    {% endif %}
),

-- 2. ENRICHMENT LAYER: Join with verified messages
messages AS (
    SELECT 
        message_id, 
        channel_key, 
        date_key 
    FROM {{ ref('fct_messages') }}
)

-- 3. FINAL LAYER: Apply Systematic Error Handling (Confidence Filters)
SELECT
    rd.detection_id,
    rd.message_id,
    m.channel_key,
    m.date_key,
    rd.detected_class,
    rd.confidence_score,
    
    -- Business Logic: Group low-confidence items as 'unverified'
    CASE 
        WHEN rd.confidence_score >= 0.70 THEN rd.image_category
        ELSE 'unverified'
    END AS image_category,

    CURRENT_TIMESTAMP AS dbt_updated_at
FROM raw_detections rd
INNER JOIN messages m ON rd.message_id = m.message_id
WHERE rd.confidence_score >= 0.40 -- Systematic Filter: Ignore total noise