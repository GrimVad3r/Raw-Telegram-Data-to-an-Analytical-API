WITH detections AS (
    SELECT * FROM {{ source('raw', 'yolo_detections') }}
),

messages AS (
    SELECT * FROM {{ ref('fct_messages') }}
)

SELECT
    d.message_id,
    m.channel_key,
    m.date_key,
    d.detected_class,
    d.confidence_score,
    d.image_category
FROM detections d
INNER JOIN messages m ON d.message_id = m.message_id