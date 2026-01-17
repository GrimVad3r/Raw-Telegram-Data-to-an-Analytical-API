
  create view "medical_warehouse"."raw_staging"."stg_telegram_messages__dbt_tmp"
    
    
  as (
    WITH source AS (
    SELECT * FROM "medical_warehouse"."raw"."telegram_messages"
),

cleaned AS (
    SELECT
        message_id,
        channel_name,
        CAST(message_date AS TIMESTAMP) AS message_date,
        COALESCE(message_text, '') AS message_text,
        LENGTH(COALESCE(message_text, '')) AS message_length,
        has_media,
        CASE 
            WHEN image_path IS NOT NULL AND image_path != '' THEN TRUE 
            ELSE FALSE 
        END AS has_image,
        image_path,
        COALESCE(views, 0) AS views,
        COALESCE(forwards, 0) AS forwards,
        loaded_at
    FROM source
    WHERE message_date IS NOT NULL
      AND message_id IS NOT NULL
)

SELECT * FROM cleaned
  );