
  
    

  create  table "medical_warehouse"."raw_marts"."fct_messages__dbt_tmp"
  
  
    as
  
  (
    WITH messages AS (
    SELECT * FROM "medical_warehouse"."raw_staging"."stg_telegram_messages"
),

channels AS (
    SELECT * FROM "medical_warehouse"."raw_marts"."dim_channels"
),

dates AS (
    SELECT * FROM "medical_warehouse"."raw_marts"."dim_dates"
)

SELECT
    m.message_id,
    c.channel_key,
    d.date_key,
    m.message_text,
    m.message_length,
    m.views AS view_count,
    m.forwards AS forward_count,
    m.has_image,
    m.image_path
FROM messages m
LEFT JOIN channels c ON m.channel_name = c.channel_name
LEFT JOIN dates d ON DATE(m.message_date) = d.full_date
  );
  