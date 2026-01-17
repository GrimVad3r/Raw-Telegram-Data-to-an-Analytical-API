
  
    

  create  table "medical_warehouse"."raw_marts"."dim_channels__dbt_tmp"
  
  
    as
  
  (
    WITH channel_stats AS (
    SELECT
        channel_name,
        MIN(message_date) AS first_post_date,
        MAX(message_date) AS last_post_date,
        COUNT(*) AS total_posts,
        AVG(views) AS avg_views,
        CASE 
            WHEN channel_name LIKE '%pharma%' THEN 'Pharmaceutical'
            WHEN channel_name LIKE '%cosmetic%' THEN 'Cosmetics'
            ELSE 'Medical'
        END AS channel_type
    FROM "medical_warehouse"."raw_staging"."stg_telegram_messages"
    GROUP BY channel_name
)

SELECT
    md5(cast(coalesce(cast(channel_name as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS channel_key,
    channel_name,
    channel_type,
    first_post_date,
    last_post_date,
    total_posts,
    ROUND(avg_views::NUMERIC, 2) AS avg_views
FROM channel_stats
  );
  