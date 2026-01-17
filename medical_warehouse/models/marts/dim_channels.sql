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
    FROM {{ ref('stg_telegram_messages') }}
    GROUP BY channel_name
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['channel_name']) }} AS channel_key,
    channel_name,
    channel_type,
    first_post_date,
    last_post_date,
    total_posts,
    ROUND(avg_views::NUMERIC, 2) AS avg_views
FROM channel_stats