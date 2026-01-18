{{ config(materialized='table', schema='marts') }}

-- 1. DEFINE DATE RANGE: Use variables for resilience & easy PR updates
{% set start_date = var('date_dimension_start', '2020-01-01') %}
{% set end_date = var('date_dimension_end', '2026-12-31') %}

WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('" ~ start_date ~ "' as date)",
        end_date="cast('" ~ end_date ~ "' as date)"
    ) }}
),

-- 2. ENRICHMENT: Add business-relevant time attributes
date_enriched AS (
    SELECT
        TO_CHAR(date_day, 'YYYYMMDD')::INTEGER AS date_key,
        date_day AS full_date,
        EXTRACT(DOW FROM date_day) AS day_of_week,
        TO_CHAR(date_day, 'FMDay') AS day_name, -- 'FM' removes trailing spaces
        EXTRACT(WEEK FROM date_day) AS week_of_year,
        EXTRACT(MONTH FROM date_day) AS month,
        TO_CHAR(date_day, 'FMMonth') AS month_name,
        EXTRACT(QUARTER FROM date_day) AS quarter,
        EXTRACT(YEAR FROM date_day) AS year,
        CASE WHEN EXTRACT(DOW FROM date_day) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend
    FROM date_spine
)

SELECT
    *,
    -- Medical business logic: identifying weekdays for pharmacy trend analysis
    CASE 
        WHEN is_weekend = FALSE THEN TRUE 
        ELSE FALSE 
    END AS is_business_day,
    CURRENT_TIMESTAMP AS dbt_updated_at -- Audit column for production tracking
FROM date_enriched