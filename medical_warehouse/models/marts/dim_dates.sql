WITH date_spine AS (
    -- The macro already generates: "select cast(...) as date_day from ..."
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2020-01-01' as date)",
        end_date="cast('2026-12-31' as date)"
    ) }}
)

SELECT
    TO_CHAR(date_day, 'YYYYMMDD')::INTEGER AS date_key,
    date_day AS full_date,
    EXTRACT(DOW FROM date_day) AS day_of_week,
    TO_CHAR(date_day, 'Day') AS day_name,
    EXTRACT(WEEK FROM date_day) AS week_of_year,
    EXTRACT(MONTH FROM date_day) AS month,
    TO_CHAR(date_day, 'Month') AS month_name,
    EXTRACT(QUARTER FROM date_day) AS quarter,
    EXTRACT(YEAR FROM date_day) AS year,
    CASE WHEN EXTRACT(DOW FROM date_day) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend
FROM date_spine