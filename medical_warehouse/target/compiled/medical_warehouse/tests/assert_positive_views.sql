-- View counts should be non-negative
SELECT 
    message_id,
    views
FROM "medical_warehouse"."raw_staging"."stg_telegram_messages"
WHERE views < 0