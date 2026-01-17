-- Messages should not have future dates
SELECT 
    message_id,
    message_date
FROM "medical_warehouse"."raw_staging"."stg_telegram_messages"
WHERE message_date > CURRENT_TIMESTAMP