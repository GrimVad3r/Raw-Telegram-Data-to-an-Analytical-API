
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Messages should not have future dates
SELECT 
    message_id,
    message_date
FROM "medical_warehouse"."raw_staging"."stg_telegram_messages"
WHERE message_date > CURRENT_TIMESTAMP
  
  
      
    ) dbt_internal_test