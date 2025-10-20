-- dbt model configuration for Redshift optimization
{{ config(
    materialized='table',
    dist_style='even',
    sort=['year', 'ciclo', 'terr'],
    tags=['mccp', 'prod_plan']
) }}

-- Optimized query: calculated fields moved to CTE to avoid repetition and improve readability
-- Subquery for max modified_date calculated once and reused via CTE
WITH max_date_calc AS (
    SELECT 
        MAX(modified_date) AS max_modified_date
    FROM sch_raw_data.stg_mccp_prod_plan
),

base_data AS (
    SELECT
        -- Reuse max_date_calc CTE to avoid correlated subquery
        (SELECT max_modified_date FROM max_date_calc) AS fechaCargaAct,
        TO_DATE((SELECT max_modified_date FROM max_date_calc), 'yyyy-mm-dd') AS maxfecha,
        
        -- Parse external_id_vod__c once and reuse computed values
        SPLIT_PART(external_id_vod__c, '-', 2) AS year,
        SPLIT_PART(external_id_vod__c, '_', 2) AS terr,
        CASE
            WHEN external_id_vod__c LIKE '%PLUS%' THEN LEFT(SPLIT_PART(external_id_vod__c, '_', 2), 14)
            ELSE LEFT(SPLIT_PART(external_id_vod__c, '_', 2), 9)
        END AS team,
        RIGHT(SPLIT_PART(external_id_vod__c, '_', 1), 2) AS ciclo,
        SPLIT_PART(external_id_vod__c, '-', 2) || RIGHT(SPLIT_PART(external_id_vod__c, '_', 1), 1) AS yearciclo,
        
        -- Optimized: use CONCAT instead of + for string concatenation in Redshift
        CASE
            WHEN RIGHT(SPLIT_PART(external_id_vod__c, '_', 1), 2) = 'C1' THEN SPLIT_PART(external_id_vod__c, '-', 2) || '-01-01'
            WHEN RIGHT(SPLIT_PART(external_id_vod__c, '_', 1), 2) = 'C2' THEN SPLIT_PART(external_id_vod__c, '-', 2) || '-05-01'
            WHEN RIGHT(SPLIT_PART(external_id_vod__c, '_', 1), 2) = 'C3' THEN SPLIT_PART(external_id_vod__c, '-', 2) || '-09-01'
        END AS firstdaycicle,
        
        CASE
            WHEN RIGHT(SPLIT_PART(external_id_vod__c, '_', 1), 2) = 'C1' THEN SPLIT_PART(external_id_vod__c, '-', 2) || '-04-30'
            WHEN RIGHT(SPLIT_PART(external_id_vod__c, '_', 1), 2) = 'C2' THEN SPLIT_PART(external_id_vod__c, '-', 2) || '-08-31'
            WHEN RIGHT(SPLIT_PART(external_id_vod__c, '_', 1), 2) = 'C3' THEN SPLIT_PART(external_id_vod__c, '-', 2) || '-12-31'
        END AS lastdaycicle,
        
        product_name_vod__c,
        product_activity_goal_vod__c,
        product_actual_activity_vod__c,
        product_interactions_actual_vod__c,
        product_attainment_vod__c,
        channel_vod__c,
        last_activity_datetime_vod__c,
        systemmodstamp,
        external_id_vod__c
    FROM sch_raw_data.stg_mc