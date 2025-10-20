-- dbt configuration for Redshift optimization
{{ config(
    materialized='table',
    dist_style='even',
    sort=['year', 'ciclo', 'team'],
    tags=['mccp', 'product_plan']
) }}

-- Optimized query with CTE to avoid repeated subquery execution and improve readability
WITH max_modified AS (
    -- Compute max modified_date once to avoid repeated subquery execution
    SELECT MAX(modified_date) AS max_date
    FROM sch_raw_data.stg_mccp_prod_plan
),
base_data AS (
    SELECT
        modified_date,
        external_id_vod__c,
        product_name_vod__c,
        product_activity_goal_vod__c,
        product_actual_activity_vod__c,
        product_interactions_actual_vod__c,
        product_attainment_vod__c,
        channel_vod__c,
        last_activity_datetime_vod__c,
        systemmodstamp,
        -- Pre-compute string operations once to avoid repetition in SELECT and GROUP BY
        SPLIT_PART(external_id_vod__c, '-', 2) AS year,
        SPLIT_PART(external_id_vod__c, '_', 2) AS terr,
        CASE
            WHEN external_id_vod__c LIKE '%PLUS%' THEN LEFT(SPLIT_PART(external_id_vod__c, '_', 2), 14)
            ELSE LEFT(SPLIT_PART(external_id_vod__c, '_', 2), 9)
        END AS team,
        RIGHT(SPLIT_PART(external_id_vod__c, '_', 1), 2) AS ciclo
    FROM sch_raw_data.stg_mccp_prod_plan
    WHERE ropu_nm = 'SPAIN'
        AND channel_vod__c IN ('ES_F2F', 'ES_S2S') -- Optimized OR to IN clause
        AND RIGHT(SPLIT_PART(external_id_vod__c, '_', 1), 2) != 'ES' -- Pre-filter using computed ciclo
        AND SPLIT_PART(external_id_vod__c, '_', 2) LIKE '%CSO%' -- Pre-filter using terr before team computation
)
SELECT
    MAX(bd.modified_date) AS fechaCargaAct,
    TO_DATE(mm.max_date, 'yyyy-mm-dd') AS maxfecha,
    bd.year,
    bd.terr,
    bd.team,
    bd.ciclo,
    bd.year || RIGHT(SPLIT_PART(bd.external_id_vod__c, '_', 1), 1) AS yearciclo, -- Use concat operator for clarity
    -- Compute cycle date boundaries
    CASE
        WHEN bd.ciclo = 'C1' THEN bd.year || '-01-01'
        WHEN bd.ciclo = 'C2' THEN bd.year || '-05-01'
        WHEN bd.ciclo = 'C3' THEN bd.year || '-09-01'
    END AS firstdaycicle,
    CASE
        WHEN bd.ciclo = 'C1' THEN bd.year || '-04-30'
        WHEN bd.ciclo = 'C2' THEN bd.year || '-08-31'
        WHEN bd.ciclo = 'C3' THEN bd.year || '-12-31'
    END AS lastdaycicle,
    -- Calculate days to prorate with optimized date conversions
    CASE
        WHEN TO_DATE(
            CASE
                WHEN bd.ciclo = 'C1' THEN bd.year || '-04-30'
                WHEN bd.ciclo = 'C2' THEN bd.year || '-08-31'
                WHEN bd.ciclo = 'C3' THEN bd.year || '-12-31'
            END, 'yyyy-mm-dd') > TO_DATE(MAX(bd.modified_date), 'yyyy-mm