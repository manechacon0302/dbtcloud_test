-- dbt model configuration for Redshift optimization
{{ config(
    materialized='table',
    dist_style='even',
    sort=['year', 'ciclo', 'product_name_vod__c'],
    tags=['mccp', 'prod_plan']
) }}

-- Optimized query with CTE for better performance and readability
WITH max_modified_date AS (
    -- Isolated subquery to compute max date once, avoiding repeated calculation
    SELECT 
        MAX(modified_date) AS max_date
    FROM 
        sch_raw_data.stg_mccp_prod_plan
),

base_data AS (
    -- Pre-filter and compute base transformations to reduce computational overhead
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
        -- Extract year, cycle, territory and team once to avoid repeated SPLIT_PART calls
        SPLIT_PART(external_id_vod__c, '-', 2) AS year,
        RIGHT(SPLIT_PART(external_id_vod__c, '_', 1), 2) AS ciclo,
        SPLIT_PART(external_id_vod__c, '_', 2) AS terr,
        CASE
            WHEN external_id_vod__c LIKE '%PLUS%' THEN LEFT(SPLIT_PART(external_id_vod__c, '_', 2), 14)
            ELSE LEFT(SPLIT_PART(external_id_vod__c, '_', 2), 9)
        END AS team
    FROM 
        sch_raw_data.stg_mccp_prod_plan
    WHERE 
        ropu_nm = 'SPAIN'
        AND channel_vod__c IN ('ES_F2F', 'ES_S2S') -- Optimized OR to IN clause
        AND RIGHT(SPLIT_PART(external_id_vod__c, '_', 1), 2) != 'ES'
        AND SPLIT_PART(external_id_vod__c, '_', 2) LIKE '%CSO%'
),

calculated_fields AS (
    -- Pre-calculate dates and intervals to avoid redundant computations in SELECT clause
    SELECT
        bd.*,
        mmd.max_date,
        bd.year || RIGHT(SPLIT_PART(bd.external_id_vod__c, '_', 1), 1) AS yearciclo,
        -- Calculate cycle boundaries
        CASE
            WHEN bd.ciclo = 'C1' THEN bd.year || '-01-01'
            WHEN bd.ciclo = 'C2' THEN bd.year || '-05-01'
            WHEN bd.ciclo = 'C3' THEN bd.year || '-09-01'
        END AS firstdaycicle,
        CASE
            WHEN bd.ciclo = 'C1' THEN bd.year || '-04-30'
            WHEN bd.ciclo = 'C2' THEN bd.year || '-08-31'
            WHEN bd.ciclo = 'C3' THEN bd.year || '-12-31'
        END AS lastdaycicle
    FROM 
        base_data bd
    CROSS JOIN 
        max_modified_date mmd
),

date_calculations AS (
    -- Calculate date differences once to avoid repeated computations
    SELECT
        cf.*,
        TO_DATE(cf.max_date, 'yyyy-mm-dd') AS maxfecha,
        MAX(cf.modified_date) OVER () AS fechaCargaAct,
        TO_DATE(cf.lastdaycicle, 'yyyy-mm-dd') AS lastdaycicle_date,
        TO_DATE(cf.firstdaycicle, 'yyyy-mm-dd') AS firstdaycicle_date,
        -- Pre-calculate working days for the entire cycle
        DATEDIFF(week, TO_DATE(cf.firstdaycicle, 'yyyy-mm-dd'), TO_DATE(cf.lastdaycicle, 'yyyy-mm-dd')) * 5 + 
        (DATEDIFF(day, TO_DATE(cf.firstdaycicle, 'yyyy-mm-dd'), TO_DATE(cf.lastdaycicle, 'yyyy-mm-dd')) - 
         DATEDIFF(week, TO_DATE(cf.firstdaycicle, 'yyyy-mm-dd'), TO_DATE(cf.lastdaycicle, 'yyyy-mm-dd')) * 7) AS working_days_ciclo
    FROM 
        calculated_fields cf
)

-- Final aggregation with optimized calculations
SELECT
    MAX(dc.modified_date) AS fechaCargaAct,
    MAX(dc.maxfecha) AS maxfecha,
    dc.year,
    dc.terr,
    dc.team,
    dc.ciclo,
    dc.yearciclo,
    MAX(dc.firstdaycicle) AS firstdaycicle,
    MAX(dc.lastdaycicle) AS lastdaycicle,
    -- Days to prorate calculation with optimized CASE logic
    CASE
        WHEN MAX(dc.lastdaycicle_date) > MAX(TO_DATE(dc.fechaCargaAct, 'yyyy-mm-dd'))
        THEN DATEDIFF(day, MAX(TO_DATE(dc.fechaCargaAct, 'yyyy-mm-dd')), MAX(dc.lastdaycicle_date))
        ELSE 1
    END AS days_to_prorrate,
    -- Working days to prorate calculation
    CASE
        WHEN MAX(dc.lastdaycicle_date) > MAX(TO_DATE(dc.fechaCargaAct, 'yyyy-mm-dd'))
        THEN DATEDIFF(week, MAX(dc.firstdaycicle_date), MAX(TO_DATE(dc.fechaCargaAct, 'yyyy-mm-dd'))) * 5 + 
             (DATEDIFF(day, MAX(dc.firstdaycicle_date), MAX(TO_DATE(dc.fechaCargaAct, 'yyyy-mm-dd'))) - 
              DATEDIFF(week, MAX(dc.firstdaycicle_date), MAX(TO_DATE(dc.fechaCargaAct, 'yyyy-mm-dd'))) * 7)
        ELSE 1
    END AS working_days_to_prorrate,
    MAX(dc.working_days_ciclo) AS working_days_ciclo,
    dc.product_name_vod__c,
    SUM(dc.product_activity_goal_vod__c) AS product_activity_goal_vod__c,
    -- Pro-rated activity goal calculation
    CASE
        WHEN MAX(dc.lastdaycicle_date) > MAX(TO_DATE(dc.fechaCargaAct, 'yyyy-mm-dd'))
        THEN SUM(dc.product_activity_goal_vod__c) * 
             (CASE
                 WHEN MAX(dc.lastdaycicle_date) > MAX(TO_DATE(dc.fechaCargaAct, 'yyyy-mm-dd'))
                 THEN DATEDIFF(week, MAX(dc.firstdaycicle_date), MAX(TO_DATE(dc.fechaCargaAct, 'yyyy-mm-dd'))) * 5 + 
                      (DATEDIFF(day, MAX(dc.firstdaycicle_date), MAX(TO_DATE(dc.fechaCargaAct, 'yyyy-mm-dd'))) - 
                       DATEDIFF(week, MAX(dc.firstdaycicle_date), MAX(TO_DATE(dc.fechaCargaAct, 'yyyy-mm-dd'))) * 7)
                 ELSE 1
             END)::FLOAT / NULLIF(MAX(dc.working_days_ciclo), 0)::FLOAT
        ELSE SUM(dc.product_activity_goal_vod__c)
    END AS product_activity_goal_vod__c_workingprorrate,
    -- Rounded pro-rated activity goal
    CASE
        WHEN MAX(dc.lastdaycicle_date) > MAX(TO_DATE(dc.fechaCargaAct, 'yyyy-mm-dd'))
        THEN ROUND(SUM(dc.product_activity_goal_vod__c) * 
                   (CASE
                       WHEN MAX(dc.lastdaycicle_date) > MAX(TO_DATE(dc.fechaCargaAct, 'yyyy-mm-dd'))
                       THEN DATEDIFF(week, MAX(dc.firstdaycicle_date), MAX(TO_DATE(dc.fechaCargaAct, 'yyyy-mm-dd'))) * 5 + 
                            (DATEDIFF(day, MAX(dc.firstdaycicle_date), MAX(TO_DATE(dc.fechaCargaAct, 'yyyy-mm-dd'))) - 
                             DATEDIFF(week, MAX(dc.firstdaycicle_date), MAX(TO_DATE(dc.fechaCargaAct, 'yyyy-mm-dd'))) * 7)
                       ELSE 1
                   END)::FLOAT / NULLIF(MAX(dc.working_days_ciclo), 0)::FLOAT)
        ELSE ROUND(SUM(dc.product_activity_goal_vod__c))
    END AS product_activity_goal_vod__c_workingprorrate_round,
    SUM(dc.product_actual_activity_vod__c) AS product_actual_activity_vod__c,
    SUM(dc.product_interactions_actual_vod__c) AS product_interactions_actual_vod__c,
    -- Count of full compliance cases
    SUM(CASE
        WHEN dc.product_attainment_vod__c >= 80
            AND dc.channel_vod__c IN ('ES_F2F', 'ES_S2S')
            AND dc.last_activity_datetime_vod__c IS NOT NULL
        THEN 1
        ELSE 0
    END) AS FC,
    -- Count of non-full compliance cases
    SUM(CASE
        WHEN dc.product_attainment_vod__c <= 79
            AND dc.channel_vod__c IN ('ES_F2F', 'ES_S2S')
            AND dc.last_activity_datetime_vod__c IS NOT NULL
        THEN 1
        ELSE 0
    END) AS noFC,
    MAX(dc.systemmodstamp) AS systemmodstamp
FROM 
    date_calculations dc
GROUP BY
    dc.external_id_vod__c,
    dc.year,
    dc.terr,
    dc.team,
    dc.product_name_vod__c,
    dc.ciclo,
    dc.yearciclo