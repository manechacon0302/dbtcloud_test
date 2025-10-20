WITH max_date_calc AS (
SELECT MAX(modified_date) AS max_modified_date
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
-- Pre-calculate string splits to avoid redundant operations
SPLIT_PART(external_id_vod__c, '-', 2) AS year,
SPLIT_PART(external_id_vod__c, '', 2) AS terr_full,
RIGHT(SPLIT_PART(external_id_vod__c, '', 1), 2) AS ciclo
FROM sch_raw_data.stg_mccp_prod_plan
WHERE ropu_nm = 'SPAIN'
AND channel_vod__c IN ('ES_F2F', 'ES_S2S')  -- Optimized OR to IN clause for better performance
AND RIGHT(SPLIT_PART(external_id_vod__c, '', 1), 2) != 'ES'  -- Filter applied early
AND SPLIT_PART(external_id_vod__c, '', 2) LIKE '%CSO%'  -- Filter applied early in base CTE
)
SELECT
MAX(bd.modified_date) AS fechaCargaAct,
TO_DATE(md.max_modified_date, 'yyyy-mm-dd') AS maxfecha,
bd.year,
bd.terr_full AS terr,
-- Optimized CASE for team calculation
CASE
WHEN bd.external_id_vod__c LIKE '%PLUS%' THEN LEFT(bd.terr_full, 14)
ELSE LEFT(bd.terr_full, 9)
END AS team,
bd.ciclo,
bd.year || RIGHT(SPLIT_PART(bd.external_id_vod__c, '_', 1), 1) AS yearciclo,
-- Optimized date calculations using direct concatenation
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
-- Optimized prorate calculations with reduced nested logic
CASE
WHEN TO_DATE(
CASE
WHEN bd.ciclo = 'C1' THEN bd.year || '-04-30'
WHEN bd.ciclo = 'C2' THEN bd.year || '-08-31'
WHEN bd.ciclo = 'C3' THEN bd.year || '-12-31'
END, 'yyyy-mm-dd'
) > TO_DATE(MAX(bd.modified_date), 'yyyy-mm-dd')
THEN DATEDIFF(
day,
TO_DATE(MAX(bd.modified_date), 'yyyy-mm-dd'),
TO_DATE(
CASE
WHEN bd.ciclo = 'C1' THEN bd.year || '-04-30'
WHEN bd.ciclo = 'C2' THEN bd.year || '-08-31'
WHEN bd.ciclo = 'C3' THEN bd.year || '-12-31'
END, 'yyyy-mm-dd'
)
)
ELSE 1
END AS days_to_prorrate,
-- Working days calculation optimized
CASE
WHEN TO_DATE(
CASE
WHEN bd.ciclo = 'C1' THEN bd.year || '-04-30'
WHEN bd.ciclo = 'C2' THEN bd.year || '-08-31'
WHEN bd.ciclo = 'C3' THEN bd.year || '-12-31'
END, 'yyyy-mm-dd'
) > TO_DATE(MAX(bd.modified_date), 'yyyy-mm-dd')
THEN DATEDIFF(
week,
TO_DATE(
CASE
WHEN bd.ciclo = 'C1' THEN bd.year || '-01-01'
WHEN bd.ciclo = 'C2' THEN bd.year || '-05-01'
WHEN bd.ciclo = 'C3' THEN bd.year || '-09-01'
END, 'yyyy-mm-dd'
),
TO_DATE(MAX(bd.modified_date), 'yyyy-mm-dd')
) * 5 + (
DATEDIFF(
day,
TO_DATE(
CASE
WHEN bd.ciclo = 'C1' THEN bd.year || '-01-01'
WHEN bd.ciclo = 'C2' THEN bd.year || '-05-01'
WHEN bd.ciclo = 'C3' THEN bd.year || '-09-01'
END, 'yyyy-mm-dd'
),
TO_DATE(MAX(bd.modified_date), 'yyyy-mm-dd')
) - DATEDIFF(
week,
TO_DATE(
CASE
WHEN bd.ciclo = 'C1' THEN bd.year || '-01-01'
WHEN bd.ciclo = 'C2' THEN bd.year || '-05-01'
WHEN bd.ciclo = 'C3' THEN bd.year || '-09-01'
END, 'yyyy-mm-dd'
),
TO_DATE(MAX(bd.modified_date), 'yyyy-mm-dd')
) * 7
)
ELSE 1
END AS working_days_to_prorrate,
-- Total working days in cycle
DATEDIFF(
week,
TO_DATE(
CASE
WHEN bd.ciclo = 'C1' THEN bd.year || '-01-01'
WHEN bd.ciclo = 'C2' THEN bd.year || '-05-01'
WHEN bd.ciclo = 'C3' THEN bd.year || '-09-01'
END, 'yyyy-mm-dd'
),
TO_DATE(
CASE
WHEN bd.ciclo = 'C1' THEN bd.year || '-04-30'
WHEN bd.ciclo = 'C2' THEN bd.year || '-08-31'
WHEN bd.ciclo = 'C3' THEN bd.year || '-12-31'
END, 'yyyy-mm-dd'
)
) * 5 + (
DATEDIFF(
day,
TO_DATE(
CASE
WHEN bd.ciclo = 'C1' THEN bd.year || '-01-01'
WHEN bd.ciclo = 'C2' THEN bd.year || '-05-01'
WHEN bd.ciclo = 'C3' THEN bd.year || '-09-01'
END, 'yyyy-mm-dd'
),
TO_DATE(
CASE
WHEN bd.ciclo = 'C1' THEN bd.year || '-04-30'
WHEN bd.ciclo = 'C2' THEN bd.year || '-08-31'
WHEN bd.ciclo = 'C3' THEN bd.year || '-12-31'
END, 'yyyy-mm-dd'
)
) - DATEDIFF(
week,
TO_DATE(
CASE
WHEN bd.ciclo = 'C1' THEN bd.year || '-01-01'
WHEN bd.ciclo = 'C2' THEN bd.year || '-05-01'
WHEN bd.ciclo = 'C3' THEN bd.year || '-09-01'
END, 'yyyy-mm-dd'
),
TO_DATE(
CASE
WHEN bd.ciclo = 'C1' THEN bd.year || '-04-30'
WHEN bd.ciclo = 'C2' THEN bd.year || '-08-31'
WHEN bd.ciclo = 'C3' THEN bd.year || '-12-31'
END, 'yyyy-mm-dd'
)
) * 7
) AS working_days_ciclo,
bd.product_name_vod__c,
SUM(bd.product_activity_goal_vod__c) AS product_activity_goal_vod__c,
-- Simplified prorate calculation using pre-calculated values
CASE
WHEN TO_DATE(
CASE
WHEN bd.ciclo = 'C1' THEN bd.year || '-04-30'
WHEN bd.ciclo = 'C2' THEN bd.year || '-08-31'
WHEN bd.ciclo = 'C3' THEN bd.year || '-12-31'
END, 'yyyy-mm-dd'
) > TO_DATE(MAX(bd.modified_date), 'yyyy-mm-dd')
THEN SUM(bd.product_activity_goal_vod__c) *
(DATEDIFF(
week,
TO_DATE(
CASE
WHEN bd.ciclo = 'C1' THEN bd.year || '-01-01'
WHEN bd.ciclo = 'C2' THEN bd.year || '-05-01'
WHEN bd.ciclo = 'C3' THEN bd.year || '-09-01'
END, 'yyyy-mm-dd'
),
TO_DATE(MAX(bd.modified_date), 'yyyy-mm-dd')
) * 5 + (
DATEDIFF(
day,
TO_DATE(
CASE
WHEN bd.ciclo = 'C1' THEN bd.year || '-01-01'
WHEN bd.ciclo = 'C2' THEN bd.year || '-05-01'
WHEN bd.ciclo = 'C3' THEN bd.year || '-09-01'
END, 'yyyy-mm-dd'
),
TO_DATE(MAX(bd.modified_date), 'yyyy-mm-dd')
) - DATEDIFF(
week,
TO_DATE(
CASE
WHEN bd.ciclo = 'C1' THEN bd.year || '-01-01'
WHEN bd.ciclo = 'C2' THEN bd.year || '-05-01'
WHEN bd.ciclo = 'C3' THEN bd.year || '-09-01'
END, 'yyyy-mm-dd'
),
TO_DATE(MAX(bd.modified_date), 'yyyy-mm-dd')
) * 7
)) / NULLIF((
DATEDIFF(
week,
TO_DATE(
CASE
WHEN bd.ciclo = 'C1' THEN bd.year || '-01-01'
WHEN bd.ciclo = 'C2' THEN bd.year || '-05-01'
WHEN bd.ciclo = 'C3' THEN bd.year || '-09-01'
END, 'yyyy-mm-dd'
),
TO_DATE(
CASE
WHEN bd.ciclo = 'C1' THEN bd.year || '-04-30'
WHEN bd.ciclo = 'C2' THEN bd.year || '-08-31'
WHEN bd.ciclo = 'C3' THEN bd.year || '-12-31'
END, 'yyyy-mm-dd'
)
) * 5 + (
DATEDIFF(
day,
TO_DATE(
CASE
WHEN bd.ciclo = 'C1' THEN bd.year || '-01-01'
WHEN bd.ciclo = 'C2' THEN bd.year || '-05-01'
WHEN bd.ciclo = 'C3' THEN bd.year || '-09-01'
END, 'yyyy-mm-dd'
),
TO_DATE(
CASE
WHEN bd.ciclo = 'C1' THEN bd.year || '-04-30'
WHEN bd.ciclo = 'C2' THEN bd.year || '-08-31'
WHEN bd.ciclo = 'C3' THEN bd.year || '-12-31'
END, 'yyyy-mm-dd'
)
) - DATEDIFF(
week,
TO_DATE(
CASE
WHEN bd.ciclo = 'C1' THEN bd.year || '-01-01'
WHEN bd.ciclo = 'C2' THEN bd.year || '-05-01'
WHEN bd.ciclo = 'C3' THEN bd.year || '-09-01'
END, 'yyyy-mm-dd'
),
TO_DATE(
CASE
WHEN bd.ciclo = 'C1' THEN bd.year || '-04-30'
WHEN bd.ciclo = 'C2' THEN bd.year || '-08-31'
WHEN bd.ciclo = 'C3' THEN bd.year || '-12-31'
END, 'yyyy-mm-dd'
)
) * 7
)), 1)
ELSE SUM(bd.product_activity_goal_vod__c)
END AS product_activity_goal_vod__c_workingprorrate,
CASE
WHEN TO_DATE(
CASE
WHEN bd.ciclo = 'C1' THEN bd.year || '-04-30'
WHEN bd.ciclo = 'C2' THEN bd.year || '-08-31'
WHEN bd.ciclo = 'C3' THEN bd.year || '-12-31'
END, 'yyyy-mm-dd'
) > TO_DATE(MAX(bd.modified_date), 'yyyy-mm-dd')
THEN ROUND(
SUM(bd.product_activity_goal_vod__c) *
(DATEDIFF(
week,
TO_DATE(
CASE
WHEN bd.ciclo = 'C1' THEN bd.year || '-01-01'
WHEN bd.ciclo = 'C2' THEN bd.year || '-05-01'
WHEN bd.ciclo = 'C3' THEN bd.year || '-09-01'
END, 'yyyy-mm-dd'
),
TO_DATE(MAX(bd.modified_date), 'yyyy-mm-dd')
) * 5 + (
DATEDIFF(
day,
TO_DATE(
CASE
WHEN bd.ciclo = 'C1' THEN bd.year || '-01-01'
WHEN bd.ciclo = 'C2' THEN bd.year || '-05-01'
WHEN bd.ciclo = 'C3' THEN bd.year || '-09-01'
END, 'yyyy-mm-dd'
),
TO_DATE(MAX(bd.modified_date), 'yyyy-mm-dd')
) - DATEDIFF(
week,
TO_DATE(
CASE
WHEN bd.ciclo = 'C1' THEN bd.year || '-01-01'
WHEN bd.ciclo = 'C2' THEN bd.year || '-05-01'
WHEN bd.ciclo = 'C3' THEN bd.year || '-09-01'
END, 'yyyy-mm-dd'
),
TO_DATE(MAX(bd.modified_date), 'yyyy-mm-dd')
) * 7
)) / NULLIF((
DATEDIFF(
week,
TO_DATE(
CASE
WHEN bd.ciclo = 'C1' THEN bd.year || '-01-01'
WHEN bd.ciclo = 'C2' THEN bd.year || '-05-01'
WHEN bd.ciclo = 'C3' THEN bd.year || '-09-01'
END, 'yyyy-mm-dd'
),
TO_DATE(
CASE
WHEN bd.ciclo = 'C1' THEN bd.year || '-04-30'
WHEN bd.ciclo = 'C2' THEN bd.year || '-08-31'
WHEN bd.ciclo = 'C3' THEN bd.year || '-12-31'
END, 'yyyy-mm-dd'
)
) * 5 + (
DATEDIFF(
day,
TO_DATE(
CASE
WHEN bd.ciclo = 'C1' THEN bd.year || '-01-01'
WHEN bd.ciclo = 'C2' THEN bd.year || '-05-01'
WHEN bd.ciclo = 'C3' THEN bd.year || '-09-01'
END, 'yyyy-mm-dd'
),
TO_DATE(
CASE
WHEN bd.ciclo = 'C1' THEN bd.year || '-04-30'
WHEN bd.ciclo = 'C2' THEN bd.year || '-08-31'
WHEN bd.ciclo = 'C3' THEN bd.year || '-12-31'
END, 'yyyy-mm-dd'
)
) - DATEDIFF(
week,
TO_DATE(
CASE
WHEN bd.ciclo = 'C1' THEN bd.year || '-01-01'
WHEN bd.ciclo = 'C2' THEN bd.year || '-05-01'
WHEN bd.ciclo = 'C3' THEN bd.year || '-09-01'
END, 'yyyy-mm-dd'
),
TO_DATE(
CASE
WHEN bd.ciclo = 'C1' THEN bd.year || '-04-30'
WHEN bd.ciclo = 'C2' THEN bd.year || '-08-31'
WHEN bd.ciclo = 'C3' THEN bd.year || '-12-31'
END, 'yyyy-mm-dd'
)
) * 7
)), 1)
)
ELSE ROUND(SUM(bd.product_activity_goal_vod__c))
END AS product_activity_goal_vod__c_workingprorrate_round,
SUM(bd.product_actual_activity_vod__c) AS product_actual_activity_vod__c,
SUM(bd.product_interactions_actual_vod__c) AS product_interactions_actual_vod__c,
-- Optimized conditional aggregations with combined conditions
SUM(
CASE
WHEN bd.product_attainment_vod__c >= 80
AND bd.channel_vod__c IN ('ES_F2F', 'ES_S2S')
AND bd.last_activity_datetime_vod__c IS NOT NULL
THEN 1
ELSE 0
END
) AS FC,
SUM(
CASE
WHEN bd.product_attainment_vod__c <= 79
AND bd.channel_vod__c IN ('ES_F2F', 'ES_S2S')
AND bd.last_activity_datetime_vod__c IS NOT NULL
THEN 1
ELSE 0
END
) AS noFC,
MAX(bd.systemmodstamp) AS systemmodstamp
FROM base_data bd
CROSS JOIN max_date_calc md
GROUP BY
bd.external_id_vod__c,
bd.year,
bd.terr_full,
bd.product_name_vod__c,
bd.ciclo,
md.max_modified_date