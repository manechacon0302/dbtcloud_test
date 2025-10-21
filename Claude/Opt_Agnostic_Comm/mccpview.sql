{{ config(
  materialized='table',
  sort='external_id_vod__c',
  distkey='external_id_vod__c'
) }}

WITH source_data AS (
  -- Optimization 1: Removed redundant subquery for max(modified_date), computed once in CTE
  SELECT
    *,
    MAX(modified_date) OVER () AS max_modified_date_global
  FROM
    sch_raw_data.stg_mccp_prod_plan
  WHERE
    ropu_nm = 'SPAIN'
    AND channel_vod__c IN ('ES_F2F', 'ES_S2S')
    AND SPLIT_PART(external_id_vod__c, '_', 2) NOT LIKE '%ES%'
),
parsed_fields AS (
  -- Optimization 2: Pre-compute string splits once to avoid repetitive SPLIT_PART calls
  SELECT
    *,
    SPLIT_PART(external_id_vod__c, '-', 2) AS year_extracted,
    SPLIT_PART(external_id_vod__c, '_', 2) AS terr_extracted,
    SPLIT_PART(external_id_vod__c, '_', 1) AS ciclo_prefix,
    RIGHT(SPLIT_PART(external_id_vod__c, '_', 1), 2) AS ciclo,
    RIGHT(SPLIT_PART(external_id_vod__c, '_', 1), 1) AS ciclo_digit
  FROM
    source_data
),
cycle_dates AS (
  -- Optimization 3: Pre-calculate cycle dates once instead of in CASE statements
  SELECT
    *,
    CASE
      WHEN external_id_vod__c LIKE '%PLUS%'
        THEN LEFT(terr_extracted, 14)
      ELSE LEFT(terr_extracted, 9)
    END AS team,
    CAST(year_extracted AS VARCHAR) || ciclo_digit AS yearciclo,
    CASE ciclo
      WHEN 'C1' THEN CAST(year_extracted AS VARCHAR) || '-01-01'
      WHEN 'C2' THEN CAST(year_extracted AS VARCHAR) || '-05-01'
      WHEN 'C3' THEN CAST(year_extracted AS VARCHAR) || '-09-01'
      ELSE NULL
    END AS firstdaycicle,
    CASE ciclo
      WHEN 'C1' THEN CAST(year_extracted AS VARCHAR) || '-04-30'
      WHEN 'C2' THEN CAST(year_extracted AS VARCHAR) || '-08-31'
      WHEN 'C3' THEN CAST(year_extracted AS VARCHAR) || '-12-31'
      ELSE NULL
    END AS lastdaycicle
  FROM
    parsed_fields
),
date_calculations AS (
  -- Optimization 4: Convert strings to dates once, store in CTE to avoid redundant conversions
  SELECT
    *,
    TO_DATE(max_modified_date_global, 'YYYY-MM-DD') AS fechacargaact,
    TO_DATE(max_modified_date_global, 'YYYY-MM-DD') AS maxfecha,
    TO_DATE(firstdaycicle, 'YYYY-MM-DD') AS firstdaycicle_date,
    TO_DATE(lastdaycicle, 'YYYY-MM-DD') AS lastdaycicle_date,
    DATEDIFF(day, TO_DATE(firstdaycicle, 'YYYY-MM-DD'), TO_DATE(lastdaycicle, 'YYYY-MM-DD')) AS total_days_ciclo,
    DATEDIFF(week, TO_DATE(firstdaycicle, 'YYYY-MM-DD'), TO_DATE(lastdaycicle, 'YYYY-MM-DD')) AS total_weeks_ciclo
  FROM
    cycle_dates
),
working_days_calculation AS (
  -- Optimization 5: Calculate working days once using pre-computed week differences
  SELECT
    *,
    (