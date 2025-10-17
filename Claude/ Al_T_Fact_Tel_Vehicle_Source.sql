{{ config(
    materialized='incremental',
    unique_key='VIN',
    on_schema_change='sync_all_columns',
    partition_by={
        'field': 'SnapShotDate',
        'data_type': 'date',
        'granularity': 'day'
    },
    cluster_by=['SystemMonitoring', 'IdDivision', 'IdCompany'],
    incremental_strategy='insert_overwrite',
    partition_expiration_days=null
) }}

WITH Evol_Ranked_VehicleStart AS (
  SELECT 
    *,
    RANK() OVER (PARTITION BY IdVehicle ORDER BY DateVehicleStart DESC) AS rank_evol
  FROM `prj-slv-dev-westeu-01.bq_com_dev_westeu_01.Al_T_Dim_VehicleEvol`
  WHERE ValidEndDate IS NULL
  {% if is_incremental() %}
    AND DATE(_PARTITIONTIME) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
  {% endif %}
), 
Vehicle_Evol_Clean AS (
  SELECT * 
  FROM Evol_Ranked_VehicleStart
  WHERE rank_evol = 1
),
Ranked_VehicleStart AS (
  SELECT 
    *,
    RANK() OVER (PARTITION BY SerialNumber ORDER BY DateVehicleStart DESC) AS rank_dim
  FROM `prj-slv-dev-westeu-01.bq_com_dev_westeu_01.Al_T_Dim_Vehicle`
  WHERE ValidEndDate IS NULL
  AND IdTypeOrigin = 1
  {% if is_incremental() %}
    AND DATE(_PARTITIONTIME) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
  {% endif %}
),
Vehicle_Clean AS (
  SELECT * 
  FROM Ranked_VehicleStart
  WHERE ValidEndDate IS NULL
  AND rank_dim = 1
),
Orders_Ranked AS(
SELECT *,
RANK() OVER (PARTITION BY IdVehicle ORDER BY WorkorderId DESC) as rank_orders
FROM `prj-slv-dev-westeu-01.bq_tel_dev_westeu_01.Al_T_Fact_WorkOrders` 
WHERE STATUS = 'INPRG' AND Istask = 0
{% if is_incremental() %}
  AND DATE(_PARTITIONTIME) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
{% endif %}
) ,
Planweb_today AS(
SELECT 
pw.IdVehicle,
CASE 
  WHEN pw.HoursPlanWeb > 0 AND pw.KmPlanWeb > 20 THEN True
  ELSE False 
END AS IsPlanweb,
pw.HoursPlanWeb as PlannedHours,
pw.KmPlanWeb as PlannedDistance
from `prj-slv-dev-westeu-01.bq_tel_dev_westeu_01.Al_T_Fact_Planweb_xsercp` pw
WHERE pw.DatePlanWeb = CURRENT_DATE() and pw.KmPlanWeb > 20
),
Provider_API_DQ AS(
SELECT vin, CreateDate, ValidStartDate, ModifDate,
RANK() OVER (PARTITION BY ModifDate ORDER BY vin DESC) AS rank_API_DQ
FROM prj-bze-dev-westeu-01.bq_tel_dev_westeu_01.Al_T_Param_OmniplusVehicles 
where ValidEndDate is null
{% if is_incremental() %}
  AND ModifDate >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
{% endif %}
GROUP BY 1,2,3,4
),
Provider_Signal_DQ AS(
SELECT vehicle_id as vin, MAX(max_processed_ts) as last_updated_vin 
FROM `prj-gld-dev-westeu-01.bq_tel_dev_westeu_01