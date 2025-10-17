{{ config(
    materialized='incremental',
    unique_key='VIN',
    incremental_strategy='merge',
    cluster_by=['SystemMonitoring', 'IdFuel', 'SnapShotDate']
) }}

WITH Evol_Ranked_VehicleStart AS (
  SELECT 
    *,
    RANK() OVER (PARTITION BY IdVehicle ORDER BY DateVehicleStart DESC) AS rank_evol
  FROM `prj-slv-dev-westeu-01.bq_com_dev_westeu_01.Al_T_Dim_VehicleEvol`
  WHERE ValidEndDate IS NULL
  {% if is_incremental() %}
    AND DateVehicleStart > (SELECT MAX(SnapShotDate) FROM {{ this }})
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
    AND DateVehicleStart > (SELECT MAX(SnapShotDate) FROM {{ this }})
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
    AND WorkorderDate > (SELECT MAX(SnapShotDate) FROM {{ this }})
  {% endif %}
),
Planweb_today AS(
  SELECT 
    pw.IdVehicle,
    CASE 
      WHEN pw.HoursPlanWeb > 0 AND pw.KmPlanWeb > 20 THEN True
      ELSE False 
    END AS IsPlanweb,
    pw.HoursPlanWeb as PlannedHours,
    pw.KmPlanWeb as PlannedDistance
  FROM `prj-slv-dev-westeu-01.bq_tel_dev_westeu_01.Al_T_Fact_Planweb_xsercp` pw
  WHERE pw.DatePlanWeb = CURRENT_DATE() and pw.KmPlanWeb > 20
),
Provider_API_DQ AS(
  SELECT vin, CreateDate, ValidStartDate, ModifDate,
    RANK() OVER (PARTITION BY ModifDate ORDER BY vin DESC) AS rank_API_DQ
  FROM prj-bze-dev-westeu-01.bq_tel_dev_westeu_01.Al_T_Param_OmniplusVehicles 
  where ValidEndDate is null 
  GROUP BY 1,2,3,4
),
Provider_Signal_DQ AS(
  SELECT vehicle_id as vin, MAX(max_processed_ts) as last_updated_vin 
  FROM `prj-gld-dev-westeu-01.bq_tel_dev_westeu_01.Al_T_Fact_Signals_NRT_30S` 
  GROUP BY 1
),

GOLD_Vehicles_Telemetria AS (
  SELECT 
    CASE WHEN SystemMonitoring = 'OMNIPLUS' then vc.SerialNumber else vc.LicensePlate end as VIN,
    vc.IdVehicle, 
    vc.LicensePlate, 
    vc.SerialNumber, 
    vc.Size, 
    vc.Seats, 
    vc.IdFuel,
    vc.IdModel,
    dm.Model, 
    vc.IdBrand,
    db.Brand, 
    CASE 
      WHEN ec.SystemMonitoring = 'OMNIPLUS' THEN 'Omniplus'
      WHEN ec.SystemMonitoring = 'JALTEST' THEN 'Jaltest'
      ELSE 'Telemetry Off'
    END AS SystemMonitoring,
    ec.BatteryType,
    ec.IdCompany,
    dc.Company,
    ec.IdZone,
    dz.Zone,
    ec.IdZU,
    dzu.Zu as ZonalUnit,
    ec.IdDivision,
    dd.Division,
    ec.IdCg,
    dcg.Cg as ContractGroup,
    CASE WHEN oor.IdVehicle is null then False else True end as IsWoInprogress,
    COALESCE(pwt.IsPlanweb, False) AS IsPlanweb,
    pwt.PlannedHours,
    pwt.PlannedDistance
  FROM Vehicle_Evol_Clean ec 
  LEFT JOIN Vehicle_Clean vc ON ec.IdVehicle = vc.IdVehicle
  LEFT JOIN `prj-slv-dev-westeu-01.bq_com_dev_westeu_01.Al_T_Dim_Model` dm on vc.IdModel = dm.IdModel and dm.ValidEndDate is null
  LEFT JOIN `prj-slv-dev-westeu-01.bq_com_dev_westeu_01.Al_T_Dim_Brand` db on vc.IdBrand = db.IdBrand and db.ValidEndDate is null
  LEFT JOIN `prj-slv-dev-westeu-01.bq_com_dev_westeu_01.Al_T_Dim_Company` dc on ec.IdCompany = dc.IdCompany and dc.ValidEndDate is null
  LEFT JOIN `prj-slv-dev-westeu-01.bq_com_dev_westeu_01.Al_T_Dim_Zone` dz on ec.IdZone = dz.IdZone and dz.ValidEndDate is null
  LEFT JOIN `prj-slv-dev-westeu-01.bq_com_dev_westeu_01.Al_T_Dim_ZonalUnit` dzu on ec.IdZU = dzu.IdZU and dzu.ValidEndDate is null
  LEFT JOIN `prj-slv-dev-westeu-01.bq_com_dev_westeu_01.Al_T_Dim_Division` dd on ec.IdDivision = dd.IdDivision and dd.ValidEndDate is null
  LEFT JOIN `prj-slv-dev-westeu-01.bq_com_dev_westeu_01.Al_T_Dim_ContractGroup` dcg on ec.IdCg = dcg.IdCg and dcg.ValidEndDate is null
  LEFT JOIN Orders_Ranked oor on vc.IdVehicle = SAFE_CAST(oor.IdVehicle AS INT64) AND oor.rank_orders = 1
  LEFT JOIN Planweb_today pwt on vc.IdVehicle = pwt.IdVehicle
  WHERE IdFuel = 4
  {% if is_incremental() %}
    AND ec.DateVehicleStart > (SELECT MAX(SnapShotDate) FROM {{ this }}) OR ec.DateVehicleStart IS NULL
  {% endif %}
)

SELECT
  gt.*,
  CASE 
    WHEN SystemMonitoring = 'Telemetry Off' and IsPlanweb is True  then 'DISPONIBLE - OPERANDO'
    WHEN SystemMonitoring = 'Telemetry Off' and IsPlanweb is False and IsWoInprogress is False then 'DISPONIBLE - SIN SERVICIO'
    WHEN SystemMonitoring = 'Telemetry Off' and IsPlanweb is False and IsWoInprogress is True then 'NO DISPONIBLE'
    WHEN SystemMonitoring <> 'Telemetry Off' and IsWoInprogress is False then 'DISPONIBLE - SIN SERVICIO'
    WHEN SystemMonitoring <> 'Telemetry Off' and IsWoInprogress is True then 'NO DISPONIBLE'
  END AS Substate,
  CASE 
    WHEN SystemMonitoring = 'Telemetry Off' then IsPlanweb
    ELSE False
  END AS IsOperating,
  init_odometer.InitialOdomiter,
  pdq.vin is not null AS IsBatchProvider,
  pdq.ModifDate AS LastBatchProviderModif,
  COALESCE(SAFE_CAST(pdqs.last_updated_vin AS DATE) > CURRENT_DATE(), FALSE) AS ISrt,
  pdqs.last_updated_vin AS LastRtSignal,
  CURRENT_DATE() AS SnapShotDate
FROM GOLD_Vehicles_Telemetria gt
LEFT JOIN (
  SELECT 
    VIN, 
    MAX(aggregatedvalue) as InitialOdomiter
  FROM `prj-gld-dev-westeu-01.bq_tel_dev_westeu_01.Al_T_Fact_Signals_NRT_15M`
  WHERE IdAlsa = 100
    AND DATE(MaxSignalTs) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    AND aggregatedvalue IS NOT NULL 
    AND aggregatedvalue <> 0
  GROUP BY 1
) AS init_odometer ON gt.VIN = init_odometer.VIN
LEFT JOIN Provider_API_DQ pdq ON pdq.VIN = gt.VIN AND pdq.rank_API_DQ = 1
LEFT JOIN Provider_Signal_DQ pdqs ON pdqs.vin = gt.vin

{% if is_incremental() %}
  WHERE gt.VIN IS NOT NULL
{% endif %}