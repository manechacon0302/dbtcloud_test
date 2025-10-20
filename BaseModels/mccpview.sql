select
  max(modified_date) as fechaCargaAct,
  to_date(
    (
      select
        max(modified_date)
      from
        sch_raw_data.stg_mccp_prod_plan
    ),
    'yyyy-mm-dd'
  ) as maxfecha,
  SPLIT_PART(external_id_vod__c, '-', 2) as year,
  SPLIT_PART(external_id_vod__c, '_', 2) as terr,
  CASE
  WHEN external_id_vod__c LIKE '%PLUS%' THEN left(SPLIT_PART(external_id_vod__c, '_', 2), 14)
  ELSE left(SPLIT_PART(external_id_vod__c, '_', 2), 9) END as team,
  /*left(SPLIT_PART(external_id_vod__c, '_', 2),9) as team,*/
  right(SPLIT_PART(external_id_vod__c, '_', 1), 2) as ciclo,
  SPLIT_PART(external_id_vod__c, '-', 2) + right(SPLIT_PART(external_id_vod__c, '_', 1), 1) as yearciclo,
  case
  when ciclo IN ('C1') then SPLIT_PART(external_id_vod__c, '-', 2) + '-01-01'
  WHEN ciclo IN ('C2') then SPLIT_PART(external_id_vod__c, '-', 2) + '-05-01'
  when ciclo IN ('C3') then SPLIT_PART(external_id_vod__c, '-', 2) + '-09-01' end as firstdaycicle,
  case
  when ciclo IN ('C1') then SPLIT_PART(external_id_vod__c, '-', 2) + '-04-30'
  WHEN ciclo IN ('C2') then SPLIT_PART(external_id_vod__c, '-', 2) + '-08-31'
  when ciclo IN ('C3') then SPLIT_PART(external_id_vod__c, '-', 2) + '-12-31' end as lastdaycicle,
  case
  when to_date(lastdaycicle, 'yyyy-mm-dd') > to_date(fechacargaact, 'yyyy-mm-dd') -- current_date --
  then --DATEDIFF(day,current_date,to_date(lastdaycicle,'dd-mm-yyyy'))
  DATEDIFF(
    day,
    to_date(fechacargaact, 'yyyy-mm-dd'),
    to_date(lastdaycicle, 'yyyy-mm-dd')
  )
  else 1 end as days_to_prorrate,
  case
  when to_date(lastdaycicle, 'yyyy-mm-dd') > to_date(fechacargaact, 'yyyy-mm-dd') -- current_date --
  then -- DATEDIFF(week,to_date(firstdaycicle,'dd-mm-yyyy'),current_date)*5+
  --  (DATEDIFF(day,to_date(firstdaycicle,'dd-mm-yyyy'),current_date)-DATEDIFF(week,to_date(firstdaycicle,'dd-mm-yyyy'),current_date)*7)
  DATEDIFF(
    week,
    to_date(firstdaycicle, 'yyyy-mm-dd'),
    to_date(fechacargaact, 'yyyy-mm-dd')
  ) * 5 + (
    DATEDIFF(
      day,
      to_date(firstdaycicle, 'yyyy-mm-dd'),
      to_date(fechacargaact, 'yyyy-mm-dd')
    ) - DATEDIFF(
      week,
      to_date(firstdaycicle, 'yyyy-mm-dd'),
      to_date(fechacargaact, 'yyyy-mm-dd')
    ) * 7
  )
  else 1 end as working_days_to_prorrate,
  DATEDIFF(
    week,
    to_date(firstdaycicle, 'yyyy-mm-dd'),
    to_date(lastdaycicle, 'yyyy-mm-dd')
  ) * 5 + (
    DATEDIFF(
      day,
      to_date(firstdaycicle, 'yyyy-mm-dd'),
      to_date(lastdaycicle, 'yyyy-mm-dd')
    ) - DATEDIFF(
      week,
      to_date(firstdaycicle, 'yyyy-mm-dd'),
      to_date(lastdaycicle, 'yyyy-mm-dd')
    ) * 7
  ) as working_days_ciclo,
  product_name_vod__c,
  sum(product_activity_goal_vod__c) AS product_activity_goal_vod__c,
  /* sum(product_activity_goal_vod__c)*days_to_prorrate AS product_activity_goal_vod__c_prorrate , */
  case
  when to_date(lastdaycicle, 'yyyy-mm-dd') > fechacargaact -- current_date --
  then sum(product_activity_goal_vod__c) * working_days_to_prorrate / working_days_ciclo
  ELSE SUM(product_activity_goal_vod__c) END AS product_activity_goal_vod__c_workingprorrate,
  case
  when to_date(lastdaycicle, 'yyyy-mm-dd') > fechacargaact -- current_date  --
  then ROUND(
    SUM(product_activity_goal_vod__c) * working_days_to_prorrate / working_days_ciclo
  )
  ELSE ROUND(SUM(product_activity_goal_vod__c)) END AS product_activity_goal_vod__c_workingprorrate_round,
  sum(product_actual_activity_vod__c) as product_actual_activity_vod__c,
  sum(product_interactions_actual_vod__c) as product_interactions_actual_vod__c,
  sum(
    CASE
    WHEN product_attainment_vod__c >= 80
    and channel_vod__c in ('ES_F2F', 'ES_S2S')
    and last_activity_datetime_vod__c is not null THEN 1 END
  ) AS FC,
  sum(
    CASE
    WHEN product_attainment_vod__c <= 79
    and channel_vod__c IN ('ES_F2F', 'ES_S2S')
    and last_activity_datetime_vod__c is not null THEN 1 END
  ) AS noFC,
  max(systemmodstamp) as systemmodstamp --,
  --max(modified_date) as fechaCargaAct
from
  sch_raw_data.stg_mccp_prod_plan
where
  ropu_nm = 'SPAIN'
  and (
    channel_vod__c = 'ES_F2F'
    OR channel_vod__c = 'ES_S2S'
  )
  AND ciclo != 'ES'
  -- and team like '%CRM%'
  and team like '%CSO%'
group by
  external_id_vod__c,
  year,
  terr,
  product_name_vod__c,
  ciclo,
  yearciclo