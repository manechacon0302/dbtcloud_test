

with base as (
    select
        external_id_vod__c,
        product_name_vod__c,
        modified_date,
        systemmodstamp,
        product_activity_goal_vod__c,
        product_actual_activity_vod__c,
        product_interactions_actual_vod__c,
        product_attainment_vod__c,
        channel_vod__c,
        last_activity_datetime_vod__c,
        ropu_nm,
        ciclo,
        team
    from
        sch_raw_data.stg_mccp_prod_plan
    where
        ropu_nm = 'SPAIN'
        and channel_vod__c in ('ES_F2F', 'ES_S2S') -- use IN for simpler predicate
        and ciclo != 'ES'
        and team like '%CSO%'
),

max_dates as (
    select
        max(modified_date) as fechaCargaAct,
        to_date(max(modified_date)) as maxfecha
    from
        sch_raw_data.stg_mccp_prod_plan
),

parsed as (
    select
        b.*,
        -- Use split_part once per part to optimize
        sp_dash_2 as year,
        sp_underscore_2 as terr,
        case
            when b.external_id_vod__c like '%PLUS%' then left(sp_underscore_2, 14)
            else left(sp_underscore_2, 9)
        end as team_adjusted,
        right(sp_underscore_1, 2) as ciclo_calc
    from (
        select
            *,
            split_part(external_id_vod__c, '-', 2) as sp_dash_2,
            split_part(external_id_vod__c, '_', 2) as sp_underscore_2,
            split_part(external_id_vod__c, '_', 1) as sp_underscore_1
        from base
    ) b
),

cycle_dates as (
    select
        p.*,
        -- Derive yearciclo by concatenating year and last char of sp_underscore_1
        year || right(sp_underscore_1, 1) as yearciclo,
        -- Compute firstdaycicle and lastdaycicle as dates to avoid redundant to_date conversion
        case
            when right(sp_underscore_1, 2) = 'C1' then to_date(year || '-01-01', 'YYYY-MM-DD')
            when right(sp_underscore_1, 2) = 'C2' then to_date(year || '-05-01', 'YYYY-MM-DD')
            when right(sp_underscore_1, 2) = 'C3' then to_date(year || '-09-01', 'YYYY-MM-DD')
        end as firstdaycicle,
        case
            when right(sp_underscore_1, 2) = 'C1' then to_date(year || '-04-30', 'YYYY-MM-DD')
            when right(sp_underscore_1, 2) = 'C2' then to_date(year || '-08-31', 'YYYY-MM-DD')
            when right(sp_underscore_1, 2) = 'C3' then to_date(year || '-12-31', 'YYYY-MM-DD')
        end as lastdaycicle,
        right(sp_underscore_1, 2) as ciclo -- override ciclo with calculation for consistency
    from parsed p
)
    select
        c.external_id_vod__c,
        max_dates.fechaCargaAct,
        max_dates.maxfecha,
        c.year,
        c.terr,
        c.team_adjusted as team,
        c.ciclo,
        c.yearciclo,
        to_char(c.firstdaycicle, 'YYYY-MM-DD') as firstdaycicle,
        to_char(c.lastdaycicle, 'YYYY-MM-DD') as lastdaycicle,
        -- Calculate days_to_prorrate: use greatest for simpler logic
        case
            when c.lastdaycicle > max_dates.fechaCargaAct then datediff(day, max_dates.fechaCargaAct, c.lastdaycicle)
            else 1
        end as days_to_prorrate,
        -- Calculate working_days_to_prorrate: weekdays between firstdaycicle and fechaCargaAct
        case
            when c.lastdaycicle > max_dates.fechaCargaAct then
                datediff(week, c.firstdaycicle, max_dates.fechaCargaAct) * 5 +
                (datediff(day, c.firstdaycicle, max_dates.fechaCargaAct) - datediff(week, c.firstdaycicle, max_dates.fechaCargaAct) * 7)
            else 1
        end as working_days_to_prorrate,
        -- working_days_ciclo: weekdays between firstdaycicle and lastdaycicle
        datediff(week, c.firstdaycicle, c.lastdaycicle) * 5 +
        (datediff(day, c.firstdaycicle, c.lastdaycicle) - datediff(week, c.firstdaycicle, c.lastdaycicle) * 7) as working_days_ciclo,
        c.product_name_vod__c,
        sum(c.product_activity_goal_vod__c) AS product_activity_goal_vod__c,
        sum(c.product_actual_activity_vod__c) as product_actual_activity_vod__c,
        sum(c.product_interactions_actual_vod__c) as product_interactions_actual_vod__c,
        sum(
            case
                when c.product_attainment_vod__c >= 80 and c.channel_vod__c in ('ES_F2F', 'ES_S2S') and c.last_activity_datetime_vod__c is not null then 1
                else 0
            end
        ) as FC,
        sum(
            case
                when c.product_attainment_vod__c <= 79 and c.channel_vod__c in ('ES_F2F', 'ES_S2S') and c.last_activity_datetime_vod__c is not null then 1
                else 0
            end
        ) as noFC,
        max(c.systemmodstamp) as systemmodstamp,
        -- calculated prorated goals based on working days to prorrate
        case
            when c.lastdaycicle > max_dates.fechaCargaAct then sum(c.product_activity_goal_vod__c) * working_days_to_prorrate / working_days_ciclo
            else sum(c.product_activity_goal_vod__c)
        end as product_activity_goal_vod__c_workingprorrate,
        case
            when c.lastdaycicle > max_dates.fechaCargaAct then round(sum(c.product_activity_goal_vod__c) * working_days_to_prorrate / working_days_ciclo)
            else round(sum(c.product_activity_goal_vod__c))
        end as product_activity_goal_vod__c_workingprorrate_round
    from
        cycle_dates c
        cross join max_dates  -- cross join once to avoid redundant calculations
    group by
        c.external_id_vod__c,
        max_dates.fechaCargaAct,
        max_dates.maxfecha,
        c.year,
        c.terr,
        c.team_adjusted,
        c.ciclo,
        c.yearciclo,
        c.firstdaycicle,
        c.lastdaycicle,
        c.product_name_vod__c,
        working_days_to_prorrate,
        working_days_ciclo

