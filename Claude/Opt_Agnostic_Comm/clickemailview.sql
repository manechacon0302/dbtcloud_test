{{ config(
    materialized='table',
    sort='prod_nm',
    dist='terr_id'
) }}

WITH date_extracted AS (
    -- OPTIMIZATION: Extract date components once in CTE to avoid repeated substring operations
    SELECT
        main.prod_nm,
        main.terr_id,
        main.sent_email_nm,
        main.status,
        main.opened,
        main.clicked,
        main.open_count,
        main.click_count,
        main.email_sent_dt,
        DATE_TRUNC('month', main.email_sent_dt::timestamp)::date AS email_month,
        EXTRACT(YEAR FROM main.email_sent_dt::timestamp)::varchar AS year_val,
        EXTRACT(MONTH FROM main.email_sent_dt::timestamp)::varchar AS month_val,
        -- OPTIMIZATION: Pre-calculate quarter assignment to avoid repeated CASE statements
        CASE
            WHEN EXTRACT(MONTH FROM main.email_sent_dt::timestamp) IN (1, 2, 3, 4) THEN 'C1'
            WHEN EXTRACT(MONTH FROM main.email_sent_dt::timestamp) IN (5, 6, 7, 8) THEN 'C2'
            WHEN EXTRACT(MONTH FROM main.email_sent_dt::timestamp) IN (9, 10, 11, 12) THEN 'C3'
            ELSE NULL
        END AS ciclo
    FROM
        sch_data_external_mv.f_sent_email_atvy main
        INNER JOIN sch_user_auth.rls_user_auth_mast usr
            ON CASE
                WHEN LENGTH(usr.opu::text) = 0 THEN 'X'
                ELSE main.cntry_cd::text
            END = CASE
                WHEN LENGTH(usr.opu::text) = 0 THEN 'X'
                ELSE usr.opu::text
            END
    WHERE
        usr.uname::bpchar = CURRENT_USER
        -- OPTIMIZATION: Filter for CSO territories early to reduce data processed
        AND main.terr_id LIKE '%CSO%'
),
status_analysis AS (
    -- OPTIMIZATION: Calculate status flags once to avoid repeated pattern matching
    SELECT
        prod_nm,
        terr_id,
        sent_email_nm,
        opened,
        clicked,
        open_count,
        click_count,
        email_sent_dt,
        year_val,
        month_val,
        email_month,
        ciclo,
        CONCAT(year_val, '0', month_val) AS fecha,
        CASE
            WHEN status LIKE '%ailed%' THEN 1
            ELSE 0
        END AS is_failed
    FROM
        date_extracted
)
SELECT
    prod_nm,
    terr_id,
    COUNT(sent_email_nm) AS all_email,
    SUM(is_failed) AS failed_email,
    -- OPTIMIZATION: Calculate delivered as count minus failed to avoid duplicate aggregation
    COUNT(sent_email_nm) - SUM(is_failed) AS delivered_email,
    SUM(opened) AS open_unique,
    SUM(clicked) AS clicked_unique,
    SUM(open_count) AS sum_open_count,
    SUM(click_count) AS sum_clicked,
    month_val AS mes,
    year_val AS year,
    fecha,
    ciclo
FROM
    status_analysis
GROUP BY
    prod_nm,
    terr_id,
    month_val,
    year_val,
    fecha,
    ciclo
ORDER BY
    prod_nm,
    terr_id,
    year_val,
    month_val;