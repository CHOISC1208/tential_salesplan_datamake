-- 実績：2024-01-01〜今日（JST） 
-- 計画：当月月初〜12ヶ月先の月末（JST） 
-- 当月のみ、残日数で planned_amount を按分 → remain_day / remain_amount / total_amount を算出


WITH params AS (
  SELECT
    DATE '2024-01-01' AS actual_start_date,
    CURRENT_DATE("Asia/Tokyo") AS actual_end_date,
    DATE_TRUNC(CURRENT_DATE("Asia/Tokyo"), MONTH) AS plan_start_month,
    DATE_SUB(DATE_ADD(DATE_TRUNC(CURRENT_DATE("Asia/Tokyo"), MONTH), INTERVAL 12 MONTH), INTERVAL 1 DAY) AS plan_end_date,
    ['SLEEPパジャマ','SLEEPアクセサリー','SLEEPアパレル'] AS target_categories,
    'online' AS actual_channel_name,
    '65a78415cb2e6a5ea86376b1' AS plan_channel_id
),

cur AS (
  SELECT
    CURRENT_DATE("Asia/Tokyo") AS today,
    DATE_TRUNC(CURRENT_DATE("Asia/Tokyo"), MONTH) AS cur_month_start,
    LAST_DAY(CURRENT_DATE("Asia/Tokyo")) AS cur_month_end,
    DATE_DIFF(LAST_DAY(CURRENT_DATE("Asia/Tokyo")),
              DATE_TRUNC(CURRENT_DATE("Asia/Tokyo"), MONTH), DAY) + 1 AS cur_days_in_month,
    DATE_DIFF(LAST_DAY(CURRENT_DATE("Asia/Tokyo")),
              CURRENT_DATE("Asia/Tokyo"), DAY) AS cur_remaining_days
),

actual_base AS (
  SELECT
    ws.category_2,
    IFNULL(ws.raw_materials, 'なし')      AS raw_materials,
    IFNULL(ws.launch_year_season, 'なし') AS launch_year_season,
    IFNULL(ws.gender, 'なし')             AS gender,
    IFNULL(ws.style, 'なし')              AS style,
    IFNULL(ws.color, 'なし')              AS color,
    DATE_TRUNC(s.order_date, MONTH)       AS month_date,
    s.sales_quantity,
    s.total_regular_price AS sales_amount
  FROM `tential-data-prd.mart_analytics.all_sales` AS s
  LEFT JOIN `tential-data-prd.warehouse_analytics_master.warehouse_skus` AS ws
    ON s.sku_code = ws.sku_code
  CROSS JOIN params p
  WHERE
    s.order_date BETWEEN p.actual_start_date AND p.actual_end_date
    AND ws.category_2 IN UNNEST(p.target_categories)
    AND s.source_name = p.actual_channel_name
),
actual_leaf AS (
  SELECT
    category_2, raw_materials, launch_year_season, gender, style, color, month_date,
    SUM(sales_quantity) AS sales_quantity,
    SUM(sales_amount)   AS sales_amount
  FROM actual_base
  GROUP BY category_2, raw_materials, launch_year_season, gender, style, color, month_date
),

plan_base AS (
  SELECT
    ws.category_2,
    IFNULL(ws.raw_materials, 'なし')      AS raw_materials,
    IFNULL(ws.launch_year_season, 'なし') AS launch_year_season,
    IFNULL(ws.gender, 'なし')             AS gender,
    IFNULL(ws.style, 'なし')              AS style,
    IFNULL(ws.color, 'なし')              AS color,
    DATE_TRUNC(sp.target_month_start_date_jst, MONTH) AS month_date,
    sp.quantity AS plan_qty,
    SAFE_CAST(REGEXP_REPLACE(CAST(ws.suggested_retail_price AS STRING), r'[^0-9.-]', '') AS INT64) AS price_int
  FROM `tential-data-prd.warehouse_hevo_scm_master.warehouse_sku_monthly_sales_plans` sp
  LEFT JOIN `tential-data-prd.mart_scm_master.skus` sku
    ON sp.sku_id = sku._id
  LEFT JOIN `tential-data-prd.warehouse_analytics_master.warehouse_skus` ws
    ON sku.code = ws.sku_code
  CROSS JOIN params p
  WHERE
    sp.__hevo__marked_deleted = FALSE
    AND sp.target_month_start_date_jst BETWEEN p.plan_start_month AND p.plan_end_date
    AND ws.category_2 IN UNNEST(p.target_categories)
    AND sp.channel_group_id = p.plan_channel_id
),
plan_leaf AS (
  SELECT
    category_2, raw_materials, launch_year_season, gender, style, color, month_date,
    SUM(plan_qty * IFNULL(price_int, 0)) AS planned_amount
  FROM plan_base
  GROUP BY category_2, raw_materials, launch_year_season, gender, style, color, month_date
),

keys AS (
  SELECT DISTINCT
    category_2, raw_materials, launch_year_season, gender, style, color, month_date
  FROM actual_leaf
  UNION DISTINCT
  SELECT
    category_2, raw_materials, launch_year_season, gender, style, color, month_date
  FROM plan_leaf
)

SELECT
  k.category_2,
  k.raw_materials,
  k.launch_year_season,
  k.gender,
  k.style,
  k.color,
  FORMAT_DATE('%Y/%m/%d', k.month_date) AS year_month,
  6 AS hierarchy_level,
  CONCAT(k.category_2, '_', k.raw_materials, '_', k.launch_year_season, '_', k.gender, '_', k.style) AS parent_key,
  CONCAT(k.category_2, '_', k.raw_materials, '_', k.launch_year_season, '_', k.gender, '_', k.style, '_', k.color) AS current_key,

  IFNULL(a.sales_quantity, 0) AS sales_quantity,
  IFNULL(a.sales_amount, 0)   AS sales_amount,
  IFNULL(p.planned_amount, 0) AS planned_amount,

  -- 当月のみ残日数・残額を計算。その他は0。
  CASE WHEN k.month_date = c.cur_month_start THEN c.cur_remaining_days ELSE 0 END AS remain_day,
  CASE WHEN k.month_date = c.cur_month_start
       THEN IFNULL(p.planned_amount, 0) * SAFE_DIVIDE(c.cur_remaining_days, c.cur_days_in_month)
       ELSE 0
  END AS remain_amount,

  -- 合計：sales_amount + remain_amount（常に数値）
  IFNULL(a.sales_amount, 0) +
  CASE WHEN k.month_date = c.cur_month_start
       THEN IFNULL(p.planned_amount, 0) * SAFE_DIVIDE(c.cur_remaining_days, c.cur_days_in_month)
       ELSE 0
  END AS total_amount

FROM keys k
LEFT JOIN actual_leaf a USING (category_2, raw_materials, launch_year_season, gender, style, color, month_date)
LEFT JOIN plan_leaf   p USING (category_2, raw_materials, launch_year_season, gender, style, color, month_date)
CROSS JOIN cur c
ORDER BY
  k.month_date,
  k.category_2,
  CASE WHEN k.raw_materials = 'なし' OR k.raw_materials IS NULL THEN 'ZZZZ' ELSE k.raw_materials END,
  CASE WHEN k.launch_year_season = 'なし' OR k.launch_year_season IS NULL THEN 'ZZZZ' ELSE k.launch_year_season END,
  CASE WHEN k.gender = 'なし' OR k.gender IS NULL THEN 'ZZZZ' ELSE k.gender END,
  CASE WHEN k.style = 'なし' OR k.style IS NULL THEN 'ZZZZ' ELSE k.style END,
  CASE WHEN k.color = 'なし' OR k.color IS NULL THEN 'ZZZZ' ELSE k.color END;
