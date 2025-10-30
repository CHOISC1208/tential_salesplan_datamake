-- 商品階層「販売計画」— 最下位レベルのみ（leaf集計）
WITH sales_plan_base AS (
  SELECT
    sp.channel_group_id,
    sp.sku_id,
    sp.target_month_start_date_jst,
    DATE_TRUNC(sp.target_month_start_date_jst, MONTH) AS month_start,  -- 月初列化
    sp.quantity AS sales_quantity,                                      -- ← スペル修正
    ws.category_2,
    IFNULL(ws.raw_materials, 'なし')      AS raw_materials,
    IFNULL(ws.launch_year_season, 'なし') AS launch_year_season,
    IFNULL(ws.gender, 'なし')             AS gender,
    IFNULL(ws.style, 'なし')              AS style,
    IFNULL(ws.color, 'なし')              AS color,
    -- 価格は整数で良ければこのまま。頑強にするなら SAFE_CAST + 正規化へ変更可
    CAST(ws.suggested_retail_price AS INT64) AS price
  FROM `tential-data-prd.warehouse_hevo_scm_master.warehouse_sku_monthly_sales_plans` sp
  LEFT JOIN `tential-data-prd.mart_scm_master.skus` sku
    ON sp.sku_id = sku._id
  LEFT JOIN `tential-data-prd.warehouse_analytics_master.warehouse_skus` ws
    ON sku.code = ws.sku_code
  WHERE
    sp.__hevo__marked_deleted = FALSE
    AND sp.target_month_start_date_jst BETWEEN DATE '2025-09-01' AND DATE '2026-02-28'
    AND ws.category_2 IN ('SLEEPパジャマ', 'SLEEPアクセサリー', 'SLEEPアパレル')
    AND sp.channel_group_id = '65a78415cb2e6a5ea86376b1'
),

leaf AS (
  SELECT
    category_2,
    raw_materials,
    launch_year_season,
    gender,
    style,
    color,
    month_start,
    SUM(sales_quantity * IFNULL(price, 0)) AS planned_amount
  FROM sales_plan_base
  GROUP BY
    category_2, raw_materials, launch_year_season, gender, style, color, month_start
)

SELECT
  category_2,
  raw_materials,
  launch_year_season,
  gender,
  style,
  color,
  FORMAT_DATE('%Y/%m/%d', month_start) AS year_month,
  6 AS hierarchy_level,  -- すべて葉なので固定

  -- 親キー（color を除いた1階層上）
  CONCAT(category_2, '_', raw_materials, '_', launch_year_season, '_', gender, '_', style) AS parent_key,
  -- 現在の階層キー（葉）
  CONCAT(category_2, '_', raw_materials, '_', launch_year_season, '_', gender, '_', style, '_', color) AS current_key,

  planned_amount
FROM leaf
WHERE planned_amount > 0
ORDER BY
  month_start,
  category_2,
  CASE WHEN raw_materials = 'なし' OR raw_materials IS NULL THEN 'ZZZZ' ELSE raw_materials END,
  CASE WHEN launch_year_season = 'なし' OR launch_year_season IS NULL THEN 'ZZZZ' ELSE launch_year_season END,
  CASE WHEN gender = 'なし' OR gender IS NULL THEN 'ZZZZ' ELSE gender END,
  CASE WHEN style = 'なし' OR style IS NULL THEN 'ZZZZ' ELSE style END,
  CASE WHEN color = 'なし' OR color IS NULL THEN 'ZZZZ' ELSE color END;
