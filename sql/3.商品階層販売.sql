WITH 
-- 販売計画データの基本準備
sales_plan_base AS (
  SELECT
    sp.channel_group_id,
    sp.sku_id,
    sp.target_month_start_date_jst,
    sp.quantity as sales_qunatity,
    -- SKUマスタから商品属性を取得
    ws.category_2,
    IFNULL(ws.raw_materials, 'なし') as raw_materials,
    IFNULL(ws.launch_year_season, 'なし') as launch_year_season,
    IFNULL(ws.gender, 'なし') as gender,
    IFNULL(ws.style, 'なし') as style,
    IFNULL(ws.color, 'なし') as color,
    CAST(ws.suggested_retail_price AS INT64) AS price,
  FROM `tential-data-prd.warehouse_hevo_scm_master.warehouse_sku_monthly_sales_plans` sp
  LEFT JOIN `tential-data-prd.mart_scm_master.skus` sku
    ON sp.sku_id = sku._id
  LEFT JOIN `tential-data-prd.warehouse_analytics_master.warehouse_skus` ws
    ON sku.code = ws.sku_code
  WHERE 
    sp.__hevo__marked_deleted = FALSE
    AND sp.target_month_start_date_jst BETWEEN '2025-09-01' AND '2026-02-28'
    AND ws.category_2 IN ('SLEEPパジャマ', 'SLEEPアクセサリー', 'SLEEPアパレル')
    AND sp.channel_group_id = '65a78415cb2e6a5ea86376b1'
),

-- 階層別に計画数量を集計
hierarchical_plan AS (
  SELECT 
    category_2,
    raw_materials,
    launch_year_season,
    gender,
    style,
    color,
　　FORMAT_DATE('%Y/%m/%d', DATE_TRUNC(target_month_start_date_jst, MONTH)) as year_month,
    SUM(sales_qunatity * price) as planned_amount
  FROM sales_plan_base
  GROUP BY GROUPING SETS (
    -- レベル1: カテゴリー
    (category_2, target_month_start_date_jst),
    -- レベル2: 素材
    (category_2, raw_materials, target_month_start_date_jst),
    -- レベル3: シーズン
    (category_2, raw_materials, launch_year_season, target_month_start_date_jst),
    -- レベル4: 性別
    (category_2, raw_materials, launch_year_season, gender, target_month_start_date_jst),
    -- レベル5: スタイル
    (category_2, raw_materials, launch_year_season, gender, style, target_month_start_date_jst),
    -- レベル6: カラー
    (category_2, raw_materials, launch_year_season, gender, style, color, target_month_start_date_jst)
  )
),

-- parent_keyとcurrent_keyを追加して親階層の合計を取得
with_keys AS (
  SELECT
    *,
    -- parent_key: 親階層を識別するキー
    CASE 
      WHEN raw_materials IS NULL THEN ''  -- カテゴリレベルの親は空
      WHEN launch_year_season IS NULL THEN category_2
      WHEN gender IS NULL THEN CONCAT(category_2, '_', IFNULL(raw_materials, ''))
      WHEN style IS NULL THEN CONCAT(category_2, '_', IFNULL(raw_materials, ''), '_', IFNULL(launch_year_season, ''))
      WHEN color IS NULL THEN CONCAT(category_2, '_', IFNULL(raw_materials, ''), '_', IFNULL(launch_year_season, ''), '_', IFNULL(gender, ''))
      ELSE CONCAT(category_2, '_', IFNULL(raw_materials, ''), '_', IFNULL(launch_year_season, ''), '_', IFNULL(gender, ''), '_', IFNULL(style, ''))
    END as parent_key,
    
    -- current_key: 現在の階層のキー
    CASE 
      WHEN raw_materials IS NULL THEN category_2
      WHEN launch_year_season IS NULL THEN CONCAT(category_2, '_', IFNULL(raw_materials, ''))
      WHEN gender IS NULL THEN CONCAT(category_2, '_', IFNULL(raw_materials, ''), '_', IFNULL(launch_year_season, ''))
      WHEN style IS NULL THEN CONCAT(category_2, '_', IFNULL(raw_materials, ''), '_', IFNULL(launch_year_season, ''), '_', IFNULL(gender, ''))
      WHEN color IS NULL THEN CONCAT(category_2, '_', IFNULL(raw_materials, ''), '_', IFNULL(launch_year_season, ''), '_', IFNULL(gender, ''), '_', IFNULL(style, ''))
      ELSE CONCAT(category_2, '_', IFNULL(raw_materials, ''), '_', IFNULL(launch_year_season, ''), '_', IFNULL(gender, ''), '_', IFNULL(style, ''), '_', IFNULL(color, ''))
    END as current_key
  FROM hierarchical_plan
),

-- 親階層の合計を計算
parent_totals AS (
  SELECT
    wk.*,
    -- 親階層の合計金額
    SUM(wk2.planned_amount) as parent_total
  FROM with_keys wk
  LEFT JOIN with_keys wk2
    ON wk.year_month = wk2.year_month
    AND wk.parent_key = wk2.current_key
  GROUP BY 
    wk.category_2, wk.raw_materials, wk.launch_year_season, 
    wk.gender, wk.style, wk.color, wk.year_month, 
    wk.planned_amount, wk.parent_key, wk.current_key
)

-- 最終結果：計画比率を計算
SELECT 
  category_2,
  IFNULL(raw_materials, '') as raw_materials,
  IFNULL(launch_year_season, '') as launch_year_season,
  IFNULL(gender, '') as gender,
  IFNULL(style, '') as style,
  IFNULL(color, '') as color,
  year_month,
  
  -- 階層レベル
  CASE 
    WHEN raw_materials IS NULL THEN 1
    WHEN launch_year_season IS NULL THEN 2
    WHEN gender IS NULL THEN 3
    WHEN style IS NULL THEN 4
    WHEN color IS NULL THEN 5
    ELSE 6
  END as hierarchy_level,
  
  -- キー情報
  parent_key,
  current_key,
  
  -- 計画比率（0-1の小数として出力）
  CASE
    WHEN raw_materials IS NULL THEN 
      -- カテゴリレベル：実際の金額比率を使用
      ROUND(planned_amount / SUM(CASE WHEN raw_materials IS NULL THEN planned_amount ELSE 0 END) OVER (PARTITION BY year_month), 4)
    ELSE
      -- その他：親階層に対する比率
      ROUND(planned_amount / NULLIF(parent_total, 0), 4)
  END as plan_ratio,

  
  -- デバッグ用：計画金額
  planned_amount,

FROM parent_totals
WHERE planned_amount > 0  -- 計画がある項目のみ

ORDER BY 
  year_month,
  category_2,
  CASE WHEN raw_materials = 'なし' OR raw_materials IS NULL THEN 'ZZZZ' ELSE raw_materials END,
  CASE WHEN launch_year_season = 'なし' OR launch_year_season IS NULL THEN 'ZZZZ' ELSE launch_year_season END,
  CASE WHEN gender = 'なし' OR gender IS NULL THEN 'ZZZZ' ELSE gender END,
  CASE WHEN style = 'なし' OR style IS NULL THEN 'ZZZZ' ELSE style END,
  CASE WHEN color = 'なし' OR color IS NULL THEN 'ZZZZ' ELSE color END