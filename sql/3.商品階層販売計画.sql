-- ③ 商品階層「販売計画」— month_start を列にしてから集計する版
WITH sales_plan_base AS (
  SELECT
    sp.channel_group_id,
    sp.sku_id,
    sp.target_month_start_date_jst,
    DATE_TRUNC(sp.target_month_start_date_jst, MONTH) AS month_start,  -- ★ 先に列化
    sp.quantity AS sales_quantity,
    ws.category_2,
    IFNULL(ws.raw_materials,'なし')       AS raw_materials,
    IFNULL(ws.launch_year_season,'なし')  AS launch_year_season,
    IFNULL(ws.gender,'なし')              AS gender,
    IFNULL(ws.style,'なし')               AS style,
    IFNULL(ws.color,'なし')               AS color,
    SAFE_CAST(
      REGEXP_REPLACE(
        REPLACE(REGEXP_REPLACE(CAST(ws.suggested_retail_price AS STRING), r'[^0-9.\-]', ''), ',', ''),
        r'^\s*$', NULL
      ) AS FLOAT64
    ) AS price
  FROM `tential-data-prd.warehouse_hevo_scm_master.warehouse_sku_monthly_sales_plans` sp
  LEFT JOIN `tential-data-prd.mart_scm_master.skus` sku
    ON sp.sku_id = sku._id
  LEFT JOIN `tential-data-prd.warehouse_analytics_master.warehouse_skus` ws
    ON sku.code = ws.sku_code
  WHERE sp.__hevo__marked_deleted = FALSE
    AND sp.target_month_start_date_jst BETWEEN DATE '2025-09-01' AND DATE '2026-02-28'
    AND ws.category_2 IN ('SLEEPパジャマ','SLEEPアクセサリー','SLEEPアパレル')
    AND sp.channel_group_id = '65a78415cb2e6a5ea86376b1'
),

-- month_start（列）だけを GROUP BY/GROUPING SETS に使う
hierarchical_plan AS (
  SELECT 
    category_2, raw_materials, launch_year_season, gender, style, color,
    month_start,
    SUM(sales_quantity * IFNULL(price,0)) AS planned_amount
  FROM sales_plan_base
  GROUP BY GROUPING SETS (
    (category_2, month_start),
    (category_2, raw_materials, month_start),
    (category_2, raw_materials, launch_year_season, month_start),
    (category_2, raw_materials, launch_year_season, gender, month_start),
    (category_2, raw_materials, launch_year_season, gender, style, month_start),
    (category_2, raw_materials, launch_year_season, gender, style, color, month_start)
  )
),

with_keys AS (
  SELECT
    *,
    CASE 
      WHEN raw_materials IS NULL THEN ''
      WHEN launch_year_season IS NULL THEN category_2
      WHEN gender IS NULL THEN CONCAT(category_2,'_',IFNULL(raw_materials,''))
      WHEN style IS NULL THEN CONCAT(category_2,'_',IFNULL(raw_materials,''),'_',IFNULL(launch_year_season,''))
      WHEN color IS NULL THEN CONCAT(category_2,'_',IFNULL(raw_materials,''),'_',IFNULL(launch_year_season,''),'_',IFNULL(gender,''))
      ELSE CONCAT(category_2,'_',IFNULL(raw_materials,''),'_',IFNULL(launch_year_season,''),'_',IFNULL(gender,''),'_',IFNULL(style,''))
    END AS parent_key,
    CASE 
      WHEN raw_materials IS NULL THEN category_2
      WHEN launch_year_season IS NULL THEN CONCAT(category_2,'_',IFNULL(raw_materials,''))
      WHEN gender IS NULL THEN CONCAT(category_2,'_',IFNULL(raw_materials,''),'_',IFNULL(launch_year_season,''))
      WHEN style IS NULL THEN CONCAT(category_2,'_',IFNULL(raw_materials,''),'_',IFNULL(launch_year_season,''),'_',IFNULL(gender,''))
      WHEN color IS NULL THEN CONCAT(category_2,'_',IFNULL(raw_materials,''),'_',IFNULL(launch_year_season,''),'_',IFNULL(gender,''),'_',IFNULL(style,''))
      ELSE CONCAT(category_2,'_',IFNULL(raw_materials,''),'_',IFNULL(launch_year_season,''),'_',IFNULL(gender,''),'_',IFNULL(style,''),'_',IFNULL(color,''))
    END AS current_key
  FROM hierarchical_plan
),

parent_totals AS (
  SELECT
    wk.*,
    SUM(wk2.planned_amount) AS parent_total
  FROM with_keys wk
  LEFT JOIN with_keys wk2
    ON wk.month_start = wk2.month_start
   AND wk.parent_key  = wk2.current_key
  GROUP BY
    wk.category_2, wk.raw_materials, wk.launch_year_season, wk.gender, wk.style, wk.color,
    wk.month_start, wk.planned_amount, wk.parent_key, wk.current_key
)

SELECT 
  category_2,
  IFNULL(raw_materials,'')      AS raw_materials,
  IFNULL(launch_year_season,'') AS launch_year_season,
  IFNULL(gender,'')             AS gender,
  IFNULL(style,'')              AS style,
  IFNULL(color,'')              AS color,
  FORMAT_DATE('%Y/%m/%d', month_start) AS year_month,                 -- ★ 最後に整形

  CASE 
    WHEN raw_materials IS NULL THEN 1
    WHEN launch_year_season IS NULL THEN 2
    WHEN gender IS NULL THEN 3
    WHEN style IS NULL THEN 4
    WHEN color IS NULL THEN 5
    ELSE 6
  END AS hierarchy_level,

  parent_key,
  current_key,

  CASE
    WHEN raw_materials IS NULL THEN 
      ROUND(
        planned_amount
        / NULLIF(
            SUM(CASE WHEN raw_materials IS NULL THEN planned_amount ELSE 0 END)
              OVER (PARTITION BY month_start),
            0
          ),
        4
      )
    ELSE
      ROUND(planned_amount / NULLIF(parent_total,0), 4)
  END AS plan_ratio,

  planned_amount
FROM parent_totals
WHERE planned_amount > 0
ORDER BY 
  month_start,
  category_2,
  CASE WHEN raw_materials='なし' OR raw_materials IS NULL THEN 'ZZZZ' ELSE raw_materials END,
  CASE WHEN launch_year_season='なし' OR launch_year_season IS NULL THEN 'ZZZZ' ELSE launch_year_season END,
  CASE WHEN gender='なし' OR gender IS NULL THEN 'ZZZZ' ELSE gender END,
  CASE WHEN style='なし' OR style IS NULL THEN 'ZZZZ' ELSE style END,
  CASE WHEN color='なし' OR color IS NULL THEN 'ZZZZ' ELSE color END;