-- 商品階層「受注」— 最下位レベルのみ（leaf集計）
WITH base_data AS (
  SELECT
    sku__category_2 AS category_2,
    IFNULL(sku__raw_materials, 'なし')      AS raw_materials,
    IFNULL(sku__launch_year_season, 'なし') AS launch_year_season,
    IFNULL(sku__gender, 'なし')             AS gender,
    IFNULL(sku__style, 'なし')              AS style,
    IFNULL(sku__color, 'なし')              AS color,
    FORMAT_DATE('%Y/%m/%d', DATE_TRUNC(order_date, MONTH)) AS year_month,
    sales_quantity,
    total_regular_price AS sales_amount
  FROM `tential-data-prd.mart_analytics.all_sales`
  WHERE sku__category_2 IN ("SLEEPパジャマ","SLEEPアクセサリー","SLEEPアパレル")
    AND order_date >= DATE '2024-01-01'
    AND source_name = "online"
),

leaf AS (
  SELECT
    category_2, raw_materials, launch_year_season, gender, style, color, year_month,
    SUM(sales_quantity) AS sales_quantity,
    SUM(sales_amount)   AS sales_amount
  FROM base_data
  GROUP BY
    category_2, raw_materials, launch_year_season, gender, style, color, year_month
)

SELECT
  category_2,
  raw_materials,
  launch_year_season,
  gender,
  style,
  color,
  year_month,
  6 AS hierarchy_level,  -- すべて葉なので固定で 6
  -- 親キー（colorまで1階層上）
  CONCAT(category_2,'_',raw_materials,'_',launch_year_season,'_',gender,'_',style) AS parent_key,
  -- 現在の階層キー（葉）
  CONCAT(category_2,'_',raw_materials,'_',launch_year_season,'_',gender,'_',style,'_',color) AS current_key,
  sales_quantity,
  sales_amount
FROM leaf
ORDER BY
  category_2,
  CASE WHEN raw_materials='なし' OR raw_materials IS NULL THEN 'ZZZZ' ELSE raw_materials END,
  CASE WHEN launch_year_season='なし' OR launch_year_season IS NULL THEN 'ZZZZ' ELSE launch_year_season END,
  CASE WHEN gender='なし' OR gender IS NULL THEN 'ZZZZ' ELSE gender END,
  CASE WHEN style='なし' OR style IS NULL THEN 'ZZZZ' ELSE style END,
  CASE WHEN color='なし' OR color IS NULL THEN 'ZZZZ' ELSE color END,
  year_month;
