WITH base_data AS (
  SELECT
    sku__category_2 AS category_2,
    IFNULL(sku__raw_materials,'なし') AS raw_materials,
    IFNULL(sku__launch_year_season,'なし') AS launch_year_season,
    IFNULL(sku__gender,'なし') AS gender,
    IFNULL(sku__style,'なし') AS style,
    IFNULL(sku__color,'なし') AS color,
    FORMAT_DATE('%Y/%m/%d', DATE_TRUNC(order_date, MONTH)) AS year_month,
    sales_quantity,
    total_regular_price AS sales_amount
  FROM `tential-data-prd.mart_analytics.all_sales`
  WHERE sku__category_2 IN ("SLEEPパジャマ","SLEEPアクセサリー","SLEEPアパレル")
    AND order_date >= DATE '2024-01-01'
    AND source_name = "online"
),
grouped_data AS (
  SELECT 
    category_2, raw_materials, launch_year_season, gender, style, color,
    year_month,
    SUM(sales_quantity) AS sales_quantity,
    SUM(sales_amount)   AS sales_amount
  FROM base_data
  GROUP BY GROUPING SETS (
    (category_2, year_month),
    (category_2, raw_materials, year_month),
    (category_2, raw_materials, launch_year_season, year_month),
    (category_2, raw_materials, launch_year_season, gender, year_month),
    (category_2, raw_materials, launch_year_season, gender, style, year_month),
    (category_2, raw_materials, launch_year_season, gender, style, color, year_month)
  )
)
SELECT 
  category_2,
  IFNULL(raw_materials,'')     AS raw_materials,
  IFNULL(launch_year_season,'') AS launch_year_season,
  IFNULL(gender,'')            AS gender,
  IFNULL(style,'')             AS style,
  IFNULL(color,'')             AS color,
  year_month,
  CASE 
    WHEN raw_materials IS NULL THEN 1
    WHEN launch_year_season IS NULL THEN 2
    WHEN gender IS NULL THEN 3
    WHEN style IS NULL THEN 4
    WHEN color IS NULL THEN 5
    ELSE 6
  END AS hierarchy_level,
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
  END AS current_key,
  sales_quantity,
  sales_amount
FROM grouped_data
ORDER BY 
  category_2,
  CASE WHEN raw_materials='なし' OR raw_materials IS NULL THEN 'ZZZZ' ELSE raw_materials END,
  CASE WHEN launch_year_season='なし' OR launch_year_season IS NULL THEN 'ZZZZ' ELSE launch_year_season END,
  CASE WHEN gender='なし' OR gender IS NULL THEN 'ZZZZ' ELSE gender END,
  CASE WHEN style='なし' OR style IS NULL THEN 'ZZZZ' ELSE style END,
  CASE WHEN color='なし' OR color IS NULL THEN 'ZZZZ' ELSE color END,
  year_month;