WITH src AS (
  SELECT
    category_2,
    IFNULL(raw_materials, 'なし') AS raw_materials,
    IFNULL(launch_year_season, 'なし') AS launch_year_season,
    IFNULL(gender, 'なし') AS gender,
    IFNULL(style, 'なし') AS style,
    IFNULL(color, 'なし') AS color,
    style_color,
    item_code,
    sku_code,
    SAFE_CAST(
      REGEXP_REPLACE(
        REPLACE(REGEXP_REPLACE(CAST(suggested_retail_price AS STRING), r'[^0-9.\-]', ''), ',', ''),
        r'^\s*$', NULL
      ) AS FLOAT64
    ) AS price_f64
  FROM `tential-data-prd.warehouse_analytics_master.warehouse_skus`
  WHERE category_2 IN ('SLEEPパジャマ','SLEEPアクセサリー','SLEEPアパレル')
    AND launch_year_season IN ("2024SS","2024FW","2025SS","2025FW","2026SS","2026FW")

  UNION ALL

  SELECT
    category_2,
    IFNULL(raw_materials, 'なし') AS raw_materials,
    IFNULL(launch_year_season, 'なし') AS launch_year_season,
    IFNULL(gender, 'なし') AS gender,
    IFNULL(style, 'なし') AS style,
    IFNULL(color, 'なし') AS color,
    style_color,
    item_code,
    sku_code,
    SAFE_CAST(
      REGEXP_REPLACE(
        REPLACE(REGEXP_REPLACE(CAST(suggested_retail_price AS STRING), r'[^0-9.\-]', ''), ',', ''),
        r'^\s*$', NULL
      ) AS FLOAT64
    ) AS price_f64
  FROM `tential-data-prd.warehouse_analytics_master.tmp_skus`
  WHERE category_1 = 'SLEEP'
    AND category_2 IS NOT NULL
    AND launch_year_season IN ("2026SS","2026FW")
),
base AS (
  SELECT
    category_2, raw_materials, launch_year_season, gender, style, color,
    ANY_VALUE(style_color) AS style_color,
    ANY_VALUE(item_code)   AS item_code,
    COUNT(DISTINCT sku_code) AS sku_count,
    AVG(price_f64) AS avg_price_f64
  FROM src
  GROUP BY 1,2,3,4,5,6
),
grouped_data AS (
  SELECT
    category_2, raw_materials, launch_year_season, gender, style, color,
    ANY_VALUE(style_color) AS style_color,
    SAFE_DIVIDE(
      SUM(IFNULL(avg_price_f64,0) * sku_count),
      NULLIF(SUM(CASE WHEN avg_price_f64 IS NULL THEN 0 ELSE sku_count END),0)
    ) AS weighted_avg_price,
    ANY_VALUE(item_code) AS item_code,
    SUM(sku_count) AS total_skus
  FROM base
  GROUP BY GROUPING SETS (
    (category_2),
    (category_2, raw_materials),
    (category_2, raw_materials, launch_year_season),
    (category_2, raw_materials, launch_year_season, gender),
    (category_2, raw_materials, launch_year_season, gender, style),
    (category_2, raw_materials, launch_year_season, gender, style, color)
  )
),
temp1 AS (
  SELECT 
    category_2,
    IFNULL(raw_materials,'') AS raw_materials,
    IFNULL(launch_year_season,'') AS launch_year_season,
    IFNULL(gender,'') AS gender,
    IFNULL(style,'') AS style,
    IFNULL(color,'') AS color,
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
    CASE WHEN color IS NOT NULL THEN IFNULL(style_color,'') ELSE '' END AS style_color,
    ROUND(IFNULL(weighted_avg_price,0)) AS suggested_retail_price,
    CASE WHEN color IS NOT NULL THEN IFNULL(item_code,'') ELSE '' END AS item_code,
    total_skus
  FROM grouped_data
)
SELECT *
FROM temp1
ORDER BY 
  category_2,
  CASE WHEN raw_materials='なし' OR raw_materials IS NULL THEN 'ZZZZ' ELSE raw_materials END,
  CASE WHEN launch_year_season='なし' OR launch_year_season IS NULL THEN 'ZZZZ' ELSE launch_year_season END,
  CASE WHEN gender='なし' OR gender IS NULL THEN 'ZZZZ' ELSE gender END,
  CASE WHEN style='なし' OR style IS NULL THEN 'ZZZZ' ELSE style END,
  CASE WHEN color='なし' OR color IS NULL THEN 'ZZZZ' ELSE color END;