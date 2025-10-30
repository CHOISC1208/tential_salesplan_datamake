with items as(
   SELECT
    code items_code,
    name items_name,
    producing_countries,
    carton_quantity,
    min_order_quantity,
    _id  items_id,
    embedded_supplier.name supplier_name 
    FROM `tential-data-prd.mart_scm_master.items` 
  Where 
    usage_type = "merchandise"
    and brand_id = "63ecaf4a59023c7e45f05a9b" 

  )


SELECT
  skus.name name,
  skus.code code,
  items.items_name,
  items.items_code,
  skus.embedded_season.name season_name,
  skus.continue_status,
  items.producing_countries,
  skus.jan_code,
  skus.manufacturer_model_number,
  skus.medical_device_registration_number,
  items.carton_quantity,
  items.min_order_quantity,
  skus.suggested_retail_price,
  skus.cost_price,
  skus.package_code,
  supplier_name,
  skus.color_name_en color_name,
  skus.size.name size_name,
  CONCAT(items.items_name, "/", skus.color_name_jp) AS style_color
FROM `tential-data-prd.mart_scm_master.skus` skus
join　items on item_id = items_id
WHERE
  name not like "%流通加工%"
  and name not like "%評価損%"
  and name not like "%マンチェス%"
  and name not like "%洗濯ネット%"
  and name not like "%オリジナルシューズケース%"
  and name not like "%選手提供用別注%"
  and name not like "%Perfume%"
  and name not like "%松本湯限定%"
  and name not like "%senonmask%"
  and name not like "%FABRIC MIST%"
  and name not like "%伊勢丹%"
  and name not like "%OEM%"
  and name not like "%22AW%"
  and name not like "%LTD%"
  and name not like "%PUMPS INSOLE%"
  and name not like "%別注%"
  and name not like "%ストライカー%"
  and name not like "%アシトレ%"
  and name not like "%Ashi-Tore%"
  --and continue_status in("continued","discontinued")

order by 2