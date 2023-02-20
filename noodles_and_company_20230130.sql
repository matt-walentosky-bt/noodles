
--Get minimum recomendation time for each user, date, item_name, item_id
WITH minimum_rec_gb as (
  SELECT 
    user_pseudo_id,
    event_date,
    (select value.string_value from unnest(event_params) where key = "basket_id") as basket_id,
    items.item_name,
    items.item_id,
    items.item_category,
    items.price as rec_price,
    items.item_category as item_category,
    items.item_list_name,
    MIN(event_timestamp) as rec_timestamp
  FROM `cool-arch-249617.analytics_207839834.events_*`, UNNEST(items) as items
  --Must specify web
  WHERE platform = 'WEB'
  --Pull from 2 days back to 32 days back
  AND _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY))
  AND FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
  AND (SELECT value.string_value from UNNEST(event_params) WHERE key = 'page_location') LIKE '%my-bag%'
  AND event_name = 'view_item_list'
  GROUP BY 1,2,3,4,5,6,7,8,9
),

--Get minimum add to cart time for each user, date, item_name, item_id
min_atc_gb as (
  SELECT 
    user_pseudo_id,
    event_date,
    items.item_name,
    items.item_id,
    items.price as atc_price,
    MIN(event_timestamp) as atc_timestamp
  FROM `cool-arch-249617.analytics_207839834.events_*`, UNNEST(items) as items
  WHERE platform = 'WEB'
  AND _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY))
  AND FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
  AND event_name = 'add_to_cart'
  GROUP BY 1,2,3,4, 5
),

purchases as (
  SELECT 
    user_pseudo_id,
    event_date,
    items.item_name,
    items.item_id,
    items.price as purchase_price,
    MIN(event_timestamp) as purchase_timestamp
  FROM `cool-arch-249617.analytics_207839834.events_*`, UNNEST(items) as items
  WHERE platform = 'WEB'
  AND _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(),INTERVAL 3 DAY))
  AND FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
  AND event_name = 'purchase'
  GROUP BY 1,2,3,4,5
),

joined_data as 
(
SELECT 
  *,
  CASE WHEN rec_timestamp < atc_timestamp THEN 1 ELSE 0 END AS rec_atc,
  CASE WHEN rec_timestamp < purchase_timestamp THEN 1 ELSE 0 END AS rec_purchase

FROM minimum_rec_gb
LEFT JOIN min_atc_gb USING(user_pseudo_id, event_date, item_name, item_id)
LEFT JOIN purchases USING(user_pseudo_id, event_date, item_name, item_id)
)

select * from joined_data;
