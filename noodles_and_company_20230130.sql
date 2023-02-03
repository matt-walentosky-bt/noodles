/*
Query created by Matt Walentosky: 2023-01-30
Purpose is to create table that can be used to measure effectiveness of product recomender
Final table has format:

user_pseudo_id,
user_id,
ga_session_id,
event_date,
min_timestamp,
add_to_cart,
event_name,
item_name,
cart_state
*/

--build query to find recommended items
with view_item_list as 
(
  select 
  user_pseudo_id,
  user_id,
  (select value.int_value from unnest(event_params) where key = "ga_session_id") as ga_session_id,
  event_date,
  min(event_timestamp) as min_timestamp,
  event_name,
  items.item_name as item_name
  FROM `cool-arch-249617.analytics_207839834.events_20230129`, unnest(items) items
  where event_name in ('view_item_list')
  group by 
  user_pseudo_id,
  user_id,
  ga_session_id,
  event_date,
  event_name,
  item_name
  order by ga_session_id
),

--build query to find items added to cart
add_to_cart as 
(
  select 
  user_pseudo_id,
  user_id,
  (select value.int_value from unnest(event_params) where key = "ga_session_id") as ga_session_id,
  event_date,
  min(event_timestamp) as min_timestamp,
  event_name,
  items.item_name as item_name
  FROM `cool-arch-249617.analytics_207839834.events_20230129`, unnest(items) items
  where event_name in ('add_to_cart')
  group by 
  user_pseudo_id,
  user_id,
  ga_session_id,
  event_date,
  event_name,
  item_name
  order by ga_session_id
),
--build query to find purchased items
purchase as  
(
  select 
  user_pseudo_id,
  user_id,
  (select value.int_value from unnest(event_params) where key = "ga_session_id") as ga_session_id,
  event_date,
  min(event_timestamp) as min_timestamp,
  event_name,
  items.item_name as item_name
  FROM `cool-arch-249617.analytics_207839834.events_20230129`, unnest(items) items
  where event_name in ('purchase')
  group by 
  user_pseudo_id,
  user_id,
  ga_session_id,
  event_date,
  event_name,
  item_name
  order by ga_session_id
),

--union together data sources
union_data as 
(
 select * from view_item_list
 union all
 select * from add_to_cart
 union all
 select * from purchase
),

--create seperate variable for add_to_cart
format_01 as 
(
select 
user_pseudo_id,
user_id,
ga_session_id,
event_date,
min_timestamp,
event_name,
item_name,
case when event_name = 'add_to_cart' then item_name else NULL END AS `add_to_cart`
from 
union_data
),

--create string_agg variable
format_02 as 
(
select 
user_pseudo_id,
user_id,
ga_session_id,
event_date,
min_timestamp,
add_to_cart,
event_name,
item_name,
STRING_AGG(add_to_cart) over (partition by ga_session_id order by min_timestamp) as cart_state
from
format_01
group by
user_pseudo_id, user_id, ga_session_id, event_name, item_name,
event_date, min_timestamp, add_to_cart
order by 
user_pseudo_id,
ga_session_id,
min_timestamp
)

select * from format_02;
