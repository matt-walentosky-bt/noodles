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

with view_item_list as 
(
  select 
  user_pseudo_id,
  user_id,
  (select value.int_value from unnest(event_params) where key = "ga_session_id") as ga_session_id,
  event_date,
  event_timestamp,
  event_name,
  items.item_name as item_name,
  items.item_list_index as item_number
  FROM `cool-arch-249617.analytics_207839834.events_20230129`, unnest(items) items
  where event_name in ('view_item_list')
  order by ga_session_id, event_timestamp
),

--build query to find items added to cart
add_to_cart as 
(
  select 
  user_pseudo_id,
  user_id,
  (select value.int_value from unnest(event_params) where key = "ga_session_id") as ga_session_id,
  event_date,
  event_timestamp,
  event_name,
  items.item_name as item_name,
  items.item_list_index as item_number
  FROM `cool-arch-249617.analytics_207839834.events_20230129`, unnest(items) items
  where event_name in ('add_to_cart')
  order by ga_session_id, event_timestamp
),
--build query to find purchased items
purchase as  
(
  select 
  user_pseudo_id,
  user_id,
  (select value.int_value from unnest(event_params) where key = "ga_session_id") as ga_session_id,
  event_date,
  event_timestamp,
  event_name,
  items.item_name as item_name,
  items.item_list_index as item_number
  FROM `cool-arch-249617.analytics_207839834.events_20230129`, unnest(items) items
  where event_name in ('purchase')
  order by ga_session_id, event_timestamp
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
event_timestamp,
event_name,
item_name,
item_number,
case when event_name = 'add_to_cart' then item_name else NULL END AS `add_to_cart`,
case when event_name = 'purchase' then item_name else NULL END AS `purchase`
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
event_timestamp,
add_to_cart,
purchase,
event_name,
item_name,
item_number,
STRING_AGG(add_to_cart) over (partition by ga_session_id order by event_timestamp) as cart_state,
STRING_AGG(purchase) over (partition by ga_session_id order by event_timestamp) as purchase_state
from
format_01
group by
user_pseudo_id, user_id, ga_session_id, 
event_name, item_name, item_number,
event_date, event_timestamp, add_to_cart, purchase
order by 
user_pseudo_id,
ga_session_id,
event_timestamp
),

min_view_item as
(
  select 
user_pseudo_id,
user_id,
ga_session_id,
event_date,
event_name,
item_name,
cart_state, 
NULL AS add_to_cart,
min(event_timestamp) as event_timestamp
from
format_02
where event_name = 'view_item_list'
group by 
user_pseudo_id,
user_id,
ga_session_id,
event_date,
event_name,
item_name,
cart_state
order by
user_pseudo_id,
user_id,
ga_session_id,
event_date,
event_timestamp
),

final_purchase_state2 as 
(
select
distinct
max_state.user_pseudo_id,
max_state.user_id,
max_state.ga_session_id,
max_state.event_date,
max_state.event_timestamp,
format_02.purchase_state as final_purchase_state
from
(
select 
user_pseudo_id,
user_id,
ga_session_id,
event_date,
max(event_timestamp) as event_timestamp
from
format_02  
group by
user_pseudo_id,
user_id,
ga_session_id,
event_date
) max_state
inner join
format_02
on 
max_state.user_pseudo_id = format_02.user_pseudo_id and
max_state.user_id = format_02.user_id and
max_state.ga_session_id = format_02.ga_session_id and
max_state.event_date = format_02.event_date and 
max_state.event_timestamp = format_02.event_timestamp
),

final_cart_state2 as 
(
select
distinct
max_state.user_pseudo_id,
max_state.user_id,
max_state.ga_session_id,
max_state.event_date,
max_state.event_timestamp,
format_02.cart_state as final_cart_state
from
(
select 
user_pseudo_id,
user_id,
ga_session_id,
event_date,
max(event_timestamp) as event_timestamp
from
format_02  
group by
user_pseudo_id,
user_id,
ga_session_id,
event_date
) max_state
inner join
format_02
on 
max_state.user_pseudo_id = format_02.user_pseudo_id and
max_state.user_id = format_02.user_id and
max_state.ga_session_id = format_02.ga_session_id and
max_state.event_date = format_02.event_date and 
max_state.event_timestamp = format_02.event_timestamp
),

final_join as
(
  select
format_02.user_pseudo_id,
format_02.user_id,
format_02.ga_session_id,
format_02.event_date,
format_02.event_timestamp,
format_02.item_name,
format_02.cart_state,
final_cart_state2.final_cart_state,
final_purchase_state2.final_purchase_state
from
(select * from format_02 where event_name  = 'view_item_list') format_02
left join
final_cart_state2
on
final_cart_state2.user_pseudo_id = format_02.user_pseudo_id and
final_cart_state2.user_id = format_02.user_id and
final_cart_state2.ga_session_id = format_02.ga_session_id and
final_cart_state2.event_date = format_02.event_date 
left join
final_purchase_state2
on
final_purchase_state2.user_pseudo_id = format_02.user_pseudo_id and
final_purchase_state2.user_id = format_02.user_id and
final_purchase_state2.ga_session_id = format_02.ga_session_id and
final_purchase_state2.event_date = format_02.event_date 
)

select 
user_pseudo_id,
user_id,
ga_session_id,
event_date,
event_timestamp,
item_name,
cart_state,
final_cart_state,
final_purchase_state,
case when REGEXP_CONTAINS(`final_cart_state`,item_name) = TRUE then 1 else 0 end as a2c_status,
case when REGEXP_CONTAINS(`final_purchase_state`,item_name) = TRUE then 1 else 0 end as purchase_status
from final_join order by user_pseudo_id, user_id, ga_session_id, event_date, event_timestamp;
