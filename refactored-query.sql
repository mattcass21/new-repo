with as as (

  select

       orders.id,
       orders.number,
       orders.completed_at,
       orders.completed_at::date as completed_at_date,
       sum(orders.total) as net_rev,
       sum(orders.item_total) as gross_rev,
       count(orders.id) as order_count

from source_data.orders

where
orders.state != 'canceled' and
extract(year from orders.completed_at) < '2018' and
orders.email not like '%company.com'

group by completed_at_date

  ),

 b as (  select order_items.order_id,
         orders.completed_at::date as
           completed_at_date,
         sum(order_items.quantity) as qty
  from source_data.order_items
    left join source_data.orders on order_items.order_id = orders.id
  where
        orders.state != 'canceled' and
        extract(year from orders.completed_at) < '2018' and
        orders.email not like '%company.com' and
        (orders.is_cancelled_order = false OR orders.is_pending_order != true)

group by completed_at_date ),

final as (
       select
       a.completed_at_date as completed_date,
       a.gross_rev,
       a.net_rev, b.qty,
       a.order_count as orders,
       b.qty/a.distinct_orders as avg_unit_per_order,
       a.Gross_Rev/a.distinct_orders as aov_gross,
       a.Net_Rev/a.distinct_orders as aov_net
       from a
       join b
       on b.completed_at_date = a.completed_at_date

       where a.net_rev >= 150000

       order by
       a.completed_at_date desc
)

select * from final





