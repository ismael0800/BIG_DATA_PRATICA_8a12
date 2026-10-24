select
    cast(order_id as integer) as order_id,
    cast(customer_id as integer) as customer_id,
    cast(product_id as integer) as product_id,
    cast(total as decimal(12,2)) as total,
    cast(order_date as date) as order_date,
    lower(trim(status)) as status
from {{ source('raw', 'orders') }}
where order_id is not null
