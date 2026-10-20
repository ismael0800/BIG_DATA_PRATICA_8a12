-- TODO 2: substitua este modelo simples pelo mart do caderno.
select
    cast(order_date as date) as order_date,
    count(*) as total_pedidos
from {{ ref('stg_orders') }}
group by 1
