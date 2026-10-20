-- TODO 1: este modelo ainda está quase bruto.
-- Rode os testes primeiro para ver a falha. Depois substitua o SELECT pelo código do caderno.
select
    order_id,
    customer_id,
    product_id,
    total,
    order_date,
    status
from {{ source('raw', 'orders') }}
