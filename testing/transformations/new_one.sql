CREATE OR REFRESH STREAMING TABLE dev.chetashri.products_silver;
CREATE FLOW product_flow
AS AUTO CDC INTO
  dev.chetashri.products_silver
FROM stream(dev.chetashri.products)
  KEYS (product_id)
  APPLY AS DELETE WHEN operation = "DELETE"
  SEQUENCE BY seqNum
  COLUMNS * EXCEPT (operation, seqNum,_rescued_data,ingestion_date)
  STORED AS SCD TYPE 1;

  CREATE OR REFRESH STREAMING TABLE dev.chetashri.customers_silver;
CREATE FLOW customers_flow
AS AUTO CDC INTO
  dev.chetashri.customers_silver
FROM stream(dev.chetashri.customers)
  KEYS (customer_id)
  APPLY AS DELETE WHEN operation = "DELETE"
  SEQUENCE BY sequenceNum
  COLUMNS * EXCEPT (operation, sequenceNum,_rescued_data,ingestion_date)
  STORED AS SCD TYPE 2;


  -- gold 
create materialized view dev.chetashri.customers_active
select customer_id,customer_name,customer_email,customer_city, customer_state
from dev.chetashri.customers_silver where `__END_AT` is NULL;

--Top 3 customers based on total sales with their email, city 

create materialized view dev.chetashri.top_customers as 
select s.customer_id, c.customer_email, c.customer_city, round(sum(total_amount)) as total_sales
from dev.chetashri.sales_clean s
join dev.chetashri.customers_active c
using(customer_id)
group by customer_id,customer_email,customer_city
order by total_sales desc;

create view dev.chetashri.three_tables as
select
  s.customer_id,
  c.customer_email,
  c.customer_city,  
  p.product_id,
  p.product_name,
  round(sum(s.total_amount)) as total_sales
from dev.chetashri.sales_clean s
join dev.chetashri.customers_active c
  on s.customer_id = c.customer_id
join dev.chetashri.products_silver p
  on s.product_id = p.product_id
group by s.customer_id, c.customer_email, c.customer_city, p.product_id, p.product_name
order by total_sales desc