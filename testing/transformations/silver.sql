create streaming table dev.chetashri.sales_clean (
  Constraint valid_order_id EXPECT (order_id is not Null) ON VIOLATION DROP ROW
) as 
select distinct * from stream dev.chetashri.sales;