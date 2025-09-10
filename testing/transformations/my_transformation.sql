--Materialised view / streaming table
-- sales ingestion
create streaming table dev.chetashri.sales as
select *,current_date() as ingestion_date from stream read_files('/Volumes/dev/default/raw/sales/',format=>'csv');
-- customer ingestion
create streaming table dev.chetashri.customers as
select *,current_date() as ingestion_date from stream read_files('/Volumes/dev/default/raw/customers/',format=>'csv');

-- products ingestion
create streaming table dev.chetashri.products as
select *,current_date() as ingestion_date from stream read_files('/Volumes/dev/default/raw/products/',format=>'csv');

-- create streaming table dev.chetashri.sales as
-- select * from stream read_files('/Volumes/dev/default/raw/sales/',format=>'csv');