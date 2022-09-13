drop table if exists dm.dm_product_sales_by_month;
create table dm.dm_product_sales_by_month
(
    product_id   int,
    product_name text,
    year         int,
    month        int,
    total_sales  money,
    total_qty    int,
    rank         int
);