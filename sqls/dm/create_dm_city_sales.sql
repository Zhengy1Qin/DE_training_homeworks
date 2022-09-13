drop table if exists dm.dm_city_sales;
create table dm.dm_city_sales
(
    city      text,
    year      int,
    month     int,
    total_due money,
    increase_due_rate  float
);