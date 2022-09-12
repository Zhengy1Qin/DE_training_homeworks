drop table dm.dm_city_sales;
create table dm.dm_city_sales
(
    city        text,
    year        text,
    month       text,
    total_due   money,
    total_cost  money,
    total_profit money,
    increase_due_rate float
);