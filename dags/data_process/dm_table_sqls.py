dm_product_sales_by_month_sql = """
truncate table dm.dm_product_sales_by_month;
with product_order as (select product_id,
                              extract(year from order_date)  as year,
                              extract(month from order_date) as month,
                              line_total,
                              order_qty
                       from dw.dw_sales_order_line_item line_item
                                left join dw.dw_sales_order sales_order
                                          on line_item.sales_order_id = sales_order.sales_order_id),
     sales_by_month as (select product_id,
                               year,
                               month,
                               sum(line_total) as total_sales,
                               sum(order_qty)  as total_qty
                        from product_order
                        group by product_id, year, month),
     sales_with_rank as (select sales_by_month.product_id,
                                name,
                                year,
                                month,
                                total_sales,
                                total_qty,
                                row_number()
                                over (partition by year,month order by total_sales desc, total_qty desc) as rank
                         from sales_by_month
                                  left join dw.dw_product product on sales_by_month.product_id = product.product_id),
     final_data as (select *
                    from sales_with_rank
                    where rank <= 5)
insert
into dm.dm_product_sales_by_month(product_id, product_name, year, month, total_sales, total_qty, rank)
select product_id, name, year, month, total_sales, total_qty, rank
from final_data;
"""

dm_city_sales_sql = """
truncate table dm.dm_city_sales;

with sales_order_with_city as (select city,
                                      extract(year from (order_date + interval '1 month'))  as next_month_year,
                                      extract(month from (order_date + interval '1 month')) as next_month,
                                      extract(year from (order_date))                       as year,
                                      extract(month from (order_date))                      as month,
                                      total_due
                               from dw.dw_sales_order sales_order
                                        left join dw.dw_address adress
                                                  on sales_order.bill_to_address_id = adress.address_id),
     total_due as (select city, year, month, next_month_year, next_month, sum(total_due) as total_due
                   from sales_order_with_city
                   group by city, year, month, next_month_year, next_month)
insert
into dm.dm_city_sales(city, year, month, total_due, increase_due_rate)
select a.city,
       a.year,
       a.month,
       a.total_due,
       case
           when last_month.total_due is null then null
           else (a.total_due - last_month.total_due) / last_month.total_due end as increase_due_rate
from total_due a
         left join total_due last_month on a.city = last_month.city and a.year = last_month.next_month_year
    and a.month = last_month.next_month;

"""
