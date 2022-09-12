default_end_time = '2999-12-31'

dm_city_sales_sql = """
select da.city,
       extract (year from dso.order_date) as year, 
       extract (month from dso.order_date) as month,
       dso.total_due,
       sum(dp.standard_cost) total_cost,
       sum(dsoli.unit_price * dsoli.order_qty) - total_cost total_profit,
       increase_due_rate
from dw.dw_sales_order dso
        left join dw.dw_customer_address dca
            on dso.customer_id = dca.customer_id
        left join dw.dw_address da
            on dca.address_id = da.address_id
        left join dw.dw_sales_order_line_item dsoli
            on dsoli.sales_order_id = dso.sales_order_id
        left join dw.dw_product dp
            on dp.product_id = dso.product_id
group by city, year, month
"""



dm_product_sales_sql = """
"""
