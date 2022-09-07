default_end_time = '2999-12-31'

dw_product_description_sql = """
with increment_data as (select *
                        from ods.ods_product_description
                        where ods_insert_date = '{{ ds }}'),
     changed_data as (select dw.product_description_id as product_description_id
                      from dw.dw_product_description dw
                               left join increment_data ods
                                         on ods.product_description_id = dw.product_description_id
                      where dw.modified_date < ods.modified_date
                         or ods.product_description_id is null)
update dw.dw_product_description
set end_date      = '{{ ds }}',
    is_valid_flag = 'False'
where dw.dw_product_description.product_description_id in (select * from changed_data);

with increment_data as (select *
                        from ods.ods_product_description
                        where ods_insert_date = '{{ ds }}')
insert
into dw.dw_product_description
( rowguid
, product_description_id
, description
, modified_date
, start_date
, end_date
, is_valid_flag)
select ods.rowguid,
       ods.product_description_id,
       ods.description,
       ods.modified_date,
       '{{ ds }}' as start_time,
       '%s'       AS end_date,
       'True'     as is_valid_flag
from increment_data ods
         left join dw.dw_product_description dw
                   on ods.product_description_id = dw.product_description_id
where dw.modified_date < ods.modified_date
   or dw.product_description_id is null
""" % default_end_time

dw_product_model_product_description_sql = """
with increment_data as (select *
                        from ods.ods_product_model_product_description
                        where ods_insert_date = '{{ ds }}'),
     changed_data as (select dw.rowguid as rowguid
                      from dw.dw_product_model_product_description dw
                               left join increment_data ods
                                         on ods.rowguid = dw.rowguid
                      where dw.modified_date < ods.modified_date
                         or ods.rowguid is null)
update dw.dw_product_model_product_description
set end_date      = '{{ ds }}',
    is_valid_flag = 'False'
where dw.dw_product_model_product_description.rowguid in (select * from changed_data);

with increment_data as (select *
                        from ods.ods_product_model_product_description
                        where ods_insert_date = '{{ ds }}')
insert
into dw.dw_product_model_product_description
( rowguid
, product_modelid
, product_description_id
, culture
, modified_date
, start_date
, end_date
, is_valid_flag)
select ods.rowguid
     , ods.product_modelid
     , ods.product_description_id
     , ods.culture
     , ods.modified_date
     , '{{ ds }}' as start_time
     , '%s'       AS end_date
     , 'True'     as is_valid_flag
from increment_data ods
         left join dw.dw_product_model_product_description dw
                   on ods.rowguid = dw.rowguid
where dw.modified_date < ods.modified_date
   or dw.rowguid is null
""" % default_end_time

dw_product_category_sql = """
truncate table dw.dw_product_category;
with increment_data as (select *
                        from ods.ods_product_category
                        where ods_insert_date = '{{ ds }}')
insert
into dw.dw_product_category
( product_category_id
, rowguid
, parent_product_category_id
, name
, modified_date
, dw_insert_date)
select 
product_category_id,
rowguid,
cast(case when ods.parent_product_category_id= 'NULL' or ods.parent_product_category_id = '' then null else ods.parent_product_category_id end
    as integer) as parent_product_category_id,
name,
modified_date,
ods_insert_date     as dw_insert_date
from increment_data ods;
"""

dw_product_model_sql = """
truncate table dw.dw_product_model;
with increment_data as (select *
                        from ods.ods_product_model
                        where ods_insert_date = '{{ ds }}')
insert
into dw.dw_product_model
( product_model_id
, rowguid
, name
, catalog_description
, modified_date
, dw_insert_date)
select 
ods.product_model_id,
ods.rowguid,
ods.name,
ods.catalog_description,
ods.modified_date,
ods.ods_insert_date     as dw_insert_date
from increment_data ods;
"""

dw_product_sql = """
with increment_data as (select *
                        from ods.ods_product
                        where ods_insert_date = '{{ ds }}'),
     changed_data as (select dw.rowguid as rowguid
                      from dw.dw_product dw
                               left join increment_data ods
                                         on ods.rowguid = dw.rowguid
                      where dw.modified_date < ods.modified_date
                         or ods.rowguid is null)
update dw.dw_product
set end_date      = '{{ ds }}',
    is_valid_flag = 'False'
where dw.dw_product.rowguid  in (select * from changed_data);

with increment_data as (select *
                        from ods.ods_product
                        where ods_insert_date = '{{ ds }}')
insert
into dw.dw_product
( rowguid
, product_id
, name
, product_number
, color
, standard_cost
, list_price
, size
, weight
, product_category_id
, product_model_id
, sell_start_date
, sell_end_date
, discontinued_date
, thumb_nail_photo
, thumbnail_photo_file_name
, modified_date
, start_date
, end_date
, is_valid_flag)
select ods.rowguid
     , ods.product_id
     , ods.name
     , ods.product_number
     , ods.color
     , ods.standard_cost
     , ods.list_price
     , ods.size
     , cast(case when ods.weight= 'NULL' or ods.weight = '' then null else ods.weight end
    as decimal(8, 2)) as weight
     , ods.product_category_id
     , ods.product_model_id
     , ods.sell_start_date
     , cast(case when ods.sell_end_date= 'NULL' or ods.sell_end_date = '' then null else ods.sell_end_date end
    as date) as sell_end_date
     , cast(case when ods.discontinued_date= 'NULL' or ods.discontinued_date = '' then null else ods.discontinued_date end
    as date) as discontinued_date
     , ods.thumb_nail_photo
     , ods.thumbnail_photo_file_name
     , ods.modified_date
     , '{{ ds }}' as start_time
     , '%s'       AS end_date
     , 'True'     as is_valid_flag
from increment_data ods
         left join dw.dw_product dw
                   on ods.rowguid = dw.rowguid
where dw.modified_date < ods.modified_date
   or dw.rowguid is null
""" % default_end_time

dw_sales_order_line_item_sql = """
with increment_data as (select *
                        from ods.ods_sales_order
                        where ods_insert_date = '{{ ds }}'),
     changed_data as (select dw.rowguid as rowguid
                      from dw.dw_sales_order_line_item dw
                               left join increment_data ods
                                         on ods.rowguid = dw.rowguid
                      where dw.modified_date < ods.modified_date
                         or ods.rowguid is null)
update dw.dw_sales_order_line_item
set end_date      = '{{ ds }}',
    is_valid_flag = 'False'
where dw.dw_sales_order_line_item.rowguid  in (select * from changed_data);

with increment_data as (select *
                        from ods.ods_sales_order
                        where ods_insert_date = '{{ ds }}')
insert
into dw.dw_sales_order_line_item
( rowguid
, sales_order_id
, sales_order_detail_id
, order_qty
, product_id
, unit_price
, unit_price_discount
, line_total
, modified_date
, start_date
, end_date
, is_valid_flag)
select ods.rowguid
     , ods.sales_order_id
     , ods.sales_order_detail_id
     , ods.order_qty
     , ods.product_id
     , ods.unit_price
     , ods.unit_price_discount
     , ods.line_total
     , ods.modified_date
     , '{{ ds }}' as start_time
     , '%s'       AS end_date
     , 'True'     as is_valid_flag
from increment_data ods
         left join dw.dw_sales_order_line_item dw
                   on ods.rowguid = dw.rowguid
where dw.modified_date < ods.modified_date
   or dw.rowguid is null
""" % default_end_time

dw_sales_order_sql = """
with increment_data as (select MD5(cast(sales_order_id as text) || cast(modified_date as text)) as sales_order_key,
                               *
                        from ods.ods_sales_order
                        where ods_insert_date = '{{ ds }}'),
     changed_data as (select dw.sales_order_key as sales_order_key
                      from dw.dw_sales_order dw
                               left join increment_data ods
                                         on ods.sales_order_key = dw.sales_order_key
                      where dw.modified_date < ods.modified_date
                         or ods.sales_order_key is null)
update dw.dw_sales_order
set end_date      = '{{ ds }}',
    is_valid_flag = 'False'
where dw.dw_sales_order.sales_order_key in (select * from changed_data);

with increment_data as (select MD5(cast(sales_order_id as text) || cast(modified_date as text)) as sales_order_key

                             , *
                        from ods.ods_sales_order),
     changed_data as (select ods.sales_order_key
                           , ods.sales_order_id
                           , ods.revision_number
                           , ods.order_date
                           , ods.due_date
                           , ods.ship_date
                           , ods.status
                           , ods.online_order_flag
                           , ods.sales_order_number
                           , ods.purchase_order_number
                           , ods.account_number
                           , ods.customer_id
                           , ods.ship_to_address_id
                           , ods.bill_to_address_id
                           , ods.ship_method
                           , ods.credit_card_approval_code
                           , ods.sub_total
                           , ods.tax_amt
                           , ods.freight
                           , ods.total_due
                           , ods.comment
                           , ods.modified_date
                           , '{{ ds }}'::date                                                                     as start_date
                           , '%s'::date                                                                           as end_date
                           , 'True'                                                                               as is_valid_flag
                           , row_number()
                             over (partition by ods.sales_order_key order by ods.modified_date desc)              as rank
                      from increment_data ods
                               left join dw.dw_sales_order dw
                                         on ods.sales_order_key = dw.sales_order_key
                      where dw.modified_date < ods.modified_date
                         or dw.sales_order_key is null)
insert
into dw.dw_sales_order
( sales_order_key
, sales_order_id
, revision_number
, order_date
, due_date
, ship_date
, status
, online_order_flag
, sales_order_number
, purchase_order_number
, account_number
, customer_id
, ship_to_address_id
, bill_to_address_id
, ship_method
, credit_card_approval_code
, sub_total
, tax_amt
, freight
, total_due
, comment
, modified_date
, start_date
, end_date
, is_valid_flag)
select sales_order_key
, sales_order_id
, revision_number
, order_date
, due_date
, ship_date
, status
, online_order_flag
, sales_order_number
, purchase_order_number
, account_number
, customer_id
, ship_to_address_id
, bill_to_address_id
, ship_method
, credit_card_approval_code
, sub_total
, tax_amt
, freight
, total_due
, comment
, modified_date
, start_date
, end_date
, is_valid_flag
from changed_data
where rank = 1

""" % default_end_time

dw_address_sql = """
with increment_data as (select *
                        from ods.ods_address
                        where ods_insert_date = '{{ ds }}'),
     changed_data as (select dw.rowguid as rowguid
                      from dw.dw_address dw
                               left join increment_data ods
                                         on ods.rowguid = dw.rowguid
                      where dw.modified_date < ods.modified_date
                         or ods.rowguid is null)
update dw.dw_address
set end_date      = '{{ ds }}',
    is_valid_flag = 'False'
where dw.dw_address.rowguid  in (select * from changed_data);

with increment_data as (select *
                        from ods.ods_address
                        where ods_insert_date = '{{ ds }}')
insert
into dw.dw_address
( rowguid
, address_id
, address_line1
, address_line2
, city
, state_province
, country_region
, postal_code
, modified_date
, start_date
, end_date
, is_valid_flag)
select ods.rowguid
     , ods.address_id
     , ods.address_line1
     , ods.address_line2
     , ods.city
     , ods.state_province
     , ods.country_region
     , ods.postal_code
     , ods.modified_date
     , '{{ ds }}' as start_time
     , '%s'       AS end_date
     , 'True'     as is_valid_flag
from increment_data ods
         left join dw.dw_address dw
                   on ods.rowguid = dw.rowguid
where dw.modified_date < ods.modified_date
   or dw.rowguid is null
""" % default_end_time

dw_customer_sql = """
with increment_data as (select *
                        from ods.ods_customer
                        where ods_insert_date = '{{ ds }}'),
     changed_data as (select dw.rowguid as rowguid
                      from dw.dw_customer dw
                               left join increment_data ods
                                         on ods.rowguid = dw.rowguid
                      where dw.modified_date < ods.modified_date
                         or ods.rowguid is null)
update dw.dw_customer
set end_date      = '{{ ds }}',
    is_valid_flag = 'False'
where dw.dw_customer.rowguid in (select * from changed_data);

with increment_data as (select *
                        from ods.ods_customer
                        where ods_insert_date = '{{ ds }}')
insert
into dw.dw_customer
( rowguid
, customer_id
, name_style
, title
, first_name
, middle_name
, last_name
, suffix
, company_name
, sales_person
, modified_date
, start_date
, end_date
, is_valid_flag)
select ods.rowguid
     , ods.customer_id
     , ods.name_style
     , ods.title
     , ods.first_name
     , ods.middle_name
     , ods.last_name
     , ods.suffix
     , ods.company_name
     , ods.sales_person
     , ods.modified_date
     , '{{ ds }}' as start_time
     , '%s'       AS end_date
     , 'True'     as is_valid_flag
from increment_data ods
         left join dw.dw_customer dw
                   on ods.rowguid = dw.rowguid
where dw.modified_date < ods.modified_date
   or dw.rowguid is null
""" % default_end_time

dw_customer_address_sql = """
with increment_data as (select *
                        from ods.ods_customer_address
                        where ods_insert_date = '{{ ds }}'),
     changed_data as (select dw.rowguid as rowguid
                      from dw.dw_customer_address dw
                               left join increment_data ods
                                         on ods.rowguid = dw.rowguid
                      where dw.modified_date < ods.modified_date
                         or ods.rowguid is null)
update dw.dw_customer_address
set end_date      = '{{ ds }}',
    is_valid_flag = 'False'
where dw.dw_customer_address.rowguid in (select * from changed_data);

with increment_data as (select *
                        from ods.ods_customer_address
                        where ods_insert_date = '{{ ds }}')
insert
into dw.dw_customer_address
( rowguid
, customer_id
, address_id
, address_type
, modified_date
, start_date
, end_date
, is_valid_flag)
select ods.rowguid
     , ods.customer_id
     , ods.address_id
     , ods.address_type
     , ods.modified_date
     , '{{ ds }}' as start_time
     , '%s'       AS end_date
     , 'True'     as is_valid_flag
from increment_data ods
         left join dw.dw_customer_address dw
                   on ods.rowguid = dw.rowguid
where dw.modified_date < ods.modified_date
   or dw.rowguid is null
""" % default_end_time
