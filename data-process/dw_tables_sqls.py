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
where dw.dw_product_description.product_description_id = changed_data.product_description_id;

with increment_data as (select *
                        from ods.ods_product_description
                        where ods_insert_date = '{{ ds }}')
insert
into dw_product_description
( rowguid
, product_description_id
, description
, modified_date
, start_date
, end_date
, is_valid_flag)
select rowguid,
       product_description_id,
       description,
       modified_date,
       '{{ ds }}' as start_time,
       '%s'       AS end_date,
       'True'     as is_valid_flag
from increment_data ods
         left join dw.dw_product_description dw
                   on ods.product_description_id = dw.product_description_id
where dw.modified_date < ods.modified_date
   or ods.product_description_id is null
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
where dw.dw_product_model_product_description.rowguid = changed_data.rowguid;

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
select rowguid
     , product_modelid
     , product_description_id
     , culture
     , modified_date
     , '{{ ds }}' as start_time
     , '%s'       AS end_date
     , 'True'     as is_valid_flag
from increment_data ods
         left join dw.dw_product_model_product_description dw
                   on ods.rowguid = dw.rowguid
where dw.modified_date < ods.modified_date
   or ods.rowguid is null
""" % default_end_time

dw_product_category_sql = """
""" % default_end_time

dw_product_model_sql = """
""" % default_end_time

dw_product_sql = """
""" % default_end_time

dw_sales_order_line_item_sql = """
""" % default_end_time

dw_sales_order_sql = """
""" % default_end_time

dw_address_sql = """
""" % default_end_time

dw_customer_sql = """
""" % default_end_time

dw_customer_address_sql = """
""" % default_end_time