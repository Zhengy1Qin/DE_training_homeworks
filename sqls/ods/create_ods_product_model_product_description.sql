drop table ods.ods_product_model_product_description;
create table ods.ods_product_model_product_description
(
    product_modelid        int,
    product_description_id int,
    culture                nchar(6),
    rowguid                text,
    modified_date          date,
    ods_insert_date            date default CURRENT_DATE
);
