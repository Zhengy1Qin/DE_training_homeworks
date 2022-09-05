create table ods.ods_product_model_product_description
(
    rowguid                text primary key,
    product_modelid        int,
    product_description_id int,
    culture                nchar(6),
    modified_date          date,
    record_date            date default CURRENT_DATE
);
