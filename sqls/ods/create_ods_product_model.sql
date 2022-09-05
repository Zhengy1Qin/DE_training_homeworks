create table ods.ods_product_model
(
    product_model_id    int primary key,
    rowguid             text,
    name                text,
    catalog_description text,
    modified_date       date,
    insert_date         date default CURRENT_DATE
);
