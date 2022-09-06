drop table ods.ods_product_model;
create table ods.ods_product_model
(
    product_model_id    int primary key,
    name                text,
    catalog_description text,
    rowguid             text,
    modified_date       date,
    ods_insert_date     date default CURRENT_DATE
);
