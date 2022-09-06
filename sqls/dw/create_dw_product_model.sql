drop table dw.dw_product_model;
create table dw.dw_product_model
(
    product_model_id    int primary key,
    rowguid             text,
    name                text,
    catalog_description text,
    modified_date       date,
    dw_insert_date      date
);
