create table dw.dw_product_category
(
    product_category_id        int primary key,
    rowguid                    text,
    parent_product_category_id int,
    name                       text,
    modified_date              date,
    dw_insert_date             date
);
