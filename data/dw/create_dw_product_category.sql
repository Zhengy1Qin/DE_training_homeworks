create table dw_product_category
(
    product_category_id        int primary key,
    rowguid                    text,
    parent_product_category_id int,
    name                       text,
    modified_date              date
);
