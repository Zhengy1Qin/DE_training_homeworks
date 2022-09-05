create table ods.ods_product_category
(
    product_category_id        int primary key,
    rowguid                    text,
    parent_product_category_id int,
    name                       text,
    modified_date              date,
    record_date                date default CURRENT_DATE
);
