drop table ods.ods_product_category;
create table ods.ods_product_category
(
    product_category_id        int,
    parent_product_category_id text,
    name                       text,
    rowguid                    text,
    modified_date              date,
    ods_insert_date            date default CURRENT_DATE
);
