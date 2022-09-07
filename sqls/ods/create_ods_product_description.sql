drop table ods.ods_product_description;
create table ods.ods_product_description
(
    product_description_id int,
    description            text,
    rowguid                text,
    modified_date          date,
    ods_insert_date            date default CURRENT_DATE
);
