create table ods.ods_product_description
(
    rowguid                text primary key,
    product_description_id int,
    description            text,
    modified_date          date,
    record_date            date default CURRENT_DATE
);
