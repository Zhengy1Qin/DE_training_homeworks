drop table ods.ods_customer_address;
create table ods.ods_customer_address
(
    customer_id   int,
    address_id    int,
    address_type  text,
    rowguid       text,
    modified_date date,
    ods_insert_date   date default CURRENT_DATE
);