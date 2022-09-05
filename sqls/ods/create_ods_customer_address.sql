create table ods.ods_customer_address
(
    rowguid       text primary key,
    customer_id   int,
    address_id    int,
    address_type  text,
    modified_date date,
    insert_date   date default CURRENT_DATE
);