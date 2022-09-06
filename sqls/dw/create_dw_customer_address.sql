drop table dw.dw_customer_address;
create table dw.dw_customer_address
(
    rowguid       text primary key,
    customer_id   int,
    address_id    int,
    address_type  text,
    modified_date date,
    start_date    date,
    end_date      date,
    is_valid_flag text
);