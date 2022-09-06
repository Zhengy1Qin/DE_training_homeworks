drop table dw.dw_customer;
create table dw.dw_customer
(
    rowguid       text primary key,
    customer_id   int,
    name_style    bit,
    title         text,
    first_name    text,
    middle_name   text,
    last_name     text,
    suffix        text,
    company_name  text,
    sales_person  text,
    modified_date date,
    start_date    date,
    end_date      date,
    is_valid_flag text
);