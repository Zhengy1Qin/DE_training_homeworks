create table ods.ods_customer
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
    email_address text,
    phone         text,
    password_hash text,
    password_salt text,
    modified_date date,
    record_date   date default CURRENT_DATE
);