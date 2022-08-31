create table dw_customer
(
    rowguid       nvarchar(60) primary key,
    customer_id   int,
    name_style    bit,
    title         nvarchar(8),
    first_name    nvarchar(50),
    middle_name   nvarchar(50),
    last_name     nvarchar(50),
    suffix        nvarchar(10),
    company_name  nvarchar(128),
    sales_person  nvarchar(256),
    modified_date datetime,
    start_date    datetime,
    end_date      datetime,
    is_valid_flag nvarchar(15)
);