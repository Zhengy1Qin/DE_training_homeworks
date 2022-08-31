create table dw_customer_address
(
    rowguid       nvarchar(60) primary key,
    customer_id   int,
    address_id    int,
    address_type  nvarchar(50),
    modified_date datetime,
    start_date    datetime,
    end_date      datetime,
    is_valid_flag nvarchar(15)
);