create table dw_address
(
    rowguid        nvarchar(60) primary key,
    address_id     int,
    address_line1  nvarchar(60),
    address_line2  nvarchar(60),
    city           nvarchar(30),
    state_province nvarchar(50),
    country_region nvarchar(50),
    postal_code    nvarchar(15),
    rowguid        nvarchar(60),
    modified_date  datetime,
    start_date     datetime,
    end_date       datetime,
    is_valid_flag  nvarchar(15)
);