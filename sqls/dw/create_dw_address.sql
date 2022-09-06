drop table dw.dw_address;
create table dw.dw_address
(
    rowguid        text primary key,
    address_id     int,
    address_line1  text,
    address_line2  text,
    city           text,
    state_province text,
    country_region text,
    postal_code    text,
    modified_date  date,
    start_date     date,
    end_date       date,
    is_valid_flag  text
);