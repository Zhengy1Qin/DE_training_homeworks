drop table ods.ods_address;
create table ods.ods_address
(
    address_id     int,
    address_line1  text,
    address_line2  text,
    city           text,
    state_province text,
    country_region text,
    postal_code    text,
    rowguid        text primary key,
    modified_date  date,
    ods_insert_date    date default CURRENT_DATE
);