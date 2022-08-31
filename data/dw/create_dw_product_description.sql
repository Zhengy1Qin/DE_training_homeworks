create table dw_product_description
(
    rowguid                nvarchar(60) primary key,
    product_description_id int,
    description            nvarchar(400),
    modified_date          datetime,
    start_date             datetime,
    end_date               datetime,
    is_valid_flag          nvarchar(15)
);
