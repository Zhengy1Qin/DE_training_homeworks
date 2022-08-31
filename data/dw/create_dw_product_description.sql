create table dw_product_description
(
    rowguid                text primary key,
    product_description_id int,
    description            text,
    modified_date          date,
    start_date             date,
    end_date               date,
    is_valid_flag          text
);
