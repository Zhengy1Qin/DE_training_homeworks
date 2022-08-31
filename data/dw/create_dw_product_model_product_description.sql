create table dw_product_model_product_description
(
    rowguid                nvarchar(60) primary key,
    product_modelid        int,
    product_description_id int,
    culture                nchar(6),
    modified_date          datetime,
    start_date             datetime,
    end_date               datetime,
    is_valid_flag          nvarchar(15)
);
