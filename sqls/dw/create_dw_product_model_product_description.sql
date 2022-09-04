create table dw.dw_product_model_product_description
(
    rowguid                text primary key,
    product_modelid        int,
    product_description_id int,
    culture                nchar(6),
    modified_date          date,
    start_date             date,
    end_date               date,
    is_valid_flag          text
);
