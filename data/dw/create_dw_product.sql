create table dw_product
(
    rowguid                   text primary key,
    product_id                int,
    name                      text,
    product_number            text,
    color                     text,
    standard_cost             money,
    list_price                money,
    size                      text,
    weight                    decimal(8, 2),
    product_category_id       int,
    product_model_id          int,
    sell_start_date           date,
    sell_end_date             date,
    discontinued_date         date,
    thumb_nail_photo          text,
    thumbnail_photo_file_name text,
    modified_date             date,
    start_date                date,
    end_date                  date,
    is_valid_flag             text
);