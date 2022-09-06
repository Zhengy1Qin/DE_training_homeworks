drop table ods.ods_product;
create table ods.ods_product
(
    product_id                int,
    name                      text,
    product_number            text,
    color                     text,
    standard_cost             money,
    list_price                money,
    size                      text,
    weight                    text,
    product_category_id       int,
    product_model_id          int,
    sell_start_date           date,
    sell_end_date             text,
    discontinued_date         text,
    thumb_nail_photo          text,
    thumbnail_photo_file_name text,
    rowguid                   text primary key,
    modified_date             date,
    ods_insert_date               date default CURRENT_DATE
);