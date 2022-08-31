create table dw_product
(
    rowguid                   nvarchar(60) primary key,
    product_id                int,
    name                      nvarchar(50),
    product_number            nvarchar(25),
    color                     nvarchar(15),
    standard_cost             money,
    list_price                money,
    size                      nvarchar(5),
    weight                    decimal(8, 2),
    product_category_id       int,
    product_model_id          int,
    sell_start_date           datetime,
    sell_end_date             datetime,
    discontinued_date         datetime,
    thumb_nail_photo          varbinary( max),
    thumbnail_photo_file_name nvarchar(50),
    modified_date             datetime,
    start_date                datetime,
    end_date                  datetime,
    is_valid_flag             nvarchar(15)
);