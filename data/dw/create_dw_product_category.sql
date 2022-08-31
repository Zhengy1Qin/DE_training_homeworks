create table dw_product_category
(
    product_category_id        int primary key,
    rowguid                    nvarchar(60),
    parent_product_category_id int,
    name                       nvarchar(50),
    modified_date              datetime
);
