create table dw_product_model
(
    product_model_id    int primary key,
    rowguid             nvarchar(60),
    name                nvarchar(50),
    catalog_description nvarchar(500),
    modified_date       datetime,
);
