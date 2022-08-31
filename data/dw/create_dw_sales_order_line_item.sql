create table dw_product_model_product_description
(
    rowguid               nvarchar(60) primary key,
    sales_order_id        int,
    sales_order_detail_id int,
    order_qty             smallint,
    product_id            int,
    unit_price            money,
    unit_price_discount   money,
    line_total            numeric(38, 6),
    modified_date         datetime,
    start_date            datetime,
    end_date              datetime,
    is_valid_flag         nvarchar(15)
);
