drop table dw.dw_sales_order_line_item;
create table dw.dw_sales_order_line_item
(
    rowguid               text primary key,
    sales_order_id        int,
    sales_order_detail_id int,
    order_qty             smallint,
    product_id            int,
    unit_price            money,
    unit_price_discount   money,
    line_total            numeric(38, 6),
    modified_date         date,
    start_date            date,
    end_date              date,
    is_valid_flag         text
);
