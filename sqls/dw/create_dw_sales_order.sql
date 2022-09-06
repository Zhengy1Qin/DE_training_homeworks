drop table dw.dw_sales_order;
create table dw.dw_sales_order
(
    sales_order_key           text primary key,
    sales_order_id            int,
    revision_number           smallint,
    order_date                date,
    due_date                  date,
    ship_date                 date,
    status                    smallint,
    online_order_flag         bit,
    sales_order_number        text,
    purchase_order_number     text,
    account_number            text,
    customer_id               int,
    ship_to_address_id        int,
    bill_to_address_id        int,
    ship_method               text,
    credit_card_approval_code varchar(15),
    sub_total                 money,
    tax_amt                   money,
    freight                   money,
    total_due                 money,
    comment                   text,
    modified_date             date,
    start_date                date,
    end_date                  date,
    is_valid_flag             text
);
