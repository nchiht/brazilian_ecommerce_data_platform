import pandas as pd
from dagster import asset, Output, AssetIn, AssetOut
from sqlalchemy import create_engine


@asset(
    ins={
        "bronze_olist_orders_dataset": AssetIn(
            key_prefix=["bronze", "ecom"],
        ),
        "bronze_olist_order_items_dataset": AssetIn(
            key_prefix=["bronze", "ecom"],
        ),
        "bronze_olist_order_payments_dataset": AssetIn(
            key_prefix=["bronze", "ecom"],
        )
    },
    io_manager_key="minio_io_manager",
    key_prefix=["silver", "ecom"],
    compute_kind="spark",
    group_name="silver_layer"
)
def fact_sales(
        context,
        bronze_olist_orders_dataset: pd.DataFrame,
        bronze_olist_order_items_dataset: pd.DataFrame,
        bronze_olist_order_payments_dataset: pd.DataFrame
) -> Output[pd.DataFrame]:

    engine = create_engine('sqlite://', echo=False)

    query = "SELECT ro.order_id, ro.customer_id, ro.order_purchase_timestamp\
                    , roi.product_id\
                    , rop.payment_value\
                    , ro.order_status\
                FROM olist_orders_dataset ro    \
                JOIN olist_order_items_dataset roi  \
                ON ro.order_id = roi.order_id   \
                JOIN olist_order_payments_dataset rop   \
                ON ro.order_id = rop.order_id"

    # Convert the DataFrame to a SQL table
    bronze_olist_orders_dataset.to_sql('olist_orders_dataset', con=engine, index=False)
    bronze_olist_order_items_dataset.to_sql('olist_order_items_dataset', con=engine, index=False)
    bronze_olist_order_payments_dataset.to_sql('olist_order_payments_dataset', con=engine, index=False)

    # Execute the SQL query using Pandas
    pd_data = pd.read_sql_query(query, con=engine)

    context.log.info(pd_data)

    return Output(
        pd_data,
        metadata={
            "table": "fact_sales",
            "records count": len(pd_data),
        },
    )


@asset(
    ins={
        "bronze_olist_products_dataset": AssetIn(
            key_prefix=["bronze", "ecom"],
        ),
        "bronze_product_category_name_translation": AssetIn(
            key_prefix=["bronze", "ecom"],
        )
    },
    io_manager_key="minio_io_manager",
    key_prefix=["silver", "ecom"],
    compute_kind="spark",
    group_name="silver_layer"
)
def dim_products(
        context,
        bronze_olist_products_dataset,
        bronze_product_category_name_translation
) -> Output[pd.DataFrame]:

    engine = create_engine('sqlite://', echo=False)

    query = "SELECT \
                rp.product_id\
                , pcnt.product_category_name_english    \
            FROM olist_products_dataset rp  \
            JOIN product_category_name_translation pcnt \
            ON rp.product_category_name = pcnt.product_category_name"

    # Convert the DataFrame to a SQL table
    bronze_olist_products_dataset.to_sql('olist_products_dataset', con=engine, index=False)
    bronze_product_category_name_translation.to_sql('product_category_name_translation', con=engine, index=False)

    # Execute the SQL query using Pandas
    pd_data = pd.read_sql_query(query, con=engine)

    context.log.info(pd_data)

    return Output(
        pd_data,
        metadata={
            "table": "dim_products",
            "records count": len(pd_data),
        },
    )