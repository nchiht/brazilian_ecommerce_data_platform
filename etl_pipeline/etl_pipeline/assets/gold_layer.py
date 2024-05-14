import pandas as pd
from dagster import asset, AssetIn, Output
from pandas import DataFrame
from sqlalchemy import create_engine


@asset(
    ins={
        'fact_sales': AssetIn(key_prefix=["silver", "ecom"]),
        'dim_products': AssetIn(key_prefix=["silver", "ecom"])
    },
    io_manager_key='minio_io_manager',
    key_prefix=["gold", "ecom"],
    compute_kind="MinIO",
    group_name="gold_layer"
)
def gold_sales_values_by_category(context, fact_sales: pd.DataFrame, dim_products: pd.DataFrame) -> Output[DataFrame]:

    query = """WITH daily_sales_products AS (
            SELECT
            DATE(order_purchase_timestamp) AS daily
            , product_id
            , ROUND(SUM(CAST(payment_value AS FLOAT)), 2) AS sales
            , COUNT(DISTINCT(order_id)) AS bills
            FROM fact_sales
            WHERE order_status = 'delivered'
            GROUP BY
            DATE(order_purchase_timestamp)
            , product_id
            ), 
            
            daily_sales_categories AS (
            SELECT
            ts.daily
            , STRFTIME('%Y-%m', ts.daily) AS monthly
            , p.product_category_name_english AS category
            , ts.sales
            , ts.bills
            , (ts.sales / ts.bills) AS values_per_bills
            FROM daily_sales_products ts
            JOIN dim_products p
            ON ts.product_id = p.product_id
            )
            
            
            SELECT
            monthly
            , category
            , SUM(sales) AS total_sales
            , SUM(bills) AS total_bills
            , (SUM(sales) * 1.0 / SUM(bills)) AS values_per_bills
            FROM daily_sales_categories
            GROUP BY
            monthly
            , category"""

    engine = create_engine('sqlite://', echo=False)

    fact_sales.to_sql('fact_sales', con=engine, index=False, if_exists='replace')
    dim_products.to_sql('dim_products', con=engine, index=False, if_exists='replace')

    pd_data = pd.read_sql_query(query, con=engine)

    context.log.info(pd_data.head(5))

    return Output(
        pd_data,
        metadata={
            'table': "sales_values_by_category",
            "records count": len(pd_data)
        }
    )


@asset(
    ins={'gold_sales_values_by_category': AssetIn(key_prefix=["gold", "ecom"])},
    io_manager_key='psql_io_manager',
    required_resource_keys={"psql_io_manager"},
    key_prefix=["public"],
    compute_kind="postgres",
    group_name="warehouse_layer"
)
def sales_values_by_category(context, gold_sales_values_by_category: pd.DataFrame) -> Output[pd.DataFrame]:
    return Output(
        gold_sales_values_by_category,
        metadata={
            'schema': 'public',
            'table': 'sales_values_by_category',
            "records count": len(gold_sales_values_by_category)
        }
    )
