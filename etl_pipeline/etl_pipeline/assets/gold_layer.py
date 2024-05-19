import pandas as pd
from dagster import asset, AssetIn, Output
from pandas import DataFrame
from pyspark.sql import SparkSession
from datetime import datetime

@asset(
    ins={
        'silver_fact_sales': AssetIn(key_prefix=["silver", "ecom"]),
        'silver_dim_products': AssetIn(key_prefix=["silver", "ecom"])
    },
    io_manager_key='minio_io_manager',
    key_prefix=["gold", "ecom"],
    compute_kind="MinIO",
    group_name="gold_layer"
)
def gold_sales_values_by_category(
        context,
        silver_fact_sales: pd.DataFrame,
        silver_dim_products: pd.DataFrame
) -> Output[pd.DataFrame]:
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
            , DATE_FORMAT(ts.daily, 'yyyy-MM') AS monthly
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

    spark = (SparkSession.builder.appName("gold_sales_by_category_{}".format(datetime.today()))
             .master("spark://spark-master:7077")
             .getOrCreate())
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")

    spark_fact_sales = spark.createDataFrame(silver_fact_sales)
    spark_dim_products = spark.createDataFrame(silver_dim_products)

    spark_fact_sales.createOrReplaceTempView("fact_sales")
    spark_dim_products.createOrReplaceTempView("dim_products")

    sparkDF = spark.sql(query)
    pd_data = sparkDF.toPandas()

    context.log.info(pd_data)

    return Output(
        pd_data,
        metadata={
            'table': "sales_values_by_category",
            "records count": len(pd_data)
        }
    )
