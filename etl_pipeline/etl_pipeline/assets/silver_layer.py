import pandas as pd
from dagster import asset, Output, AssetIn, AssetOut
from sqlalchemy import create_engine
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType


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

    query = """SELECT ro.order_id, ro.customer_id, ro.order_purchase_timestamp
                    , roi.product_id
                    , rop.payment_value
                    , ro.order_status
                FROM olist_orders_dataset ro    
                JOIN olist_order_items_dataset roi  
                ON ro.order_id = roi.order_id   
                JOIN olist_order_payments_dataset rop   
                ON ro.order_id = rop.order_id"""

# Using Spark
    spark = (SparkSession.builder.appName("silver_fact-sales_{}".format(datetime.today()))
             .master("spark://spark-master:7077")
             .getOrCreate())
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")

    spark_bronze_olist_orders = spark.createDataFrame(bronze_olist_orders_dataset)
    spark_bronze_olist_order_items = spark.createDataFrame(bronze_olist_order_items_dataset)
    spark_bronze_olist_order_payments = spark.createDataFrame(bronze_olist_order_payments_dataset)

    spark_bronze_olist_orders.createOrReplaceTempView("olist_orders_dataset")
    spark_bronze_olist_order_items.createOrReplaceTempView("olist_order_items_dataset")
    spark_bronze_olist_order_payments.createOrReplaceTempView("olist_order_payments_dataset")

    sparkDF = spark.sql(query)
    pd_data = sparkDF.toPandas()

# Using sqlalchemy
    # engine = create_engine('sqlite://', echo=False)
    #
    # # Convert the DataFrame to a SQL table
    # bronze_olist_orders_dataset.to_sql('olist_orders_dataset', con=engine, index=False)
    # bronze_olist_order_items_dataset.to_sql('olist_order_items_dataset', con=engine, index=False)
    # bronze_olist_order_payments_dataset.to_sql('olist_order_payments_dataset', con=engine, index=False)
    #
    # # Execute the SQL query using Pandas
    # pd_data = pd.read_sql_query(query, con=engine)
    #
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

    query = """SELECT 
                rp.product_id
                , pcnt.product_category_name_english    
            FROM olist_products_dataset rp  
            JOIN product_category_name_translation pcnt 
            ON rp.product_category_name = pcnt.product_category_name"""

# Using Spark
    spark = (SparkSession.builder.appName("silver_dim-products_{}".format(datetime.today()))
             .master("spark://spark-master:7077")
             .getOrCreate())
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")

    # Creating spark dataframe
    spark_olist_products = spark.createDataFrame(bronze_olist_products_dataset)
    spark_product_category_name_translation = spark.createDataFrame(bronze_product_category_name_translation)

    spark_olist_products.createOrReplaceTempView("olist_products_dataset")
    spark_product_category_name_translation.createOrReplaceTempView("product_category_name_translation")

    sparkDF = spark.sql(query)
    pd_data = sparkDF.toPandas()

# # Using sqlalchenmy
#     engine = create_engine('sqlite://', echo=False)
#
#     # Convert the DataFrame to a SQL table
#     bronze_olist_products_dataset.to_sql('olist_products_dataset', con=engine, index=False)
#     bronze_product_category_name_translation.to_sql('product_category_name_translation', con=engine, index=False)
#
#     # Execute the SQL query using Pandas
#     pd_data = pd.read_sql_query(query, con=engine)

    context.log.info(pd_data)

    return Output(
        pd_data,
        metadata={
            "table": "dim_products",
            "records count": len(pd_data),
        },
    )