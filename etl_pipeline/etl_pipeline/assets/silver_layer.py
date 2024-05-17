import pandas as pd
from dagster import asset, Output, AssetIn, AssetOut
from datetime import datetime
from pyspark.sql import SparkSession


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

    query = """SELECT ro.order_id
                    , ro.customer_id
                    , ro.order_purchase_timestamp
                    , roi.product_id
                    , roi.seller_id
                    , rop.payment_type
                    , rop.payment_installments
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
        bronze_olist_products_dataset: pd.DataFrame,
        bronze_product_category_name_translation: pd.DataFrame
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

    context.log.info(pd_data)

    return Output(
        pd_data,
        metadata={
            "table": "dim_products",
            "records count": len(pd_data),
        },
    )


@asset(
    ins={
        "bronze_olist_sellers_dataset": AssetIn(
            key_prefix=["bronze", "ecom"],
        )
    },
    io_manager_key="minio_io_manager",
    key_prefix=["silver", "ecom"],
    compute_kind="spark",
    group_name="silver_layer"
)
def dim_sellers(
        context,
        bronze_olist_sellers_dataset: pd.DataFrame,
) -> Output[pd.DataFrame]:

    query = """SELECT
                seller_id
                , seller_city
                , seller_state 
            FROM olist_sellers_dataset osd """

# Using Spark
    spark = (SparkSession.builder.appName("silver_dim-products_{}".format(datetime.today()))
             .master("spark://spark-master:7077")
             .getOrCreate())
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")

    # Creating spark dataframe
    spark_olist_sellers = spark.createDataFrame(bronze_olist_sellers_dataset)

    spark_olist_sellers.createOrReplaceTempView("olist_sellers_dataset")

    sparkDF = spark.sql(query)
    pd_data = sparkDF.toPandas()

    context.log.info(pd_data)

    return Output(
        pd_data,
        metadata={
            "table": "dim_sellers",
            "records count": len(pd_data),
        },
    )


@asset(
    ins={
        "bronze_olist_customers_dataset": AssetIn(
            key_prefix=["bronze", "ecom"],
        )
    },
    io_manager_key="minio_io_manager",
    key_prefix=["silver", "ecom"],
    compute_kind="spark",
    group_name="silver_layer"
)
def dim_customers(
        context,
        bronze_olist_customers_dataset: pd.DataFrame
) -> Output[pd.DataFrame]:

    query = """SELECT 
                customer_id
                , customer_unique_id
                , customer_city
                , customer_state 
            FROM olist_customers_dataset ocd """

# Using Spark
    spark = (SparkSession.builder.appName("silver_dim-customers_{}".format(datetime.today()))
             .master("spark://spark-master:7077")
             .getOrCreate())
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")

    # Creating spark dataframe
    spark_olist_customers = spark.createDataFrame(bronze_olist_customers_dataset)

    spark_olist_customers.createOrReplaceTempView("olist_customers_dataset")

    sparkDF = spark.sql(query)
    pd_data = sparkDF.toPandas()

    context.log.info(pd_data)

    return Output(
        pd_data,
        metadata={
            "table": "dim_customers",
            "records count": len(pd_data),
        },
    )
