import pandas as pd
from dagster import asset, Output, AssetIn, AssetOut
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from sqlalchemy import create_engine, DATETIME


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
    compute_kind="pandas",
    group_name="silver_layer"
)
def silver_fact_sales(
        context,
        bronze_olist_orders_dataset: pd.DataFrame,
        bronze_olist_order_items_dataset: pd.DataFrame,
        bronze_olist_order_payments_dataset: pd.DataFrame
) -> Output[pd.DataFrame]:
    query = """SELECT ro.order_id
                    , ro.customer_id
                    , ro.order_purchase_timestamp
                    , ro.order_delivered_carrier_date
                    , ro.order_delivered_customer_date
                    , ro.order_estimated_delivery_date
                    , roi.product_id
                    , roi.seller_id
                    , roi.shipping_limit_date
                    , (roi.price + roi.freight_value) as price
                    , rop.payment_type
                    , rop.payment_installments
                    , rop.payment_value
                    , ro.order_status
                FROM olist_orders_dataset ro    
                JOIN olist_order_items_dataset roi  
                ON ro.order_id = roi.order_id   
                JOIN olist_order_payments_dataset rop   
                ON ro.order_id = rop.order_id"""

    engine = create_engine('sqlite://', echo=False)

    dtypes = {
        'order_purchase_timestamp': DATETIME,
        'order_delivered_carrier_date': DATETIME,
        'order_delivered_customer_date': DATETIME,
        'order_estimated_delivery_date': DATETIME
    }

    # Convert the DataFrame to a SQL table
    bronze_olist_orders_dataset.to_sql('olist_orders_dataset', con=engine, index=False, dtype=dtypes)
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
    compute_kind="pandas",
    group_name="silver_layer"
)
def silver_dim_products(
        context,
        bronze_olist_products_dataset: pd.DataFrame,
        bronze_product_category_name_translation: pd.DataFrame
) -> Output[pd.DataFrame]:

    engine = create_engine('sqlite://', echo=False)

    query = """SELECT 
                rp.product_id
                , pcnt.product_category_name_english    
            FROM olist_products_dataset rp  
            JOIN product_category_name_translation pcnt 
            ON rp.product_category_name = pcnt.product_category_name"""

    bronze_olist_products_dataset.to_sql('olist_products_dataset', con=engine, index=False)
    bronze_product_category_name_translation.to_sql('product_category_name_translation', con=engine, index=False)

    pd_data = pd.read_sql_query(query, con=engine)
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
        ),
        "bronze_olist_geolocation_dataset": AssetIn(
            key_prefix=["bronze", "ecom"]
        )
    },
    io_manager_key="minio_io_manager",
    key_prefix=["silver", "ecom"],
    compute_kind="pandas",
    group_name="silver_layer"
)
def silver_dim_sellers(
        context,
        bronze_olist_sellers_dataset: pd.DataFrame,
        bronze_olist_geolocation_dataset: pd.DataFrame,
) -> Output[pd.DataFrame]:
    engine = create_engine('sqlite://', echo=False)

    query = """SELECT
                seller_id
                , seller_city
                , seller_state
                , geolocation_lat
                , geolocation_lng
            FROM olist_sellers_dataset osd
            JOIN olist_geolocation_dataset ogd
            ON osd.seller_zip_code_prefix = ogd.geolocation_zip_code_prefix"""

    bronze_olist_sellers_dataset.to_sql('olist_sellers_dataset', con=engine, index=False)
    bronze_olist_geolocation_dataset.to_sql('olist_geolocation_dataset', con=engine, index=False)

    pd_data = pd.read_sql_query(query, con=engine)
    context.log.info(pd_data)

    return Output(
        pd_data,
        metadata={
            "table": "dim_sellers",
            "records count": len(pd_data),
        }
    )


@asset(
    ins={
        "bronze_olist_customers_dataset": AssetIn(
            key_prefix=["bronze", "ecom"],
        ),
        "bronze_olist_geolocation_dataset": AssetIn(
            key_prefix=["bronze", "ecom"]
        )
    },
    io_manager_key="minio_io_manager",
    key_prefix=["silver", "ecom"],
    compute_kind="pandas",
    group_name="silver_layer"
)
def silver_dim_customers(
        context,
        bronze_olist_customers_dataset: pd.DataFrame,
        bronze_olist_geolocation_dataset: pd.DataFrame
) -> Output[pd.DataFrame]:

    # engine = create_engine('sqlite://', echo=False)
    #
    # query = """SELECT
    #             customer_id
    #             , customer_unique_id
    #             , customer_city
    #             , customer_state
    #             , geolocation_lat
    #             , geolocation_lng
    #         FROM olist_customers_dataset ocd
    #         JOIN olist_geolocation_dataset ogd
    #         ON ocd.customer_zip_code_prefix = ogd.geolocation_zip_code_prefix"""

    # bronze_olist_customers_dataset.to_sql('olist_customers_dataset', con=engine, index=False)
    # bronze_olist_geolocation_dataset.to_sql('olist_geolocation_dataset', con=engine, index=False)

    # pd_data = pd.read_sql_query(query, con=engine)
    data = pd.merge(bronze_olist_customers_dataset, bronze_olist_geolocation_dataset, left_on='customer_zip_code_prefix'
                    , right_on='geolocation_zip_code_prefix')
    cols = ['customer_id', 'customer_unique_id', 'customer_city', 'customer_state', 'geolocation_lat', 'geolocation_lng']
    pd_data = data[cols]
    context.log.info(pd_data)

    return Output(
        pd_data,
        metadata={
            "table": "dim_customers",
            "records count": len(pd_data),
        },
    )


@asset(
    ins={
        "bronze_olist_orders_dataset": AssetIn(
            key_prefix=["bronze", "ecom"],
        ),
        "bronze_olist_customers_dataset": AssetIn(
            key_prefix=["bronze", "ecom"]
        ),
        "bronze_olist_order_items_dataset": AssetIn(
            key_prefix=["bronze", "ecom"]
        ),
        "bronze_products_dataset": AssetIn(
            key_prefix=["bronze", "ecom"]
        )
    },
    io_manager_key="minio_io_manager",
    key_prefix=["silver", "ecom"],
    compute_kind="spark",
    group_name="silver_layer"
)
def silver_dim_geolocation(
        context,
        bronze_olist_geolocation_dataset: pd.DataFrame
) -> Output[DataFrame]:
    query = """SELECT *
                FROM olist_geolocation_dataset ogd"""

    # Using Spark
    spark = (SparkSession.builder.appName("silver_dim-geolocation_{}".format(datetime.today()))
             .master("spark://spark-master:7077")
             .getOrCreate())
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")

    spark_bronze_olist_geolocation = spark.createDataFrame(bronze_olist_geolocation_dataset)

    spark_bronze_olist_geolocation.createOrReplaceTempView("olist_geolocation_dataset")

    sparkDF = spark.sql(query)
    # pd_data = sparkDF.toPandas()
    # context.log.info(pd_data)

    return Output(
        # pd_data,
        # metadata={
        #     "table": "dim_geolocation",
        #     "records count": len(pd_data),
        # },
        sparkDF,
        metadata={
            "table": "dim_geolocation",
            "records count": sparkDF.count()
        }
    )
