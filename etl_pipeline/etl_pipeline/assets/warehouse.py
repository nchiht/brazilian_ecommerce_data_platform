import pandas as pd
from dagster import asset, AssetIn, Output, AssetExecutionContext


@asset(
    ins={'silver_fact_sales': AssetIn(key_prefix=["silver", "ecom"])},
    io_manager_key='psql_io_manager',
    required_resource_keys={"psql_io_manager"},
    key_prefix=["warehouse"],
    compute_kind="postgres",
    group_name="warehouse_layer"
)
def fact_sales(context: AssetExecutionContext, silver_fact_sales: pd.DataFrame) -> Output[pd.DataFrame]:
    return Output(
        silver_fact_sales,
        metadata={
            'schema': 'warehouse',
            'table': 'fact_sales',
            "records count": len(silver_fact_sales)
        }
    )


@asset(
    ins={'silver_dim_products': AssetIn(key_prefix=["silver", "ecom"])},
    io_manager_key='psql_io_manager',
    required_resource_keys={"psql_io_manager"},
    key_prefix=["warehouse"],
    compute_kind="postgres",
    group_name="warehouse_layer"
)
def dim_products(context: AssetExecutionContext, silver_dim_products: pd.DataFrame) -> Output[pd.DataFrame]:
    return Output(
        silver_dim_products,
        metadata={
            'schema': 'warehouse',
            'table': 'dim_products',
            "records count": len(silver_dim_products)
        }
    )


@asset(
    ins={'silver_dim_sellers': AssetIn(key_prefix=["silver", "ecom"])},
    io_manager_key='psql_io_manager',
    required_resource_keys={"psql_io_manager"},
    key_prefix=["warehouse"],
    compute_kind="postgres",
    group_name="warehouse_layer"
)
def dim_sellers(context, silver_dim_sellers: pd.DataFrame) -> Output[pd.DataFrame]:
    return Output(
        silver_dim_sellers,
        metadata={
            'schema': 'warehouse',
            'table': 'dim_sellers',
            "records count": len(silver_dim_sellers)
        }
    )


@asset(
    ins={'silver_dim_customers': AssetIn(key_prefix=["silver", "ecom"])},
    io_manager_key='psql_io_manager',
    required_resource_keys={"psql_io_manager"},
    key_prefix=["warehouse"],
    compute_kind="postgres",
    group_name="warehouse_layer"
)
def dim_customers(context, silver_dim_customers: pd.DataFrame) -> Output[pd.DataFrame]:
    return Output(
        silver_dim_customers,
        metadata={
            'schema': 'warehouse',
            'table': 'dim_customers',
            "records count": len(silver_dim_customers)
        }
    )


@asset(
    ins={'silver_dim_geolocation': AssetIn(key_prefix=["silver", "ecom"])},
    io_manager_key='psql_io_manager',
    required_resource_keys={"psql_io_manager"},
    key_prefix=["warehouse"],
    compute_kind="postgres",
    group_name="warehouse_layer"
)
def dim_geolocation(context, silver_dim_geolocation: pd.DataFrame) -> Output[pd.DataFrame]:
    return Output(
        silver_dim_geolocation,
        metadata={
            'schema': 'warehouse',
            'table': 'dim_geolocation',
            "records count": len(silver_dim_geolocation)
        }
    )
