import pandas as pd
from dagster import asset, AssetIn, AssetOut, Output, DailyPartitionsDefinition


@asset(
    ins={
        "bronze_olist_order_items_dataset": AssetIn(key_prefix=["bronze", "ecom"])
    },
    key_prefix=["public"],
    io_manager_key="psql_io_manager",
    required_resource_keys={"psql_io_manager"},
    compute_kind="PostgreSQL",
    name="olist_order_items_dataset",
    group_name="partitioning"
)
def fnc_order_items(context, bronze_olist_order_items_dataset: pd.DataFrame) -> Output[pd.DataFrame]:
    return Output(
        bronze_olist_order_items_dataset,
        metadata={
            "table name": "olist_order_items_dataset",
            "ingestion_strategy": "full-load",
            "records count": len(bronze_olist_order_items_dataset)
        }
    )


@asset(
    ins={
        "bronze_olist_order_payments_dataset": AssetIn(key_prefix=["bronze", "ecom"])
    },
    key_prefix=["public"],
    io_manager_key="psql_io_manager",
    required_resource_keys={"psql_io_manager"},
    compute_kind="PostgreSQL",
    group_name="partitioning"
)
def olist_order_payments_dataset(context, bronze_olist_order_payments_dataset: pd.DataFrame) -> Output[pd.DataFrame]:
    return Output(
        bronze_olist_order_payments_dataset,
        metadata={
            "table name": "olist_order_payments_dataset",
            "ingestion_strategy": "full-load",
            "records count": len(bronze_olist_order_payments_dataset)
        }
    )


@asset(
    ins={
        "bronze_olist_products_dataset": AssetIn(key_prefix=["bronze", "ecom"])
    },
    key_prefix=["public"],
    io_manager_key="psql_io_manager",
    required_resource_keys={"psql_io_manager"},
    compute_kind="PostgreSQL",
    group_name="partitioning"
)
def olist_products_dataset(context, bronze_olist_products_dataset: pd.DataFrame) -> Output[pd.DataFrame]:
    return Output(
        bronze_olist_products_dataset,
        metadata={
            "table name": "olist_products_dataset",
            "ingestion_strategy": "full-load",
            "records count": len(bronze_olist_products_dataset)
        }
    )


@asset(
    ins={
        "bronze_olist_orders_dataset": AssetIn(key_prefix=["bronze", "ecom"])
    },
    key_prefix=["public"],
    io_manager_key="psql_io_manager",
    required_resource_keys={"psql_io_manager"},
    compute_kind="PostgreSQL",
    group_name="partitioning"
)
def olist_orders_dataset(context, bronze_olist_orders_dataset: pd.DataFrame) -> Output[pd.DataFrame]:
    return Output(
        bronze_olist_orders_dataset,
        metadata={
            "table name": "olist_orders_dataset",
            "ingestion_strategy": "incremental",
            "records count": len(bronze_olist_orders_dataset)
        }
    )