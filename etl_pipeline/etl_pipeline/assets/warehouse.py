import pandas as pd
from dagster import asset, AssetIn, Output


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
