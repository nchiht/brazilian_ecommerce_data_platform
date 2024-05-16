from .assets import bronze_layer, silver_layer, partitioning, gold_layer, warehouse
from .resources import mysql, minio, psql # type: ignore

from dagster import Definitions, load_assets_from_modules

all_assets = load_assets_from_modules(
    [bronze_layer, silver_layer, gold_layer, warehouse]
    # [bronze_layer, partitioning]
)

defs = Definitions(
    assets=all_assets,
    resources={
        "mysql_io_manager": mysql,
        "minio_io_manager": minio,
        "psql_io_manager": psql
    },
)
