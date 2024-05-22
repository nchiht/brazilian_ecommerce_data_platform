import os

from .assets import (bronze_layer, silver_layer, gold_layer, warehouse, dbt_assets)
from .resources import mysql, minio, psql  # type: ignore

from dagster import Definitions, load_assets_from_modules
from dagster_dbt import DbtCliResource
from .constants import dbt_manifest_path, dbt_project_dir, dbt_profile_dir


all_assets = load_assets_from_modules(
    # [bronze_layer, silver_layer, gold_layer, warehouse, dbt_assets]
    [bronze_layer, silver_layer, warehouse, dbt_assets]
)

defs = Definitions(
    assets=all_assets,
    resources={
        "mysql_io_manager": mysql,
        "minio_io_manager": minio,
        "psql_io_manager": psql,
        "dbt": DbtCliResource(project_dir=os.fspath(dbt_project_dir), profiles_dir=os.fspath(dbt_profile_dir))
    },
)
