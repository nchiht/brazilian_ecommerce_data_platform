from contextlib import contextmanager
from typing import Any

import pandas as pd
from dagster import IOManager, OutputContext, InputContext
from minio import Minio


@contextmanager
def connect_minio(config):
    client = Minio(
        endpoint=config.get("endpoint_url"),
        access_key=config.get("aws_access_key_id"),
        secret_key=config.get("aws_secret_access_key"),
        secure=False
    )
    try:
        yield client
    except Exception as e:
        return e


class SparkIOManager(IOManager):

    def __init__(self, config):
        self._config = config

    def load_input(self, context: "InputContext") -> Any:
        pass

    def handle_output(self, context: "OutputContext", obj: Any) -> None:

        pass
