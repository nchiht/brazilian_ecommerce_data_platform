import os
from contextlib import contextmanager
from datetime import datetime
from typing import Union

import pandas as pd
from dagster import IOManager, OutputContext, InputContext
from minio import Minio
import pyarrow as pa
import pyarrow.parquet as pq


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
    except Exception:
        raise


class MinIOIOManager(IOManager):
    def __init__(self, config):
        self._config = config

    def _get_path(self, context: Union[InputContext, OutputContext]):
        layer, schema, table = context.asset_key.path
        key = "/".join([layer, schema, table.replace(f"{layer}_", "")])
        tmp_file_path = "/tmp/file-{}-{}.parquet".format(
            datetime.today().strftime("%Y%m%d%H%M%S"),

            "-".join(context.asset_key.path)
        )
        return f"{key}.pq", tmp_file_path

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        # convert to parquet format
        key_name, tmp_file_path = self._get_path(context)
        obj.to_parquet(tmp_file_path, engine='pyarrow')

        context.log.info("Establishing minio connection")
        try:
            # connect to MinIO
            with connect_minio(self._config) as client:
                # client = connect_minio(self._config)

                # Make the bucket if it doesn't exist.
                bucket_name = self._config.get("bucket")
                found = client.bucket_exists(bucket_name)
                if not found:
                    client.make_bucket(bucket_name)
                    print("Created bucket", bucket_name)
                else:
                    print("Bucket", bucket_name, "already exists")

                # upload to MinIO
                client.fput_object(bucket_name, key_name, tmp_file_path)
                print("successfully uploaded to bucket", bucket_name)

                # clean up tmp file
                os.remove(tmp_file_path)
        except Exception:
            raise

    def load_input(self, context: InputContext) -> pd.DataFrame:
        key_name, tmp_file_path = self._get_path(context)
        try:
            with connect_minio(self._config) as client:
                bucket_name = self._config.get("bucket")

                # get parquet file from minio
                client.fget_object(bucket_name, key_name, tmp_file_path)
                content = pd.read_parquet(tmp_file_path)
                context.log.info(content)

                # clean up tmp file
                os.remove(tmp_file_path)
                return content
        except Exception:
            raise

    # def integrate_data(self, sql: str, context, ) -> None:
    #     key_name, tmp_file_path = self._get_path(context)
    #     try:
    #         df =
    #         with connect_minio(self._config) as client:
    #             context.log.info("hello")
    #     except Exception as e:
    #         raise e