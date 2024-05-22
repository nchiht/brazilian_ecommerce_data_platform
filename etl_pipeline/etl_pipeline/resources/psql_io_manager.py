from contextlib import contextmanager
import pandas as pd
from dagster import IOManager, OutputContext, InputContext
from sqlalchemy import create_engine, text
from datetime import datetime


@contextmanager
def connect_psql(config):
    conn_info = (
            f"postgresql+psycopg2://{config['user']}:{config['password']}"
            + f"@{config['host']}:{config['port']}"
            + f"/{config['database']}"
    )
    db_conn = create_engine(conn_info)
    try:
        yield db_conn
    except Exception:
        raise


class PostgreSQLIOManager(IOManager):

    def __init__(self, config):
        self._config = config

    def load_input(self, context: InputContext) -> pd.DataFrame:
        pass

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        schema, table = context.asset_key.path[-2], context.asset_key.path[-1]
        tmp_tbl = f"{table}_tmp_{datetime.now().strftime('%Y_%m_%d')}"
        context.log.info(context.asset_key)

        with connect_psql(self._config) as db_conn:
            primary_keys = (context.metadata or {}).get("primary_keys", [])
            ls_columns = (context.metadata or {}).get("columns", [])

            # with db_conn.connect() as cursor:
            #     cursor.execute(
            #         text(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            #     )

            try:

                # creating entire new table if this asset is not partitioned
                obj.to_sql(name=table, schema=schema, con=db_conn, if_exists='replace', index=False)

            except context.has_asset_partitions:
                # partitioning process
                with db_conn.connect() as cursor:
                    # create temp table
                    cursor.execute(
                        text(f"CREATE TEMP TABLE IF NOT EXISTS {tmp_tbl} (LIKE {schema}.{table})")
                    )
                    # insert new data
                    obj[ls_columns].to_sql(
                        name=tmp_tbl,
                        con=db_conn,
                        schema=schema,
                        if_exists="replace",
                        index=False,
                        chunksize=10000,
                        method="multi",
                    )

                with db_conn.connect() as cursor:
                    # check data inserted
                    result = cursor.execute(text(f"SELECT COUNT(*) FROM {tmp_tbl}"))
                    for row in result:
                        print(f"Temp table records: {row}")
                    # upsert data
                    if len(primary_keys) > 0:
                        conditions = " AND ".join(
                            [f""" {schema}.{table}."{k}" = {tmp_tbl}."{k}" """ for k in primary_keys])
                        command = f"""
                            BEGIN TRANSACTION;
                            DELETE FROM {schema}.{table}
                            USING {tmp_tbl}
                            WHERE {conditions};
                            INSERT INTO {schema}.{table}
                            SELECT * FROM {tmp_tbl};
                            END TRANSACTION;
                        """
                    else:
                        command = f"""
                            BEGIN TRANSACTION;
                            TRUNCATE TABLE {schema}.{table};

                            INSERT INTO {schema}.{table}
                            SELECT * FROM {tmp_tbl};
                            END TRANSACTION;
                        """

                    cursor.execute(text(command))
                    # drop temp table
                    cursor.execute(text(f"DROP TABLE IF EXISTS {tmp_tbl}"))
