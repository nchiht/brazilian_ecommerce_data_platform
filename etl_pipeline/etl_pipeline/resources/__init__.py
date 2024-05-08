from .mysql_io_manager import MySQLIOManager
from .minio_io_manager import MinIOIOManager
from .psql_io_manager import PostgreSQLIOManager

MYSQL_CONFIG = {
    "host": "de_mysql",
    "port": 3306,
    "database": "brazillian_ecommerce",
    "user": "admin",
    "password": "admin123",
}

MINIO_CONFIG = {
    "endpoint_url": "minio:9000",
    "bucket": "warehouse",
    "aws_access_key_id": "minio",
    "aws_secret_access_key": "minio123",
}
PSQL_CONFIG = {
    "host": "de_psql",
    "port": 5432,
    "database": "postgres",
    "user": "admin",
    "password": "admin123",
    "schema": "public"
}

mysql = MySQLIOManager(MYSQL_CONFIG)
minio = MinIOIOManager(MINIO_CONFIG)
psql = PostgreSQLIOManager(PSQL_CONFIG)
