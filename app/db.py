import os
import databricks.sql as dbsql

def _server_hostname_from_host(url: str) -> str:
    return url.replace("https://", "").rstrip("/")

def get_conn():
    host = os.environ["DATABRICKS_HOST"]
    token = os.environ["DATABRICKS_TOKEN"]
    wh_id = os.environ["WAREHOUSE_ID"]

    return dbsql.connect(
        server_hostname=_server_hostname_from_host(host),
        http_path=f"/sql/1.0/warehouses/{wh_id}",
        access_token=token,
    )
