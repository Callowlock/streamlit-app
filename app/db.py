from databricks import sql
from databricks.sdk.core import Config
import os

def get_conn():
    cfg = Config()
    http_path = f"/sql/1.0/warehouses/{os.environ['WAREHOUSE_ID']}"
    return sql.connect(
        server_hostname=cfg.host,
        http_path=http_path,
        credentials_provider=lambda: cfg.authenticate,
    )
