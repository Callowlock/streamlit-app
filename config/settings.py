import os

CATALOG = os.getenv("CATALOG", "main")
SCHEMA  = os.getenv("SCHEMA", "retail_gold")
TABLE   = os.getenv("TABLE", "vw_sales_daily")
FQTN    = f"{CATALOG}.{SCHEMA}.{TABLE}"
