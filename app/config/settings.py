import os

CATALOG = os.getenv("CATALOG", "main")
SCHEMA  = os.getenv("SCHEMA", "retail")
TABLE   = os.getenv("TABLE", "superstore_silver")
FQTN    = f"{CATALOG}.{SCHEMA}.{TABLE}"
