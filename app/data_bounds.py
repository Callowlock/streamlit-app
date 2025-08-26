from functools import lru_cache
import pandas as pd
from config.settings import FQTN
from app.db import get_conn

@lru_cache(maxsize=1)
def get_date_bounds():
    with get_conn().cursor() as cur:
        cur.execute(
            f"SELECT CAST(min(order_date) AS date), CAST(max(order_date) AS date) FROM {FQTN}"
        )
        lo, hi = cur.fetchone()
    # keep behavior identical to your app.py
    return pd.to_datetime(lo).date(), pd.to_datetime(hi).date()
