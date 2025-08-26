import os
import streamlit as st
import pandas as pd
from databricks import sql
import re
from decimal import Decimal
import numpy as np


# ---------- CONFIG ----------
'''CATALOG = os.getenv("CATALOG", "main")
SCHEMA  = os.getenv("SCHEMA", "retail")
TABLE   = os.getenv("TABLE", "superstore_silver")
FQTN    = f"{CATALOG}.{SCHEMA}.{TABLE}"'''

st.set_page_config(page_title="Superstore + Genie", layout="wide")
st.title("Ask Genie")
st.caption(f"Querying: `{FQTN}` via SQL Warehouse")

# ---------- CONNECTOR ----------
'''def get_conn():
    return sql.connect(
        server_hostname=os.getenv("DB_HOST"),
        http_path=os.getenv("DB_HTTP_PATH"),
        access_token=os.getenv("DB_TOKEN"),
    )'''

# ---- Date Range of Dataset ----

@st.cache_data(show_spinner=False)
def get_date_bounds():
    with get_conn().cursor() as cur:
        cur.execute(f"SELECT CAST(min(order_date) AS date), CAST(max(order_date) AS date) FROM {FQTN}")
        lo, hi = cur.fetchone()
    return pd.to_datetime(lo).date(), pd.to_datetime(hi).date()

DATA_MIN, DATA_MAX = get_date_bounds()


# ---------- GENIE INTEGRATION POINT ---------
def genie_to_sql(nl_query: str, fqtn: str) -> str:
    q = " ".join(nl_query.strip().lower().split())

    # --- dictionaries / allowlists ---
    metric_alias = {
        "sales": "sales",
        "revenue": "sales",
        "profit": "profit",
        "quantity": "quantity",
        "qty": "quantity",
        "discount": "discount",
        "profit margin": "profit_margin",
        "margin": "profit_margin",
        "profit %": "profit_margin",
    }
    # dims we explicitly support
    DIM_PATTERNS = [
        (r"\bcustomer(s)?( name(s)?)?\b", "customer_name"),
        (r"\bproduct(s)?( name(s)?)?\b", "product_name"),
        (r"\bcategory\b", "category"),
        (r"\bsubcategory\b", "subcategory"),
        (r"\bregion(s)?\b", "region"),
        (r"\bsegment(s)?\b", "segment"),
        (r"\bstate(s)?\b", "state"),
        (r"\bcit(y|ies)\b", "city"),
        (r"\bship[ _]?mode(s)?\b", "ship_mode"),
    ]
    # canonical region/segment values (normalize case)
    region_vals = {"west":"West", "east":"East", "central":"Central", "south":"South"}
    segment_vals = {"consumer":"Consumer", "corporate":"Corporate", "home office":"Home Office", "homeoffice":"Home Office"}

    # --- helpers ---
    def pick_metric(text: str) -> str:
        # prefer two-word "profit margin" before "profit"/"margin"
        if "profit margin" in text or "profit %" in text:
            return "profit_margin"
        for k, v in metric_alias.items():
            if re.search(rf"\b{k}\b", text):
                return v
        return "sales"

    def pick_grain(text: str) -> str | None:
        if re.search(r"\bby month\b", text):
            return "month"
        if re.search(r"\bby quarter\b", text):
            return "quarter"
        if re.search(r"\bby year\b", text):
            return "year"
        return None

    def pick_dim(text: str) -> str | None:
        m = re.search(r"\bby ([a-z ]+?)\b($| in | last |\d| top | and | with )", text)
        if m:
            cand = m.group(1).strip()
            for pat, col in DIM_PATTERNS:
                if re.search(pat, cand):
                    return col
        for pat, col in DIM_PATTERNS:
            if re.search(pat, text):
                return col
        return None

    def pick_topn(text: str) -> int | None:
        m = re.search(r"\btop\s+(\d+)\b", text)
        return int(m.group(1)) if m else None

    def year_filter(text: str) -> str | None:
        m = re.search(r"\bin\s+(20\d{2}|19\d{2})\b", text)
        if not m: 
            return None
        y = int(m.group(1))
        # hard fail if out of coverage (clear feedback > empty result)
        if y < DATA_MIN.year or y > DATA_MAX.year:
            raise ValueError(f"No data for {y}. Data covers {DATA_MIN} to {DATA_MAX}.")
        return f"year(order_date) = {y}"

    def last_n_months_filter(text: str) -> str | None:
        m = re.search(r"\blast\s+(\d+)\s+months?\b", text)
        if not m:
            return None
        n = int(m.group(1))
        # make 'last N months' relative to DATA_MAX, not today
        return f"order_date BETWEEN add_months(date '{DATA_MAX}', -{n}) AND date '{DATA_MAX}'"

    def region_filter(text: str) -> str | None:
        m = re.search(r"\bin\s+(west|east|central|south)\b", text)
        if m:
            return f"region = '{region_vals[m.group(1)]}'"
        return None

    # only add a segment filter when NOT doing a breakdown
    def segment_filter(text: str) -> str | None:
        if re.search(r"\bby\s+segment(s)?\b", text):  # user asked to group by segment
            return None
        m = re.search(r"\bsegment(s)?\s+(consumer|corporate|home[ _]?office)s?\b", text)
        if m:
            v = m.group(2).replace(" ", "").lower()
            return {
                "consumer": "segment = 'Consumer'",
                "corporate": "segment = 'Corporate'",
                "homeoffice": "segment = 'Home Office'",
            }[v]
        return None

    def where_clause(parts: list[str]) -> str:
        parts = [p for p in parts if p]
        return ("WHERE " + " AND ".join(parts)) if parts else ""


    # --- parse ---
    metric = pick_metric(q)
    grain  = pick_grain(q)          # month/quarter/year or None
    dim    = pick_dim(q)            # e.g., category, product_name, region, ...
    topn   = pick_topn(q)           # int or None
    
    if topn and not dim:
        if re.search(r"\bproducts?\b", q):  dim = "product_name"
        elif re.search(r"\bcustomers?\b", q): dim = "customer_name"
    
    filters = []
    filters.append(year_filter(q))
    filters.append(last_n_months_filter(q))
    filters.append(region_filter(q))
    filters.append(segment_filter(q))
    where = where_clause(filters)

    # --- SQL builders ---
    def agg_expr(m: str) -> str:
        if m == "profit_margin":
            # supply both sales & profit + derived margin
            return ("SUM(profit) AS profit, "
                    "SUM(sales) AS sales, "
                    "CASE WHEN SUM(sales)=0 THEN NULL ELSE SUM(profit)/SUM(sales) END AS profit_margin")
        elif m in ("sales","profit","quantity","discount"):
            return f"SUM({m}) AS {m}"
        # default
        return "SUM(sales) AS sales"

    def order_by_for(m: str, key_alias: str) -> str:
        if key_alias in ("month","quarter","year","period"):
            return f"ORDER BY {key_alias}"
        if m == "profit_margin":
            return "ORDER BY profit_margin DESC NULLS LAST"
        return f"ORDER BY {m} DESC"

    # 1) Time series: "<metric> by month/quarter/year"
    if grain:
        dt = {"month":"month","quarter":"quarter","year":"year"}[grain]
        trunc = {"month":"date_trunc('month', order_date)",
                 "quarter":"date_trunc('quarter', order_date)",
                 "year":"date_trunc('year', order_date)"}[grain]
        # allow multi-metric for common case "sales and profit by <grain>"
        wants_multi = ("sales and profit" in q) or ("profit and sales" in q)
        if wants_multi and metric in ("sales","profit"):
            agg = "SUM(sales) AS sales, SUM(profit) AS profit"
        else:
            agg = agg_expr(metric)
        return f"""
        SELECT {trunc} AS {dt}, {agg}
        FROM {fqtn}
        {where}
        GROUP BY {dt}
        {order_by_for(metric, dt)}
        """.strip()

    # 2) Profit margin by category/other dim
    if metric == "profit_margin" and dim:
        return f"""
        SELECT
          {dim},
          SUM(profit) AS profit,
          SUM(sales)  AS sales,
          CASE WHEN SUM(sales)=0 THEN NULL ELSE SUM(profit)/SUM(sales) END AS profit_margin
        FROM {fqtn}
        {where}
        GROUP BY {dim}
        ORDER BY profit_margin DESC NULLS LAST
        """.strip()

    # 3) Top N by product/customer/... (defaults to product when 'top' & 'product' in query)
    if topn and dim:
        agg = agg_expr(metric)
        return f"""
        SELECT {dim}, {agg}
        FROM {fqtn}
        {where}
        GROUP BY {dim}
        {order_by_for(metric, dim)}
        LIMIT {topn}
        """.strip()
    # common phrasing: "top N products by sales"
    if topn and not dim and ("product" in q or "products" in q):
        dim = "product_name"
        agg = agg_expr(metric)
        return f"""
        SELECT {dim}, {agg}
        FROM {fqtn}
        {where}
        GROUP BY {dim}
        {order_by_for(metric, dim)}
        LIMIT {topn}
        """.strip()

    # 4) Breakdown by category/region/segment/... (optionally multi-metric sales+profit)
    if dim:
        wants_multi = ("sales and profit" in q) or ("profit and sales" in q)
        agg = "SUM(sales) AS sales, SUM(profit) AS profit" if wants_multi else agg_expr(metric)
        return f"""
        SELECT {dim}, {agg}
        FROM {fqtn}
        {where}
        GROUP BY {dim}
        {order_by_for(metric, dim)}
        """.strip()

    # 5) Default: safe preview
    return f"SELECT * FROM {fqtn} LIMIT 100"



_BANNED_VERBS = re.compile(
    r"\b("
    r"insert|update|delete|merge|drop|alter|grant|revoke|truncate|"
    r"call|copy|create|replace|refresh|optimize|vacuum|set|use|"
    r"comment|analyze|msck|repair|restore|snapshot|reorg"
    r")\b",
    flags=re.IGNORECASE,
)

def is_safe_select(sql_text: str) -> bool:
    # normalize spacing/lower for checks; keep original for execution
    s = " ".join(sql_text.strip().lower().split())

    # 1) forbid multi‑statement or comments (easy injection vectors)
    if ";" in s or "--" in s or "/*" in s or "*/" in s:
        return False

    # 2) must start with SELECT or WITH
    if not (s.startswith("select") or s.startswith("with")):
        return False

    # 3) forbid any DML/DDL/control verbs anywhere
    if _BANNED_VERBS.search(s):
        return False

    # 4) heuristic: if it starts with WITH, ensure a SELECT appears later
    if s.startswith("with") and " select " not in f" {s} ":
        return False

    return True


# ---------- UI ----------
default_example = "Show sales by month"
with st.form("genie_form"):
    user_q = st.text_area("Ask in plain English (or paste a SELECT):", value=default_example, height=90)
    submitted = st.form_submit_button("Run")

if submitted and user_q.strip():
    if user_q.strip().lower().startswith("select"):
        sql_text = user_q
    else:
        with st.spinner("Asking Genie..."):
            try:
                sql_text = genie_to_sql(user_q, FQTN)
            except ValueError as ve:
                st.warning(str(ve))
                st.stop()

    # enforce gate for both user and NL paths
    if not is_safe_select(sql_text):
        st.error("Only read-only single-statement SELECTs are allowed.")
        st.stop()

    # show the SQL only if it passed
    st.code(sql_text, language="sql")

    try:
        with get_conn().cursor() as cur:
            cur.execute(sql_text)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
        pdf = pd.DataFrame(rows, columns=cols)
    except Exception as e:
        st.error(f"Query failed: {e}")
        st.stop()

    if pdf.empty:
        st.info("No rows returned.")
        st.stop()

    st.subheader("Results")
    st.dataframe(pdf)

    # Quick auto-chart if possible
    try:
        import altair as alt
        time_cols = [c for c in pdf.columns if any(k in c.lower() for k in ["date","month","day"])]
        num_cols  = [c for c in pdf.columns if pd.api.types.is_numeric_dtype(pdf[c])]
        if time_cols and num_cols:
            st.subheader("Quick chart")
            st.altair_chart(
                alt.Chart(pdf).mark_line().encode(
                    x=f"{time_cols[0]}:T",
                    y=f"{num_cols[0]}:Q",
                    tooltip=list(pdf.columns)
                ).properties(height=320),
                use_container_width=True
            )
    except Exception:
        pass

    st.download_button(
        "Download results (CSV)",
        data=pdf.to_csv(index=False).encode("utf-8"),
        file_name="genie_results.csv",
        mime="text/csv"
    )

with st.expander("How this works"):
    st.markdown(
        "- Uses DB_HOST, DB_HTTP_PATH, and DB_TOKEN (from app.yaml or environment).\n"
        "- Catalog/Schema/Table are injected as env vars, defaulting to main.retail.superstore_silver.\n"
        "- Replace the `genie_to_sql` stub with your real Genie integration."
    )
