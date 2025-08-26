import streamlit as st
import pandas as pd
import os
from config.settings import FQTN
from app.db import get_conn
from app.data_bounds import get_date_bounds
from app.ui import render_form, render_results, render_quick_chart, render_download
from app.utils import is_safe_select, expand_table
from providers.rules_provider import translate as rules_translate

if "WAREHOUSE_ID" not in os.environ:
    st.error("WAREHOUSE_ID not set. Check app.yaml 'valueFrom: sql-warehouse' binding.")
    st.stop()

st.set_page_config(page_title="Superstore + Genie", layout="wide")
st.title("Ask Genie")
st.caption(f"Querying: `{FQTN}` via SQL Warehouse")

DATA_MIN, DATA_MAX = get_date_bounds()  # cached; same semantics as your version

user_q, submitted = render_form(default_example="Show sales by month")

if submitted and user_q.strip():
    q = user_q.strip()

    # Detect manual SQL if it starts with SELECT or WITH (ignoring case)
    is_manual = q.lower().startswith(("select", "with"))

    if is_manual:
        sql_text = q
    else:
        with st.spinner("Asking Genie..."):
            try:
                sql_text = rules_translate(q, FQTN, DATA_MIN, DATA_MAX)
            except ValueError as ve:
                st.warning(str(ve))
                st.stop()

    # Expand {FQTN} placeholders for both manual and Genie-generated SQL
    sql_text = expand_table(sql_text)

    # Safety check
    if not is_safe_select(sql_text):
        st.error("Only read-only single-statement SELECTs are allowed.")
        st.stop()

    # Show SQL back to user
    st.code(sql_text, language="sql")

    # Execute
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

    # Render outputs
    render_results(pdf)
    render_quick_chart(pdf)
    render_download(pdf)


with st.expander("How this works"):
    st.markdown(
        "- Uses DB_HOST, DB_HTTP_PATH, and DB_TOKEN (env).\n"
        "- Catalog/Schema/Table via env (default main.retail.superstore_silver).\n"
        "- The NL→SQL rules come from a provider module you can swap out for Genie."
    )
