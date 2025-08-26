import streamlit as st
import pandas as pd

def render_form(default_example: str):
    with st.form("genie_form"):
        user_q = st.text_area(
            "Ask in plain English (or paste a SELECT):",
            value=default_example, height=90
        )
        submitted = st.form_submit_button("Run")
    return user_q, submitted

def render_results(pdf: pd.DataFrame):
    st.subheader("Results")
    st.dataframe(pdf, use_container_width=True)

def render_quick_chart(pdf: pd.DataFrame):
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

def render_download(pdf: pd.DataFrame, filename: str = "genie_results.csv"):
    st.download_button(
        "Download results (CSV)",
        data=pdf.to_csv(index=False).encode("utf-8"),
        file_name=filename,
        mime="text/csv"
    )
