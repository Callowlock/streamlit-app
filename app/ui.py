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
        if pdf is None or pdf.empty:
            return

        df = pdf.copy()
        # Lowercase for consistent matching (doesn't change display names)
        df.columns = [c.lower() for c in df.columns]

        # Pick a time-like column deterministically
        ts_col = next((c for c in ["quarter", "month", "year", "order_date", "date"] if c in df.columns), None)
        num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]

        if not ts_col or not num_cols:
            return

        # Detect if ts_col is datetime-like
        is_dt = pd.api.types.is_datetime64_any_dtype(df[ts_col])

        # Normalize quarter for grouped bars
        # If ts_col is datetime, derive 1..4 → 'Q1'..'Q4'
        if ts_col == "quarter":
            if is_dt:
                df["quarter"] = df[ts_col].dt.quarter
            qmap = {1: "Q1", 2: "Q2", 3: "Q3", 4: "Q4"}
            if pd.api.types.is_integer_dtype(df["quarter"]):
                df["quarter"] = df["quarter"].map(qmap)
            quarter_order = ["Q1", "Q2", "Q3", "Q4"]

        # Prefer common metric names; fall back to all numerics
        preferred = ("sales", "profit", "quantity", "discount", "profit_margin")
        metrics = [c for c in num_cols if c in preferred] or num_cols

        # Grouped bars ONLY for quarter with multiple metrics and <=4 periods
        n_periods = df[ts_col].nunique(dropna=True)
        use_grouped_bars = (ts_col == "quarter") and (len(metrics) >= 2) and (n_periods <= 4)

        st.subheader("Quick chart")

        if use_grouped_bars:
            # Drop ratios in grouped bars (scale mismatch)
            bar_metrics = [m for m in metrics if m != "profit_margin"] or metrics
            work = df[["quarter"] + bar_metrics].copy()
            long_df = work.melt(id_vars=["quarter"], value_vars=bar_metrics,
                                var_name="metric", value_name="value")
            st.altair_chart(
                alt.Chart(long_df).mark_bar().encode(
                    x=alt.X("quarter:N", sort=quarter_order, title="Quarter"),
                    y=alt.Y("value:Q"),
                    color=alt.Color("metric:N", title="Metric"),
                    tooltip=["quarter", "metric", "value"],
                ).properties(height=360),
                use_container_width=True,
            )
            return

        # Otherwise: line(s)
        def x_enc_for(col: str):
            # Temporal axis for true datetimes
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                return alt.X(f"{col}:T", title=col.replace("_"," ").title())
            # Discrete labels for derived calendar fields
            if col in {"year", "quarter", "month"}:
                return alt.X(f"{col}:N", title=col.title())
            return alt.X(f"{col}:N", title=col.replace("_"," ").title())

        plot_df = df[[ts_col] + metrics].copy().sort_values(ts_col)

        if len(metrics) == 1:
            y = metrics[0]
            st.altair_chart(
                alt.Chart(plot_df).mark_line(point=True).encode(
                    x=x_enc_for(ts_col),
                    y=alt.Y(f"{y}:Q"),
                    tooltip=list(plot_df.columns),
                ).properties(height=320),
                use_container_width=True,
            )
        else:
            long_df = plot_df.melt(id_vars=[ts_col], value_vars=metrics,
                                   var_name="metric", value_name="value")
            st.altair_chart(
                alt.Chart(long_df).mark_line(point=True).encode(
                    x=x_enc_for(ts_col),
                    y=alt.Y("value:Q"),
                    color=alt.Color("metric:N", title="Metric"),
                    tooltip=[ts_col, "metric", "value"],
                ).properties(height=320),
                use_container_width=True,
            )
    except Exception:
        # Fail safe to table if plotting errors out
        return


def render_download(pdf: pd.DataFrame, filename: str = "genie_results.csv"):
    st.download_button(
        "Download results (CSV)",
        data=pdf.to_csv(index=False).encode("utf-8"),
        file_name=filename,
        mime="text/csv"
    )
