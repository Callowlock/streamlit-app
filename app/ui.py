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

def render_quick_chart(pdf: pd.DataFrame, debug: bool = False):
    try:
        import altair as alt
        if pdf is None or pdf.empty:
            return

        df = pdf.copy()
        # Normalize colnames
        df.columns = [c.strip().lower() for c in df.columns]

        if debug:
            st.write("DEBUG DataFrame shape:", df.shape)
            st.write("DEBUG columns:", list(df.columns))
            st.dataframe(df.head())  # show first few rows

        # Force numeric coercion where possible
        for c in df.columns:
            if df[c].dtype == "object":
                df[c] = pd.to_numeric(df[c], errors="ignore")

        num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
        cat_cols = [c for c in df.columns if df[c].dtype == "object" or df[c].dtype.name == "category"]

        # --- 1. Detect time-like column (relaxed) ---
        ts_candidates = [c for c in df.columns if any(tok in c for tok in ["year","quarter","month","date"])]
        ts_col = ts_candidates[0] if ts_candidates else None

        if ts_col and num_cols:
            is_dt = pd.api.types.is_datetime64_any_dtype(df[ts_col])

            # Normalize quarters
            if "quarter" in ts_col:
                qmap = {1:"Q1",2:"Q2",3:"Q3",4:"Q4"}
                if pd.api.types.is_integer_dtype(df[ts_col]):
                    df[ts_col] = df[ts_col].map(qmap)
                quarter_order = ["Q1","Q2","Q3","Q4"]

            # Normalize months if integers
            if "month" in ts_col and pd.api.types.is_integer_dtype(df[ts_col]):
                month_map = {1:"Jan",2:"Feb",3:"Mar",4:"Apr",5:"May",6:"Jun",
                             7:"Jul",8:"Aug",9:"Sep",10:"Oct",11:"Nov",12:"Dec"}
                df[ts_col] = df[ts_col].map(month_map)
                month_order = list(month_map.values())

            preferred = ("sales","profit","quantity","discount","profit_margin")
            metrics = [c for c in num_cols if c in preferred] or num_cols

            n_periods = df[ts_col].nunique(dropna=True)
            use_grouped_bars = ("quarter" in ts_col) and (len(metrics) >= 2) and (n_periods <= 4)

            st.subheader("Quick chart")

            if use_grouped_bars:
                bar_metrics = [m for m in metrics if m != "profit_margin"] or metrics
                long_df = df[[ts_col] + bar_metrics].melt(id_vars=[ts_col],
                    value_vars=bar_metrics, var_name="metric", value_name="value")
                st.altair_chart(
                    alt.Chart(long_df).mark_bar().encode(
                        x=alt.X(f"{ts_col}:N", sort=quarter_order, title=ts_col.title()),
                        y="value:Q",
                        color="metric:N",
                        tooltip=[ts_col,"metric","value"],
                    ).properties(height=360),
                    use_container_width=True,
                )
                return

            # Otherwise: line(s)
            def x_enc_for(col: str):
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    return alt.X(f"{col}:T", title=col.replace("_"," ").title())
                return alt.X(f"{col}:N", title=col.replace("_"," ").title())

            plot_df = df[[ts_col] + metrics].sort_values(ts_col)

            if len(metrics) == 1:
                y = metrics[0]
                st.altair_chart(
                    alt.Chart(plot_df).mark_line(point=True).encode(
                        x=x_enc_for(ts_col),
                        y=f"{y}:Q",
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
                        y="value:Q",
                        color="metric:N",
                        tooltip=[ts_col,"metric","value"],
                    ).properties(height=320),
                    use_container_width=True,
                )
            return

        # --- 2. Categorical bar charts ---
        if cat_cols and num_cols:
            # prefer most specific categorical column
            priority = ["sub_category","customer_name","region","segment","category"]
            x = next((c for c in priority if c in cat_cols), cat_cols[0])
            ycols = [c for c in num_cols if c not in ("profit_margin",)]
            st.subheader("Quick chart")
            if len(ycols) == 1:
                y = ycols[0]
                st.altair_chart(
                    alt.Chart(df).mark_bar().encode(
                        x=alt.X(f"{x}:N", sort="-y"),
                        y=f"{y}:Q",
                        tooltip=list(df.columns),
                    ).properties(height=360),
                    use_container_width=True,
                )
            else:
                long_df = df.melt(id_vars=[x], value_vars=ycols,
                                  var_name="metric", value_name="value")
                st.altair_chart(
                    alt.Chart(long_df).mark_bar().encode(
                        x=alt.X(f"{x}:N", title=x.replace("_"," ").title()),
                        y="value:Q",
                        color="metric:N",
                        tooltip=[x,"metric","value"],
                    ).properties(height=360),
                    use_container_width=True,
                )
            return

        # --- 3. KPI (single number, no time, no category) ---
        if len(num_cols) == 1 and not cat_cols and not ts_col:
            metric = num_cols[0]
            val = df[metric].iloc[0]  # take first row
            st.subheader("Quick chart")
            st.metric(label=metric.replace("_", " ").title(), value=f"{val:,.2f}")
            return

        # --- 4. Nothing matched ---
        return

    except Exception:
        return





def render_download(pdf: pd.DataFrame, filename: str = "genie_results.csv"):
    st.download_button(
        "Download results (CSV)",
        data=pdf.to_csv(index=False).encode("utf-8"),
        file_name=filename,
        mime="text/csv"
    )
