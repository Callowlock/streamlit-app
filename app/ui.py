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

        # --- Normalize dtypes & columns ---
        df = pdf.copy()
        lower_map = {c: c.lower() for c in df.columns}
        df.rename(columns=lower_map, inplace=True)

        # Identify columns
        time_like = [c for c in df.columns if any(k in c for k in ["order_date", "date", "month", "quarter", "year"])]
        num_cols  = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
        cat_cols  = [c for c in df.columns if c not in num_cols]

        # Parse dates where appropriate
        for c in time_like:
            if "year" in c:
                # keep numeric for year
                if not pd.api.types.is_numeric_dtype(df[c]):
                    df[c] = pd.to_numeric(df[c], errors="ignore")
            else:
                try:
                    df[c] = pd.to_datetime(df[c], errors="ignore")
                except Exception:
                    pass

        # Heuristic: choose a single time column if present
        ts_col = next((c for c in ["month", "quarter", "year", "order_date", "date"] if c in df.columns), None)

        # Avoid unreadable auto-plots
        if len(df.columns) > 20 or any(df[c].nunique() > 100 for c in cat_cols if c != (ts_col or "")):
            return

        # Helper for x encoding (fixes the YEAR bug)
        def x_enc_for(col: str):
            if col == "year":
                return alt.X(f"{col}:O", title="Year")  # categorical axis
            return alt.X(f"{col}:T", title=col.title())

        # --- Time series: line ---
        if ts_col and len(num_cols) >= 1:
            # prefer common metric aliases if present
            metrics = [c for c in num_cols if c in ("sales", "profit", "profit_margin", "quantity", "discount")] or num_cols
            plot_df = df[[ts_col] + metrics].copy().sort_values(ts_col)

            st.subheader("Quick chart")
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
                long_df = plot_df.melt(id_vars=[ts_col], value_vars=metrics, var_name="metric", value_name="value")
                st.altair_chart(
                    alt.Chart(long_df).mark_line(point=True).encode(
                        x=x_enc_for(ts_col),
                        y=alt.Y("value:Q"),
                        color="metric:N",
                        tooltip=[ts_col, "metric", "value"],
                    ).properties(height=320),
                    use_container_width=True,
                )
            return

        # --- Categorical: bar (dim + metric[s]) ---
        candidate_dims = [c for c in cat_cols if c != (ts_col or "")]
        if candidate_dims and len(num_cols) >= 1:
            dim = candidate_dims[0]
            metrics = [c for c in num_cols if c in ("sales", "profit", "profit_margin", "quantity", "discount")] or num_cols

            work = df[[dim] + metrics].copy()
            top_metric = metrics[0]
            N = 25
            work = work.sort_values(top_metric, ascending=False).head(N)

            st.subheader("Quick chart")
            if len(metrics) == 1:
                y = metrics[0]
                st.altair_chart(
                    alt.Chart(work).mark_bar().encode(
                        x=alt.X(f"{dim}:N", sort="-y"),
                        y=alt.Y(f"{y}:Q"),
                        tooltip=list(work.columns),
                    ).properties(height=360),
                    use_container_width=True,
                )
            else:
                long_df = work.melt(id_vars=[dim], value_vars=metrics, var_name="metric", value_name="value")
                st.altair_chart(
                    alt.Chart(long_df).mark_bar().encode(
                        x=alt.X(f"{dim}:N", sort="-y"),
                        y=alt.Y("value:Q"),
                        color="metric:N",
                        tooltip=[dim, "metric", "value"],
                    ).properties(height=360),
                    use_container_width=True,
                )
            return

        # --- Scatter: exactly two numeric columns ---
        if len(num_cols) == 2:
            x, y = num_cols
            st.subheader("Quick chart")
            st.altair_chart(
                alt.Chart(df[[x, y]]).mark_circle().encode(
                    x=alt.X(f"{x}:Q"),
                    y=alt.Y(f"{y}:Q"),
                    tooltip=[x, y],
                ).properties(height=360),
                use_container_width=True,
            )
            return

        # Otherwise, fail safe to table
        return

    except Exception:
        # Never block render on chart failure
        return



def render_download(pdf: pd.DataFrame, filename: str = "genie_results.csv"):
    st.download_button(
        "Download results (CSV)",
        data=pdf.to_csv(index=False).encode("utf-8"),
        file_name=filename,
        mime="text/csv"
    )
