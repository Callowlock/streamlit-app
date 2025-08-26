import re

def translate(nl_query: str, fqtn: str, data_min, data_max) -> str:
    """
    Port of your genie_to_sql() with identical behavior, except:
    - data_min/data_max are passed in (no Streamlit cache dependency here)
    """
    q = " ".join(nl_query.strip().lower().split())

    metric_alias = {
        "sales": "sales", "revenue": "sales", "profit": "profit",
        "quantity": "quantity", "qty": "quantity", "discount": "discount",
        "profit margin": "profit_margin", "margin": "profit_margin", "profit %": "profit_margin",
    }
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
    region_vals  = {"west":"West", "east":"East", "central":"Central", "south":"South"}
    segment_vals = {"consumer":"Consumer", "corporate":"Corporate",
                    "home office":"Home Office", "homeoffice":"Home Office"}

    def pick_metric(text: str) -> str:
        if "profit margin" in text or "profit %" in text:  # prefer two-word match
            return "profit_margin"
        for k, v in metric_alias.items():
            if re.search(rf"\b{k}\b", text):
                return v
        return "sales"

    def pick_grain(text: str) -> str | None:
        if re.search(r"\bby month\b", text):   return "month"
        if re.search(r"\bby quarter\b", text): return "quarter"
        if re.search(r"\bby year\b", text):    return "year"
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
        if y < data_min.year or y > data_max.year:
            raise ValueError(f"No data for {y}. Data covers {data_min} to {data_max}.")
        return f"year(order_date) = {y}"

    def last_n_months_filter(text: str) -> str | None:
        m = re.search(r"\blast\s+(\d+)\s+months?\b", text)
        if not m:
            return None
        n = int(m.group(1))
        return (
            f"order_date BETWEEN add_months(date '{data_max}', -{n}) AND date '{data_max}'"
        )

    def region_filter(text: str) -> str | None:
        m = re.search(r"\bin\s+(west|east|central|south)\b", text)
        if m:
            return f"region = '{region_vals[m.group(1)]}'"
        return None

    def segment_filter(text: str) -> str | None:
        if re.search(r"\bby\s+segment(s)?\b", text):
            return None
        m = re.search(r"\bsegment(s)?\s+(consumer|corporate|home[ _]?office)s?\b", text)
        if m:
            v = m.group(2).replace(" ", "").lower()
            return {
                "consumer":   "segment = 'Consumer'",
                "corporate":  "segment = 'Corporate'",
                "homeoffice": "segment = 'Home Office'",
            }[v]
        return None

    def where_clause(parts: list[str]) -> str:
        parts = [p for p in parts if p]
        return ("WHERE " + " AND ".join(parts)) if parts else ""

    metric = pick_metric(q)
    grain  = pick_grain(q)
    dim    = pick_dim(q)
    topn   = pick_topn(q)

    if topn and not dim:
        if re.search(r"\bproducts?\b", q):   dim = "product_name"
        elif re.search(r"\bcustomers?\b", q): dim = "customer_name"

    filters = [
        year_filter(q),
        last_n_months_filter(q),
        region_filter(q),
        segment_filter(q),
    ]
    where = where_clause(filters)

    def agg_expr(m: str) -> str:
        if m == "profit_margin":
            return ("SUM(profit) AS profit, "
                    "SUM(sales)  AS sales, "
                    "CASE WHEN SUM(sales)=0 THEN NULL ELSE SUM(profit)/SUM(sales) END AS profit_margin")
        elif m in ("sales", "profit", "quantity", "discount"):
            return f"SUM({m}) AS {m}"
        return "SUM(sales) AS sales"

    def order_by_for(m: str, key_alias: str) -> str:
        if key_alias in ("month", "quarter", "year", "period"):
            return f"ORDER BY {key_alias}"
        if m == "profit_margin":
            return "ORDER BY profit_margin DESC NULLS LAST"
        return f"ORDER BY {m} DESC"

    if grain:
        dt = {"month":"month", "quarter":"quarter", "year":"year"}[grain]
        trunc = {
            "month":   "date_trunc('month', order_date)",
            "quarter": "date_trunc('quarter', order_date)",
            "year":    "date_trunc('year', order_date)",
        }[grain]
        wants_multi = ("sales and profit" in q) or ("profit and sales" in q)
        agg = "SUM(sales) AS sales, SUM(profit) AS profit" if (wants_multi and metric in ("sales","profit")) else agg_expr(metric)
        return f"""
        SELECT {trunc} AS {dt}, {agg}
        FROM {fqtn}
        {where}
        GROUP BY {dt}
        {order_by_for(metric, dt)}
        """.strip()

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

    return f"SELECT * FROM {fqtn} LIMIT 100"
