import re
from config.settings import FQTN

_BANNED_VERBS = re.compile(
    r"\b("
    r"insert|update|delete|merge|drop|alter|grant|revoke|truncate|"
    r"call|copy|create|replace|refresh|optimize|vacuum|set|use|"
    r"comment|analyze|msck|repair|restore|snapshot|reorg"
    r")\b",
    flags=re.IGNORECASE,
)

def is_safe_select(sql_text: str) -> bool:
    s = " ".join(sql_text.strip().lower().split())
    if ";" in s or "--" in s or "/*" in s or "*/" in s:
        return False
    if not (s.startswith("select") or s.startswith("with")):
        return False
    if _BANNED_VERBS.search(s):
        return False
    if s.startswith("with") and " select " not in f" {s} ":
        return False
    return True

def expand_table(sql_text: str) -> str:
    """Allow ad-hoc SQL to use {FQTN} like our f-strings do."""
    return sql_text.replace("{FQTN}", FQTN)