# util/common.py
from __future__ import annotations
import pandas as pd
from decimal import Decimal

# ---- Channel policy ----
ALLOW_CHANNELS = ("online", "retail", "wholesale")
EXCLUDE_CHANNELS = ("others",)  # 必要に応じて拡張

def to_month_start(s) -> pd.Series:
    s = pd.to_datetime(s, errors="coerce")
    return s.values.astype("datetime64[M]").astype("datetime64[ns]")

def ensure_float_cols(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        if c in out.columns:
            out[c] = out[c].apply(lambda x: float(x) if isinstance(x, Decimal) else x)
            out[c] = pd.to_numeric(out[c], errors="coerce")
        else:
            out[c] = 0.0
    return out.astype({c: "float64" for c in cols})

def ensure_channel_column(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    if "channel" not in d.columns:
        d["channel"] = pd.NA
    d["channel"] = d["channel"].astype("string")
    return d

def drop_excluded_channels(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    if "channel" not in d.columns:
        return d
    mm = d["channel"].astype(str).str.strip().str.lower()
    return d[~mm.isin(EXCLUDE_CHANNELS)].copy()

def keep_only_allowed_channels(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    if "channel" not in d.columns:
        return d
    mm = d["channel"].astype(str).str.strip().str.lower()
    return d[mm.isin(ALLOW_CHANNELS)].copy()

def key_sort(df: pd.DataFrame, keys=("sku","channel","month")) -> pd.DataFrame:
    return df.sort_values(list(keys)).reset_index(drop=True)
