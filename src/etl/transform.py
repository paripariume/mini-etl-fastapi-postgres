import pandas as pd

def clean_orders(df: pd.DataFrame) -> pd.DataFrame:
    required = {"order_id", "order_date", "customer", "amount"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"missing columns: {missing}")

    out = df.copy()
    out["order_id"] = out["order_id"].astype(int)
    out["order_date"] = pd.to_datetime(out["order_date"]).dt.date
    out["customer"] = out["customer"].astype(str).str.strip()
    out["amount"] = pd.to_numeric(out["amount"], errors="coerce").fillna(0).round(2)

    # 0以下は除外（ビジネスルール例）
    out = out[out["amount"] > 0]
    return out
