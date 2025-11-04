import pandas as pd
from src.etl.transform import clean_orders
import pytest

def test_clean_orders_basic():
    df = pd.DataFrame({
        "order_id":[1,2,3],
        "order_date":["2025-10-01","2025-10-02","2025-10-03"],
        "customer":["A","B","C"],
        "amount":[100,0,-5]
    })
    out = clean_orders(df)
    assert len(out) == 1
    assert out.iloc[0]["amount"] == 100

def test_clean_orders_missing():
    df = pd.DataFrame({"x":[1]})
    with pytest.raises(ValueError):
        clean_orders(df)
