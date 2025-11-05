import os
import pandas as pd
from datetime import datetime, timezone
from sqlalchemy.orm import Session
from sqlalchemy import select
from src.app.db import SessionLocal, engine
from src.app.models import Base, Order, ETLMetrics
from .transform import clean_orders

DATA_PATH = os.getenv("DATA_PATH", "data/input.csv")

def upsert_orders(session: Session, df: pd.DataFrame) -> int:
    inserted = 0
    for row in df.to_dict(orient="records"):
        exists = session.execute(
            select(Order).where(Order.order_id == row["order_id"])
        ).scalar_one_or_none()
        if exists:
            continue
        session.add(Order(**row))
        inserted += 1
    return inserted

def upsert_metrics(session: Session, inserted_count: int) -> None:
    m = session.get(ETLMetrics, 1)
    now = datetime.now(timezone.utc)
    if m is None:
        m = ETLMetrics(id=1, last_load_at=now, last_load_inserted=inserted_count)
        session.add(m)
    else:
        m.last_load_at = now
        m.last_load_inserted = inserted_count

def main():
    Base.metadata.create_all(bind=engine)
    if not os.path.exists(DATA_PATH):
        raise FileNotFoundError(f"DATA_PATH not found: {DATA_PATH}")
    raw = pd.read_csv(DATA_PATH)
    cleaned = clean_orders(raw)
    with SessionLocal() as s:
        count = upsert_orders(s, cleaned)
        upsert_metrics(s, count)
        s.commit()
        print(f"Inserted: {count}")

if __name__ == "__main__":
    main()
