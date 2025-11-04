import os
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import select
from src.app.db import SessionLocal, engine
from src.app.models import Base, Order
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

def main():
    Base.metadata.create_all(bind=engine)
    raw = pd.read_csv(DATA_PATH)
    cleaned = clean_orders(raw)
    with SessionLocal() as s:
        count = upsert_orders(s, cleaned)
        s.commit()
        print(f"Inserted: {count}")

if __name__ == "__main__":
    main()
