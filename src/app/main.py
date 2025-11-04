from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session
from sqlalchemy import func, select
from .db import SessionLocal, engine
from .models import Base, Order

from datetime import date
from fastapi import Query

app = FastAPI(title="Mini ETL + API")
Base.metadata.create_all(bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/orders/summary")
def orders_summary(db: Session = Depends(get_db)):
    stmt = (
        select(
            Order.customer.label("customer"),
            func.count().label("count"),
            func.sum(Order.amount).label("amount_sum"),
        )
        .group_by(Order.customer)
        .order_by(func.sum(Order.amount).desc())
    )
    rows = db.execute(stmt).mappings().all()
    return {"summary": [dict(r) for r in rows]}

@app.get("/orders/daily")
def orders_daily(
    db: Session = Depends(get_db),
    start: date | None = Query(None),
    end: date | None = Query(None),
):
    q = select(
        Order.order_date.label("order_date"),
        func.sum(Order.amount).label("amount_sum"),
        func.count().label("count"),
    ).group_by(Order.order_date).order_by(Order.order_date)

    if start:
        q = q.filter(Order.order_date >= start)
    if end:
        q = q.filter(Order.order_date <= end)

    rows = db.execute(q).mappings().all()
    return {"daily": [dict(r) for r in rows]}