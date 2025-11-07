# src/app/models.py
from sqlalchemy.orm import declarative_base, Mapped, mapped_column
from sqlalchemy import String, Integer, Date, Numeric, DateTime, Text, UniqueConstraint
from datetime import date, datetime
from decimal import Decimal

Base = declarative_base()

class Order(Base):
    __tablename__ = "orders"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    order_id: Mapped[int] = mapped_column(Integer, unique=True, index=True, nullable=False)
    order_date: Mapped[date] = mapped_column(Date, nullable=False)
    customer: Mapped[str] = mapped_column(String(50), nullable=False)
    amount: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)

class ETLMetrics(Base):
    __tablename__ = "etl_metrics"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)  # 常に1
    last_load_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_load_inserted: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_load_status: Mapped[str] = mapped_column(String(16), nullable=False, default="OK")  # OK / FAILED
    last_error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    __table_args__ = (UniqueConstraint("id", name="uq_etl_metrics_id"),)
