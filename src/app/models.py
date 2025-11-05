from sqlalchemy.orm import declarative_base, Mapped, mapped_column
from sqlalchemy import String, Integer, Date, Numeric, DateTime
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

# ★ ETL実行状況を保持する単一行テーブル
class ETLMetrics(Base):
    __tablename__ = "etl_metrics"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)  # 常に1で運用
    last_load_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_load_inserted: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
