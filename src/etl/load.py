from __future__ import annotations

import argparse
import csv
import json
import logging
import os
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Sequence

from sqlalchemy import MetaData, Table, create_engine, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Engine, Result
from sqlalchemy.exc import NoSuchTableError, SQLAlchemyError

# -----------------------------------------------------------------------------
# Settings
# -----------------------------------------------------------------------------
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg://app:app@db:5432/appdb",
)
TABLE_NAME = os.getenv("ORDER_TABLE", "orders")
UNIQUE_KEY = os.getenv("ORDER_UNIQUE_KEY", "order_id")  # 例: orders(order_id) に UNIQUE か PK が必要
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# CSV ソース（任意）
# - 明示指定があればそれを読む
# - なければソースは 0 件として進む（動作確認やパイプライン検証用）
CSV_SOURCE = os.getenv("ETL_SOURCE_FILE")  # 例: /app/data/orders.csv

# 変更しない列（更新対象から除外したいカラム名。created_at など）
DO_NOT_UPDATE_COLS = set(os.getenv("DO_NOT_UPDATE_COLS", "created_at").split(","))

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
log = logging.getLogger("etl.load")


def _mask_dsn(url: str) -> str:
    # app:app@ を ***:***@ に置換（最低限のマスキング）
    return url.replace("://app:app@", "://***:***@")


# -----------------------------------------------------------------------------
# DB helpers
# -----------------------------------------------------------------------------
def get_engine() -> Engine:
    return create_engine(DATABASE_URL, future=True)


def reflect_table(engine: Engine, table_name: str) -> Table:
    md = MetaData()
    try:
        return Table(table_name, md, autoload_with=engine)
    except NoSuchTableError as e:
        raise RuntimeError(
            f"Table '{table_name}' not found. Did you run migrations?"
        ) from e


def ensure_unique_key_exists(tbl: Table, unique_col: str) -> None:
    # 軽い検査：指定列が存在するかだけ（UNIQUE 制約の有無までは DB メタからは簡単に断定しない）
    if unique_col not in tbl.c:
        raise RuntimeError(
            f"Unique key column '{unique_col}' does not exist in table '{tbl.name}'."
        )


# -----------------------------------------------------------------------------
# Source readers
# -----------------------------------------------------------------------------
def read_csv_rows(path: str, limit: Optional[int] = None) -> List[Dict]:
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    rows: List[Dict] = []
    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            rows.append(dict(row))
            if limit is not None and len(rows) >= limit:
                break
    return rows


def fetch_source_rows(args: argparse.Namespace, table: Table) -> List[Dict]:
    """
    取り込み元を返す。ここをあなたの実案件に合わせて差し替えればよい。
    既定は CSV（ETL_SOURCE_FILE or --source）、未指定なら空配列。
    - テーブルの既存カラム以外のキーは落とす（INSERT時の未知カラムエラー回避）
    - --since があれば、行中に 'updated_at' or 'order_date' があればフィルタ試行（ISO8601文字列前提）
    """
    source_path = args.source or CSV_SOURCE
    if not source_path:
        log.warning("No source specified (ETL_SOURCE_FILE/--source). Proceeding with 0 rows.")
        return []

    rows = read_csv_rows(source_path, limit=args.limit)

    # テーブルに存在するカラムだけ残す
    valid_cols = {c.name for c in table.columns}
    cleaned: List[Dict] = []
    for r in rows:
        cleaned.append({k: v for k, v in r.items() if k in valid_cols})

    # since フィルタ（任意）
    if args.since:
        since_dt = datetime.fromisoformat(args.since)
        def _candidate_ts(row: Dict) -> Optional[str]:
            # よくある列名を順に見る
            for key in ("updated_at", "order_date", "created_at"):
                if key in row and row[key]:
                    return row[key]
            return None

        filtered = []
        for r in cleaned:
            ts = _candidate_ts(r)
            try:
                if ts and datetime.fromisoformat(str(ts)) >= since_dt:
                    filtered.append(r)
            except ValueError:
                # 解析できない日付は通さない
                pass
        log.info("since filter applied: %s -> %s rows", len(cleaned), len(filtered))
        cleaned = filtered

    return cleaned


# -----------------------------------------------------------------------------
# Upsert
# -----------------------------------------------------------------------------


def build_upsert_stmt(table: Table, rows: Sequence[Dict], unique_key: str):
    """
    PostgreSQL ネイティブ UPSERT:
      INSERT ... ON CONFLICT (unique_key) DO UPDATE SET ...
    かつ RETURNING で inserted_flag を返して、INSERT/UPDATE を判定する。
    """
    stmt = pg_insert(table).values(rows)

    # 更新対象の列を決める（ユニークキーと DO_NOT_UPDATE_COLS を除外）
    excluded = stmt.excluded
    update_cols = {}
    for c in table.columns:
        name = c.name
        if name == unique_key:
            continue
        if name in DO_NOT_UPDATE_COLS:
            continue
        update_cols[name] = excluded[name]

    stmt = stmt.on_conflict_do_update(
        index_elements=[unique_key],
        set_=update_cols,
    ).returning(table.c.id, text("xmax"))

    return stmt


def execute_upsert(engine: Engine, table: Table, rows: Sequence[Dict], unique_key: str) -> Dict[str, int]:
    if not rows:
        return {"inserted": 0, "updated": 0}

    stmt = build_upsert_stmt(table, rows, unique_key)

    inserted = 0
    updated = 0

    with engine.begin() as conn:
        result: Result = conn.execute(stmt)

        # `RETURNING` で新規挿入された行のIDとxmaxを取得
        inserted_rows = result.fetchall()
        log.info(f"DEBUG: {inserted_rows}")
        # xmax が 0 の場合は INSERT、それ以外は UPDATE
        for row in inserted_rows:
            
            row_id, xmax_val = row
            #log.info(f"xmax_val: {xmax_val}")
            if int(xmax_val) == 0:
                #log.info(f"inserted++")
                inserted += 1
            else:
                #log.info(f"updated++")
                updated += 1

        log.info(f"Inserted {inserted} rows")
        log.info(f"Updated {updated} rows")
    return {"inserted": inserted, "updated": updated}



# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Mini-ETL loader (PostgreSQL UPSERT).")
    parser.add_argument("--dry-run", action="store_true", help="Do not write to DB.")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of source rows.")
    parser.add_argument("--source", type=str, default=None, help="Path to CSV source file.")
    parser.add_argument("--since", type=str, default=None, help="Filter rows >= ISO8601 datetime.")
    parser.add_argument("-v", "--verbose", action="store_true", help="Set log level to DEBUG.")
    args = parser.parse_args(list(argv) if argv is not None else None)

    if args.verbose:
        logging.getLogger().setLevel("DEBUG")

    log.info("Starting ETL at %s", datetime.now(timezone.utc).isoformat())
    log.info("DB URL (masked): %s", _mask_dsn(DATABASE_URL))

    engine = get_engine()

    # つながる先の current_schema を記録
    try:
        with engine.connect() as conn:
            current_schema = conn.execute(text("SELECT current_schema();")).scalar()
            log.info("current_schema=%s", current_schema)
    except SQLAlchemyError:
        log.exception("DB connection failed")
        raise

    # テーブルリフレクション
    table = reflect_table(engine, TABLE_NAME)
    ensure_unique_key_exists(table, UNIQUE_KEY)

    # ソース取得
    rows = fetch_source_rows(args, table)
    log.info("source_rows=%d", len(rows))
    if rows[:3]:
        # サンプル 3 行だけログ
        log.info("sample_rows=%s", json.dumps(rows[:3], ensure_ascii=False))

    if args.dry_run:
        log.info("Dry-run mode. No DB writes will be performed.")
        print("Inserted: 0")
        return 0

    # UPSERT
    try:
        counts = execute_upsert(engine, table, rows, UNIQUE_KEY)
        total = counts["inserted"] + counts["updated"]
        log.info("UPSERT result: %s", counts)
        return 0
    except SQLAlchemyError:
        log.exception("ETL failed during UPSERT")
        raise


if __name__ == "__main__":
    raise SystemExit(main())
