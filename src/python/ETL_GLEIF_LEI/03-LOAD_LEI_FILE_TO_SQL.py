import os
import re
import csv
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any

import pyodbc


# ============================================================
# BOOTSTRAP (so "common" imports work everywhere)
# ============================================================

def _bootstrap_sys_path() -> None:
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "config").exists() and (parent / "src" / "python").exists():
            sys.path.insert(0, str(parent / "src" / "python"))
            return
    raise RuntimeError("Repo root not found (expected 'config' and 'src/python').")

_bootstrap_sys_path()

from common.config_loader import load_config  # ESMA standard


# ============================================================
# PARAMS (NO CLI)  -- BUSINESS: unchanged
# ============================================================

SCHEMA = "stg"

FILE_PATTERNS = {
    "lei2-golden": re.compile(r"gleif-goldencopy-lei2-.*__filtered\.csv$", re.IGNORECASE),
    "rr-golden": re.compile(r"gleif-goldencopy-rr-.*__filtered\.csv$", re.IGNORECASE),
    "repex-golden": re.compile(r"gleif-goldencopy-repex-.*__filtered\.csv$", re.IGNORECASE),
}

TABLES = {
    "lei2-golden": "STG_LEI_CDF_GOLDEN",
    "rr-golden": "STG_LEI_RELATION",
    "repex-golden": "STG_LEI_REPORTING_EXCEPTION",
}

TRUNCATE_ALL_TABLES_AT_START = True

FAIL_IF_FILE_HAS_UNKNOWN_COLS = True
FAIL_IF_MISSING_NOT_NULL_COLS = True

ENCODING_IN = "utf-8-sig"
BATCH_SIZE = 20000

USE_FAST_EXECUTEMANY = True


# ============================================================
# UTILS  -- BUSINESS: unchanged
# ============================================================

def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def die(msg: str, code: int = 1) -> None:
    print(f"{now_str()} [ERROR] {msg}")
    sys.exit(code)


def connect_sql(sql: Dict[str, str]) -> pyodbc.Connection:
    conn_str = (
        f"DRIVER={{{sql['driver']}}};"
        f"SERVER={sql['server']};"
        f"DATABASE={sql['database']};"
        f"UID={sql['user']};"
        f"PWD={sql['password']};"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str, autocommit=True)


def pick_latest_file(folder: Path, pattern: re.Pattern) -> Optional[Path]:
    files = [p for p in folder.glob("*.csv") if pattern.search(p.name)]
    if not files:
        return None
    return max(files, key=lambda p: p.stat().st_mtime)


def parse_file_timestamp_from_name(name: str) -> Optional[datetime]:
    m = re.search(r"(\d{8})-(\d{4})-", name)
    if not m:
        return None
    return datetime.strptime(m.group(1) + m.group(2), "%Y%m%d%H%M")


def get_table_schema(conn: pyodbc.Connection, schema: str, table: str) -> Dict[str, Dict[str, Any]]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
          COLUMN_NAME,
          DATA_TYPE,
          CHARACTER_MAXIMUM_LENGTH,
          NUMERIC_PRECISION,
          NUMERIC_SCALE,
          IS_NULLABLE,
          ORDINAL_POSITION
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
        """,
        (schema, table),
    )

    out: Dict[str, Dict[str, Any]] = {}
    for row in cur.fetchall():
        col = str(row.COLUMN_NAME)
        dtype = str(row.DATA_TYPE).lower()
        maxlen = row.CHARACTER_MAXIMUM_LENGTH
        if maxlen in (-1, None):
            maxlen = None
        else:
            maxlen = int(maxlen)

        out[col.lower()] = {
            "name": col,
            "type": dtype,
            "maxlen": maxlen,
            "precision": int(row.NUMERIC_PRECISION) if row.NUMERIC_PRECISION is not None else None,
            "scale": int(row.NUMERIC_SCALE) if row.NUMERIC_SCALE is not None else None,
            "is_nullable": (str(row.IS_NULLABLE).upper() == "YES"),
            "ordinal": int(row.ORDINAL_POSITION),
        }

    cur.close()
    if not out:
        raise ValueError(f"Table not found or no columns: {schema}.{table}")
    return out


def truncate_table(conn: pyodbc.Connection, schema: str, table: str) -> None:
    cur = conn.cursor()
    cur.execute(f"TRUNCATE TABLE [{schema}].[{table}]")
    cur.close()


def safe_truncate_str(v: str, maxlen: Optional[int]) -> str:
    if maxlen is None:
        return v
    return v if len(v) <= maxlen else v[:maxlen]


def normalize_raw(raw: Optional[str]) -> Optional[str]:
    if raw is None:
        return None
    v = raw.strip()
    if v == "":
        return None
    return v


def build_insert_sql_all_text(
    schema: str,
    table: str,
    insert_cols: List[str],
    table_cols: Dict[str, Dict[str, Any]],
) -> str:
    """
    Build INSERT where ALL params are ?, but for datetime/decimal we use TRY_CONVERT on SQL side.
    => Avoid ODBC decimal binding => avoids HY104.
    """
    col_list_sql = ", ".join(f"[{c}]" for c in insert_cols)

    values_expr = []
    for c in insert_cols:
        meta = table_cols[c.lower()]
        dtype = meta["type"]

        if dtype in ("datetime", "datetime2", "smalldatetime", "date", "time"):
            values_expr.append("TRY_CONVERT(datetime2(0), ?)")
        elif dtype in ("decimal", "numeric"):
            p = meta["precision"] or 20
            s = meta["scale"] or 6
            values_expr.append(f"TRY_CONVERT(decimal({p},{s}), ?)")
        else:
            values_expr.append("?")

    placeholders = ", ".join(values_expr)
    return f"INSERT INTO [{schema}].[{table}] ({col_list_sql}) VALUES ({placeholders})"


def apply_setinputsizes_all_as_text(cur: pyodbc.Cursor, insert_cols: List[str], table_cols: Dict[str, Dict[str, Any]]) -> None:
    """
    Force ALL params to be sent as NVARCHAR buffers (safe large),
    to avoid 'buffer truncation' with fast_executemany.
    """
    sizes = []
    for c in insert_cols:
        meta = table_cols[c.lower()]
        maxlen = meta["maxlen"]
        L = maxlen if (maxlen is not None and maxlen > 0) else 4000
        sizes.append((pyodbc.SQL_WVARCHAR, L))
    cur.setinputsizes(sizes)


def build_insert_plan(
    table_cols: Dict[str, Dict[str, Any]],
    file_headers: List[str],
    file_key: str,
    csv_path: Path,
    script_name: str,
    launch_ts: datetime,
) -> Tuple[List[str], Dict[str, Any]]:
    table_lower = set(table_cols.keys())

    unknown = [h for h in file_headers if h.lower() not in table_lower]
    if unknown and FAIL_IF_FILE_HAS_UNKNOWN_COLS:
        raise ValueError(
            f"{file_key}: File contains columns not in table {SCHEMA}.{TABLES[file_key]}:\n"
            + "\n".join(unknown)
        )

    file_ts = parse_file_timestamp_from_name(csv_path.name) or launch_ts
    meta_values = {
        "scriptname": script_name,
        "launchtimestamp": launch_ts.strftime("%Y-%m-%d %H:%M:%S"),
        "sourcefilename": csv_path.name,
        "sourcefiletimestamp": file_ts.strftime("%Y-%m-%d %H:%M:%S"),
        "sourceurl": None,
    }

    meta_cols_present = [k for k in meta_values.keys() if k in table_lower]
    file_cols_present = [h for h in file_headers if h.lower() in table_lower]

    if FAIL_IF_MISSING_NOT_NULL_COLS:
        missing_nn = []
        for col_l, meta in table_cols.items():
            if meta["is_nullable"]:
                continue
            if col_l in meta_cols_present:
                continue
            if any(h.lower() == col_l for h in file_cols_present):
                continue
            missing_nn.append(meta["name"])

        if missing_nn:
            raise ValueError(
                f"{file_key}: Missing NOT NULL required columns for {SCHEMA}.{TABLES[file_key]}:\n"
                + "\n".join(missing_nn)
            )

    supply = set(meta_cols_present) | set(h.lower() for h in file_cols_present)

    insert_cols = [
        table_cols[c]["name"]
        for c in sorted(table_cols.keys(), key=lambda k: table_cols[k]["ordinal"])
        if c in supply
    ]

    return insert_cols, meta_values


def load_csv_to_table(
    conn: pyodbc.Connection,
    schema: str,
    table: str,
    file_key: str,
    csv_path: Path,
    script_name: str,
    launch_ts: datetime,
) -> int:
    table_cols = get_table_schema(conn, schema, table)

    with csv_path.open("r", encoding=ENCODING_IN, newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            raise ValueError(f"No header found: {csv_path.name}")

        insert_cols, meta_values = build_insert_plan(
            table_cols, reader.fieldnames, file_key, csv_path, script_name, launch_ts
        )

        insert_sql = build_insert_sql_all_text(schema, table, insert_cols, table_cols)

        cur = conn.cursor()
        if USE_FAST_EXECUTEMANY:
            cur.fast_executemany = True
            apply_setinputsizes_all_as_text(cur, insert_cols, table_cols)

        buffer: List[Tuple[Any, ...]] = []
        inserted = 0

        for row in reader:
            values: List[Any] = []
            for col_actual in insert_cols:
                col_l = col_actual.lower()
                meta = table_cols[col_l]
                dtype = meta["type"]

                if col_l in meta_values:
                    mv = meta_values[col_l]
                    if isinstance(mv, str) and dtype in ("nvarchar", "varchar", "nchar", "char"):
                        mv = safe_truncate_str(mv, meta["maxlen"])
                    values.append(mv)
                    continue

                raw = None
                if col_actual in row:
                    raw = row.get(col_actual)
                else:
                    for k in row.keys():
                        if k.lower() == col_l:
                            raw = row.get(k)
                            break

                v = normalize_raw(raw)
                if v is not None and dtype in ("nvarchar", "varchar", "nchar", "char"):
                    v = safe_truncate_str(v, meta["maxlen"])

                values.append(v)

            buffer.append(tuple(values))

            if len(buffer) >= BATCH_SIZE:
                cur.executemany(insert_sql, buffer)
                inserted += len(buffer)
                buffer.clear()

        if buffer:
            cur.executemany(insert_sql, buffer)
            inserted += len(buffer)

        cur.close()
        return inserted


# ============================================================
# MAIN  -- only config/path refactor added
# ============================================================

def main():
    script_name = Path(__file__).name
    launch_ts = datetime.now()

    # --- NEW: Load config via shared loader (ESMA standard)
    cfg = load_config()

    # --- NEW: Filtered directory derived from config
    try:
        gleif_csv_dir = Path(cfg.get("GLEIF_CSV", "directory")).expanduser().resolve()
    except Exception:
        die("Missing config value: [GLEIF_CSV] directory (in config/config.ini or config.template.ini)")

    filtered_dir = gleif_csv_dir / "filtered"

    if not filtered_dir.exists():
        die(f"Filtered directory not found: {filtered_dir}")

    # Latest files (BUSINESS unchanged)
    latest: Dict[str, Path] = {}
    for key, pat in FILE_PATTERNS.items():
        p = pick_latest_file(filtered_dir, pat)
        if p is None:
            die(f"Missing filtered CSV for {key} in {filtered_dir}")
        latest[key] = p

    # --- NEW: SQL config derived from cfg (ESMA standard)
    try:
        sql_cfg = {
            "server": cfg.get("SQLSERVER", "server"),
            "database": cfg.get("SQLSERVER", "database_stg"),
            "user": cfg.get("SQLSERVER", "user"),
            "password": cfg.get("SQLSERVER", "password"),
            "driver": cfg.get("SQLSERVER", "driver", fallback="ODBC Driver 17 for SQL Server"),
        }
    except Exception:
        die("Missing SQLSERVER config keys (server/database_stg/user/password[/driver]) in config.ini")

    print(f"{now_str()} [OK] CONFIG - Using config: config/config.ini (or config.template.ini)")
    print(f"{now_str()} [OK] DB     - {sql_cfg['database']} | schema={SCHEMA}")
    print(f"{now_str()} [OK] PATH   - {filtered_dir}")
    for k, p in latest.items():
        print(f"{now_str()} [OK] FILE   - {k}: {p.name}")

    conn = connect_sql(sql_cfg)
    try:
        if TRUNCATE_ALL_TABLES_AT_START:
            print(f"\n{now_str()} [STEP] TRUNCATE ALL TABLES (start)")
            truncate_table(conn, SCHEMA, TABLES["lei2-golden"])
            truncate_table(conn, SCHEMA, TABLES["rr-golden"])
            truncate_table(conn, SCHEMA, TABLES["repex-golden"])
            print(f"{now_str()} [OK] TRUNCATE ALL DONE")

        for file_key in ["lei2-golden", "rr-golden", "repex-golden"]:
            csv_path = latest[file_key]
            table = TABLES[file_key]

            print(f"\n{now_str()} [STEP] LOAD {file_key} -> {SCHEMA}.{table} from {csv_path.name}")
            cnt = load_csv_to_table(
                conn=conn,
                schema=SCHEMA,
                table=table,
                file_key=file_key,
                csv_path=csv_path,
                script_name=script_name,
                launch_ts=launch_ts,
            )
            print(f"{now_str()} [OK] LOADED {file_key}: {cnt} rows")

        print(f"\n{now_str()} [DONE] All filtered files loaded successfully.")
    finally:
        conn.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        die(str(e))
