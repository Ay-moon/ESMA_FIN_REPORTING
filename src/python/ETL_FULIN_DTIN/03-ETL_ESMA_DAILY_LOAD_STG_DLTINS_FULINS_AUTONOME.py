#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
03-ETL_ESMA_DAILY_LOAD_STG_DLTINS_FULINS_AUTONOME.py
===================================================

Loads BSV (pipe-delimited) files under <data_root>\\csv into STG tables:
- stg.ESMA_FULINS_WIDE
- stg.ESMA_DLTINS_WIDE

Rules:
- Each run: TRUNCATE TABLE then BULK INSERT files for the MAX available YYYYMMDD folder.
- Detailed logging in log.ESMA_Load_Log:
    * ScriptName
    * rows deleted before truncate (count)
    * deleted date (max folder date)
    * rows inserted after bulk
    * inserted date
- Autonomous script (no mandatory parameters).
"""

# ----------------------------
# Bootstrap import path (so "common" is importable)
# ----------------------------
import sys
from pathlib import Path

def _bootstrap_sys_path() -> Path:
    """
    Ensure <repo_root>/src/python is in sys.path so imports like:
        from common.config_loader import load_config
    work no matter from where you run the script.

    Repo root is detected as the first parent containing a "config" directory
    and "src/python" directory.
    """
    here = Path(__file__).resolve()
    print(f"[BOOTSTRAP] Script path: {here}", file=sys.stderr)
    
    for parent in here.parents:
        config_dir = parent / "config"
        src_python_dir = parent / "src" / "python"
        print(f"[BOOTSTRAP] Checking {parent}: config={config_dir.exists()}, src/python={src_python_dir.exists()}", file=sys.stderr)
        
        if config_dir.exists() and src_python_dir.exists():
            sys.path.insert(0, str(src_python_dir))
            print(f"[BOOTSTRAP] SUCCESS: Added {src_python_dir} to sys.path", file=sys.stderr)
            return parent
    
    print(f"[BOOTSTRAP] ERROR: Repo root not found (expected 'config' and 'src/python' in parents of {here})", file=sys.stderr)
    raise RuntimeError("Cannot bootstrap sys.path: repo root not found (missing 'config' or 'src/python').")

try:
    REPO_ROOT = _bootstrap_sys_path()
    print("[BOOTSTRAP] sys.path bootstrap completed", file=sys.stderr)
except Exception as e:
    print(f"[BOOTSTRAP] FAILED: {e}", file=sys.stderr)
    raise

# ----------------------------
# Standard libs
# ----------------------------
from datetime import datetime
from typing import List, Tuple
import traceback

import pyodbc

# ----------------------------
# Shared config loader (single source of truth)
# ----------------------------
from common.config_loader import load_config

# --- Identity ---
SCRIPT_NAME = Path(__file__).name

# --- Constants ---
DELIMITER = "|"

TABLE_FUL = "stg.ESMA_FULINS_WIDE"
TABLE_DLT = "stg.ESMA_DLTINS_WIDE"


# ----------------------------
# SQL
# ----------------------------
def sql_conn(cfg) -> pyodbc.Connection:
    driver = cfg["SQLSERVER"].get("driver", "ODBC Driver 17 for SQL Server")
    server = cfg["SQLSERVER"]["server"]
    database = cfg["SQLSERVER"]["database_stg"]
    user = cfg["SQLSERVER"]["user"]
    password = cfg["SQLSERVER"]["password"]
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};DATABASE={database};"
        f"UID={user};PWD={password};"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str, autocommit=True)


def sql_log_line(conn, message, element="", complement="", file_name=""):
    sql = """
    INSERT INTO log.ESMA_Load_Log
        (ScriptName, LaunchTimestamp, StartTime, Message, FileName, Element, Complement)
    VALUES (?, SYSDATETIME(), SYSDATETIME(), ?, ?, ?, ?)
    """
    cur = conn.cursor()
    cur.execute(sql, (SCRIPT_NAME, str(message)[:4000], file_name[:260], element[:200], str(complement)[:4000]))
    cur.close()


def sql_log_long(conn, message: str, element: str, complement: str = "") -> None:
    if not message:
        return
    chunk = 3500
    msg = str(message)
    total = (len(msg) + chunk - 1) // chunk
    for i in range(total):
        part = msg[i * chunk:(i + 1) * chunk]
        sql_log_line(conn, part, element=f"{element}_{i+1:02d}/{total:02d}", complement=complement)


def sql_count_rows(conn, table: str) -> int:
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    n = cur.fetchone()[0]
    cur.close()
    return int(n)


# ----------------------------
# Files
# ----------------------------
def list_bsv_files(csv_root: Path) -> Tuple[List[Path], List[Path], str, str]:
    """
    Returns FULINS and DLTINS BSV files for the MAX available YYYYMMDD folder.
    Robust: if folders do not exist, returns empty lists without raising.
    """
    ful_root = csv_root / "FULINS"
    dlt_root = csv_root / "DLTINS"

    ful_files: List[Path] = []
    dlt_files: List[Path] = []
    ful_date = ""
    dlt_date = ""

    if ful_root.exists():
        ful_dirs = sorted([p for p in ful_root.iterdir() if p.is_dir() and p.name.isdigit()])
        if ful_dirs:
            ful_date = ful_dirs[-1].name
            ful_files = sorted(ful_dirs[-1].rglob("*.bsv"))

    if dlt_root.exists():
        dlt_dirs = sorted([p for p in dlt_root.iterdir() if p.is_dir() and p.name.isdigit()])
        if dlt_dirs:
            dlt_date = dlt_dirs[-1].name
            dlt_files = sorted(dlt_dirs[-1].rglob("*.bsv"))

    return ful_files, dlt_files, ful_date, dlt_date


# ----------------------------
# Load helpers
# ----------------------------
def truncate_table(conn, table: str) -> None:
    cur = conn.cursor()
    cur.execute(f"TRUNCATE TABLE {table}")
    cur.close()


def bulk_insert(conn, table: str, file_path: Path) -> None:
    """
    Important note:
    BULK INSERT is executed by SQL Server. Therefore, SQL Server must be able to access the file path.
    - If SQL Server is local: local path works.
    - If SQL Server is remote: you likely need a UNC path (\\server\\share\\file.bsv).
    """
    sql = f"""
    BULK INSERT {table}
    FROM '{file_path}'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = '{DELIMITER}',
        ROWTERMINATOR = '0x0a',
        TABLOCK,
        KEEPNULLS,
        CODEPAGE = '65001'
    );
    """
    cur = conn.cursor()
    cur.execute(sql)
    cur.close()


# ----------------------------
# Main
# ----------------------------
def main() -> int:
    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Single-source config load (same as scripts 01/02)
    cfg = load_config()
    conn = sql_conn(cfg)

    try:
        sql_log_line(conn, "BEGIN", element="RUN", complement=f"run_ts={run_ts}")

        data_root = REPO_ROOT / "data"
        csv_root = data_root / "csv"

        # Warnings if CSV folders are missing
        if not csv_root.exists():
            sql_log_line(conn, f"WARNING - csv root folder not found: {csv_root}", element="WARN_NO_CSV_ROOT")
        if not (csv_root / "FULINS").exists():
            sql_log_line(conn, f"WARNING - FULINS csv folder not found: {csv_root / 'FULINS'}", element="WARN_NO_FULINS_CSV")
        if not (csv_root / "DLTINS").exists():
            sql_log_line(conn, f"WARNING - DLTINS csv folder not found: {csv_root / 'DLTINS'}", element="WARN_NO_DLTINS_CSV")

        ful_files, dlt_files, ful_date, dlt_date = list_bsv_files(csv_root)

        # ---- FULINS ----
        if ful_files:
            rows_before = sql_count_rows(conn, TABLE_FUL)
            truncate_table(conn, TABLE_FUL)

            sql_log_line(
                conn,
                f"FULINS_DELETE rows_deleted={rows_before} date_deleted={ful_date}",
                element="FUL_DELETE",
                complement=TABLE_FUL
            )

            for f in ful_files:
                bulk_insert(conn, TABLE_FUL, f)

            rows_after = sql_count_rows(conn, TABLE_FUL)
            sql_log_line(
                conn,
                f"FULINS_INSERT rows_inserted={rows_after} date_inserted={ful_date}",
                element="FUL_INSERT",
                complement=TABLE_FUL
            )
        else:
            sql_log_line(conn, "Skip FULINS - no files", element="FUL_SKIP")

        # ---- DLTINS ----
        if dlt_files:
            rows_before = sql_count_rows(conn, TABLE_DLT)
            truncate_table(conn, TABLE_DLT)

            sql_log_line(
                conn,
                f"DLTINS_DELETE rows_deleted={rows_before} date_deleted={dlt_date}",
                element="DLT_DELETE",
                complement=TABLE_DLT
            )

            for f in dlt_files:
                bulk_insert(conn, TABLE_DLT, f)

            rows_after = sql_count_rows(conn, TABLE_DLT)
            sql_log_line(
                conn,
                f"DLTINS_INSERT rows_inserted={rows_after} date_inserted={dlt_date}",
                element="DLT_INSERT",
                complement=TABLE_DLT
            )
        else:
            sql_log_line(conn, "Skip DLTINS - no files", element="DLT_SKIP")

        sql_log_line(conn, "END", element="RUN", complement=f"run_ts={run_ts}")
        return 0

    except Exception:
        sql_log_long(conn, traceback.format_exc(), element="TRACEBACK", complement=f"run_ts={run_ts}")
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
