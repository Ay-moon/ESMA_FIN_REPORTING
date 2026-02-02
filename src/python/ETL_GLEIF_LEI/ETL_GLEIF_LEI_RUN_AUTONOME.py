# ETL_GLEIF_LEI_RUN_AUTONOME.py
# Orchestrator: runs 01 -> 02 -> 03, and logs into existing table [log].[ESMA_Load_Log]
#
# This version is aligned with the ESMA standard used by your 3 scripts:
# - Bootstrap repo root (so "common" imports work everywhere)
# - Load config via common.config_loader.load_config()
# - Use SQLSERVER section from config to log in [log].[ESMA_Load_Log]

from __future__ import annotations

import socket
import subprocess
import sys
import traceback
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Tuple


# ============================================================
# BOOTSTRAP (so "common" imports work everywhere)  -- SAME AS YOUR SCRIPTS
# ============================================================

def _bootstrap_sys_path() -> None:
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "config").exists() and (parent / "src" / "python").exists():
            sys.path.insert(0, str(parent / "src" / "python"))
            return
    raise RuntimeError("Repo root not found (expected 'config' and 'src/python').")

_bootstrap_sys_path()

from common.config_loader import load_config  # ESMA standard (same as your scripts)  # noqa: E402


# -----------------------------
# SCRIPTS (same folder)
# -----------------------------
SCRIPT_01 = "01-LOAD_LEI_FILE.py"
SCRIPT_02 = "02-CREATE_NEW_LEI_FILE.py"
SCRIPT_03 = "03-LOAD_LEI_FILE_TO_SQL.py"

LOG_SCHEMA_FALLBACK = "log"
LOG_TABLE = "ESMA_Load_Log"

# Column max lengths from your table structure
MAX_SCRIPTNAME = 255
MAX_MESSAGE = 4000
MAX_FILENAME = 4000
MAX_ELEMENT = 255
MAX_COMPLEMENT = 1000


# -----------------------------
# Helpers
# -----------------------------
def now_utc_naive_dt2_0() -> datetime:
    # SQL datetime2(0) compatible (no tz, seconds precision)
    return datetime.utcnow().replace(microsecond=0)

def trunc(s: Optional[str], n: int) -> Optional[str]:
    if s is None:
        return None
    return s if len(s) <= n else s[:n]

def run_step(script_path: Path) -> Tuple[int, str, str]:
    proc = subprocess.run(
        [sys.executable, str(script_path)],
        capture_output=True,
        text=True,
        check=False,
    )
    return proc.returncode, proc.stdout or "", proc.stderr or ""


# -----------------------------
# SQL Logging: [log].[ESMA_Load_Log]
# -----------------------------
def sql_connect(sql: Dict[str, str]):
    try:
        import pyodbc  # type: ignore
    except ImportError:
        raise RuntimeError("pyodbc not installed. Install with: py -m pip install pyodbc")

    conn_str = (
        f"DRIVER={{{sql['driver']}}};"
        f"SERVER={sql['server']};"
        f"DATABASE={sql['database']};"
        f"UID={sql['user']};"
        f"PWD={sql['password']};"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str, autocommit=False)

def insert_log_start(cur, schema_log: str, script_name: str, message: str, element: str, complement: str) -> int:
    """
    Insert a single row and return LogID.
    Table structure expected (as you showed):
      LogID (PK, int, IDENTITY)
      ScriptName (nvarchar(255), not null)
      LaunchTimestamp (datetime2(0), not null)
      StartTime (datetime2(0), null)
      EndTime (datetime2(0), null)
      Message (nvarchar(4000), null)
      FileName (nvarchar(4000), null)
      Element (nvarchar(255), null)
      Complement (nvarchar(1000), null)
      CreatedOn (datetime2(0), null)
    """
    now = now_utc_naive_dt2_0()
    host = socket.gethostname()

    sql = f"""
INSERT INTO [{schema_log}].[{LOG_TABLE}]
    (ScriptName, LaunchTimestamp, StartTime, Message, Element, Complement, CreatedOn)
OUTPUT INSERTED.LogID
VALUES (?, ?, ?, ?, ?, ?, ?);
"""
    row = cur.execute(
        sql,
        (
            trunc(script_name, MAX_SCRIPTNAME),
            now,  # LaunchTimestamp
            now,  # StartTime
            trunc(message, MAX_MESSAGE),
            trunc(element, MAX_ELEMENT),
            trunc(f"{complement} | host={host}", MAX_COMPLEMENT),
            now,  # CreatedOn
        ),
    ).fetchone()
    return int(row[0])

def update_log_end(cur, schema_log: str, log_id: int, message: str, complement: str) -> None:
    now = now_utc_naive_dt2_0()
    sql = f"""
UPDATE [{schema_log}].[{LOG_TABLE}]
SET
    EndTime = ?,
    Message = ?,
    Complement = ?
WHERE LogID = ?;
"""
    cur.execute(
        sql,
        (
            now,
            trunc(message, MAX_MESSAGE),
            trunc(complement, MAX_COMPLEMENT),
            log_id,
        ),
    )


def load_runtime_sql_config() -> Tuple[Dict[str, str], str]:
    """
    Read SQLSERVER config from shared loader (same standard as your scripts).
    """
    cfg = load_config()

    # Required keys aligned with your scripts:
    # server, database_stg, user, password (+ optional driver, schema_log)
    sql = {
        "server": cfg.get("SQLSERVER", "server"),
        "database": cfg.get("SQLSERVER", "database_stg"),
        "user": cfg.get("SQLSERVER", "user"),
        "password": cfg.get("SQLSERVER", "password"),
        "driver": cfg.get("SQLSERVER", "driver", fallback="ODBC Driver 17 for SQL Server"),
        "schema_log": cfg.get("SQLSERVER", "schema_log", fallback=LOG_SCHEMA_FALLBACK),
    }

    return sql, sql["schema_log"]


def main() -> int:
    here = Path(__file__).resolve().parent
    steps = [here / SCRIPT_01, here / SCRIPT_02, here / SCRIPT_03]

    missing = [p.name for p in steps if not p.exists()]
    if missing:
        raise FileNotFoundError(
            "Missing ETL scripts (expected in same folder as orchestrator):\n- " + "\n- ".join(missing)
        )

    orchestrator_name = Path(__file__).name

    # Load SQL config via shared loader (ESMA standard)
    sql_cfg, schema_log = load_runtime_sql_config()

    conn = sql_connect(sql_cfg)
    try:
        cur = conn.cursor()

        log_id = insert_log_start(
            cur=cur,
            schema_log=schema_log,
            script_name=orchestrator_name,
            message="START ORCHESTRATION",
            element="ORCHESTRATOR",
            complement=" -> ".join(p.name for p in steps),
        )
        conn.commit()

        all_stdout = []
        all_stderr = []

        try:
            for p in steps:
                code, out, err = run_step(p)
                all_stdout.append(f"\n===== {p.name} (stdout) =====\n{out}")
                all_stderr.append(f"\n===== {p.name} (stderr) =====\n{err}")

                if code != 0:
                    raise RuntimeError(f"Step failed: {p.name} (exit code {code})")

            # SUCCESS
            msg = "SUCCESS ORCHESTRATION"
            complement = ("".join(all_stdout)[-600:] + "\n" + "".join(all_stderr)[-350:]).strip()
            if not complement:
                complement = "All steps completed successfully."

            cur = conn.cursor()
            update_log_end(cur, schema_log, log_id, msg, complement)
            conn.commit()
            return 0

        except Exception:
            msg = "FAILED ORCHESTRATION"
            tb = traceback.format_exc()
            complement = (("".join(all_stderr)[-500:]).strip() + "\n--- ERROR ---\n" + tb).strip()

            cur = conn.cursor()
            update_log_end(cur, schema_log, log_id, msg, complement)
            conn.commit()
            raise

    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
