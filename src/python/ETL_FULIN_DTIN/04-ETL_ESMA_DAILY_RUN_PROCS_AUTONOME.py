#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
04-ETL_ESMA_DAILY_RUN_PROCS_AUTONOME_V3.py
=========================================

Fix rowcount KO :
Tu n'as pas les droits SELECT sur les tables mart.* (erreur 229), donc COUNT_BIG(*) échoue.
Et ta proc SQL utilise sys.dm_db_partition_stats qui peut aussi renvoyer 0 sans VIEW DATABASE STATE.

=> V3 calcule les rowcounts avec une stratégie "fallback" sans SELECT sur les tables :
1) COUNT_BIG(*) FROM <table>                     (si droit SELECT)
2) sys.dm_db_partition_stats (VIEW DB STATE)     (si droit)
3) sys.partitions / sys.objects (métadonnées)    (souvent OK sans SELECT table)

Les logs indiquent la méthode utilisée : method=COUNT | DMV | PARTITIONS.
"""
import sys
from pathlib import Path

def _bootstrap_sys_path() -> None:
    """Bootstrap sys.path to include src/python directory"""
    here = Path(__file__).resolve()
    print(f"[BOOTSTRAP] Script path: {here}", file=sys.stderr)
    
    for parent in here.parents:
        config_dir = parent / "config"
        src_python_dir = parent / "src" / "python"
        print(f"[BOOTSTRAP] Checking {parent}: config={config_dir.exists()}, src/python={src_python_dir.exists()}", file=sys.stderr)
        
        if config_dir.exists() and src_python_dir.exists():
            sys.path.insert(0, str(src_python_dir))
            print(f"[BOOTSTRAP] SUCCESS: Added {src_python_dir} to sys.path", file=sys.stderr)
            return
    
    print(f"[BOOTSTRAP] ERROR: Repo root not found (expected 'config' and 'src/python' in parents of {here})", file=sys.stderr)
    raise RuntimeError("Repo root not found (expected 'config' and 'src/python').")

try:
    _bootstrap_sys_path()
    print("[BOOTSTRAP] sys.path bootstrap completed", file=sys.stderr)
except Exception as e:
    print(f"[BOOTSTRAP] FAILED: {e}", file=sys.stderr)
    raise

try:
    from common.config_loader import load_config
    print("[IMPORT] Successfully imported load_config", file=sys.stderr)
except ImportError as e:
    print(f"[IMPORT] FAILED to import load_config: {e}", file=sys.stderr)
    raise


import configparser
from datetime import datetime
import traceback
from typing import List, Optional, Tuple

import pyodbc

SCRIPT_NAME = Path(__file__).name

FORCE = False

PROC_STG = "stg.usp_Run_Daily_stg_Load"
PROC_MART = "mart.usp_Run_Daily_Mart_Load"

DEFAULT_FULINS_TABLE = "ESMA_FULINS_WIDE"
DEFAULT_DLTINS_TABLE = "ESMA_DLTINS_WIDE"

MART_TABLES_TO_COUNT = [
    "mart.DimCFI",
    "mart.DimCurrency",
    "mart.DimIssuer",
    "mart.DimTradingVenue",
    "mart.DimInstrument_SCD2",
    "mart.DimInstrumentListing_SCD2",
    "mart.FactInstrumentSnapshot",
]


def _get_sqlserver_param(cfg: configparser.ConfigParser, key: str, *, required: bool = True, default: str = "") -> str:
    if "SQLSERVER" not in cfg:
        raise ValueError("Invalid ini (missing [SQLSERVER])")
    if required and key not in cfg["SQLSERVER"]:
        raise ValueError(f"Invalid ini (missing [SQLSERVER].{key})")
    return cfg["SQLSERVER"].get(key, default)


def sql_conn(cfg: configparser.ConfigParser, database: str) -> pyodbc.Connection:
    driver = _get_sqlserver_param(cfg, "driver", required=False, default="ODBC Driver 17 for SQL Server")
    server = _get_sqlserver_param(cfg, "server")
    user = _get_sqlserver_param(cfg, "user")
    password = _get_sqlserver_param(cfg, "password")
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};DATABASE={database};"
        f"UID={user};PWD={password};"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str, autocommit=True)


def sql_log_line(conn, message, element: str = "", complement: str = "", file_name: str = "", schema_log: str = "log") -> None:
    sql = f"""
    INSERT INTO {schema_log}.ESMA_Load_Log
        (ScriptName, LaunchTimestamp, StartTime, Message, FileName, Element, Complement)
    VALUES (?, SYSDATETIME(), SYSDATETIME(), ?, ?, ?, ?)
    """
    cur = conn.cursor()
    cur.execute(sql, (SCRIPT_NAME, str(message)[:4000], file_name[:260], element[:200], str(complement)[:4000]))
    cur.close()


def sql_log_long(conn, message: str, element: str, complement: str = "", schema_log: str = "log") -> None:
    if not message:
        return
    msg = str(message)
    chunk_size = 3500
    total = (len(msg) + chunk_size - 1) // chunk_size
    for i in range(total):
        part = msg[i * chunk_size:(i + 1) * chunk_size]
        sql_log_line(conn, part, element=f"{element}_{i+1:02d}/{total:02d}", complement=complement, schema_log=schema_log)


# ----------------------------
# Rowcounts robustes
# ----------------------------
def try_count_big(conn: pyodbc.Connection, full_table_name: str) -> Optional[int]:
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT COUNT_BIG(1) FROM {full_table_name};")
        val = cur.fetchone()[0]
        return int(val or 0)
    finally:
        cur.close()


def try_dmv_partition_stats(conn: pyodbc.Connection, full_table_name: str) -> Optional[int]:
    # nécessite souvent VIEW DATABASE STATE
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT COALESCE(SUM(row_count),0)
            FROM sys.dm_db_partition_stats
            WHERE object_id = OBJECT_ID(?) AND index_id IN (0,1);
            """,
            (full_table_name,),
        )
        val = cur.fetchone()[0]
        return int(val or 0)
    finally:
        cur.close()


def try_sys_partitions(conn: pyodbc.Connection, full_table_name: str) -> Optional[int]:
    # souvent accessible (métadonnées), même sans SELECT sur la table
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT COALESCE(SUM(p.rows),0)
            FROM sys.partitions p
            WHERE p.object_id = OBJECT_ID(?) AND p.index_id IN (0,1);
            """,
            (full_table_name,),
        )
        val = cur.fetchone()[0]
        return int(val or 0)
    finally:
        cur.close()


def count_table_rows_robust(conn: pyodbc.Connection, full_table_name: str) -> Tuple[int, str]:
    """
    Retourne (rowcount, method).
    method in {COUNT, DMV, PARTITIONS, UNKNOWN}
    """
    # 1) direct count
    try:
        rc = try_count_big(conn, full_table_name)
        return rc, "COUNT"
    except Exception:
        pass

    # 2) dmv
    try:
        rc = try_dmv_partition_stats(conn, full_table_name)
        return rc, "DMV"
    except Exception:
        pass

    # 3) sys.partitions (metadata)
    try:
        rc = try_sys_partitions(conn, full_table_name)
        return rc, "PARTITIONS"
    except Exception:
        pass

    return -1, "UNKNOWN"


def count_stg_total(conn: pyodbc.Connection, schema_stg: str, fulins_table: str, dltins_table: str) -> int:
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            SELECT
                (SELECT COUNT_BIG(1) FROM {schema_stg}.{fulins_table}) +
                (SELECT COUNT_BIG(1) FROM {schema_stg}.{dltins_table}) AS total_rows
            """
        )
        val = cur.fetchone()[0]
        return int(val or 0)
    finally:
        cur.close()


def log_counts(conn_log: pyodbc.Connection,
               conn_data: pyodbc.Connection,
               tables: List[str],
               phase: str,
               run_ts: str,
               schema_log: str) -> None:
    for t in tables:
        try:
            rc, method = count_table_rows_robust(conn_data, t)
            sql_log_line(
                conn_log,
                f"ROWCOUNT_{phase}",
                element=t,
                complement=f"rowcount={rc} method={method} run_ts={run_ts}",
                schema_log=schema_log,
            )
        except Exception as e:
            sql_log_line(
                conn_log,
                f"ROWCOUNT_{phase}_WARN - {type(e).__name__}: {e}",
                element=t,
                complement=f"run_ts={run_ts}",
                schema_log=schema_log,
            )


def exec_proc(conn: pyodbc.Connection, conn_log: pyodbc.Connection, proc_fullname: str, schema_log: str = "log", run_ts: str = "") -> bool:
    """
    Execute a stored procedure with comprehensive error handling.
    
    Returns:
        True if successful, False if permission denied or procedure not found
    
    Raises:
        Exception for other SQL errors
    """
    cur = conn.cursor()
    try:
        print(f"[EXEC_PROC] Executing: {proc_fullname}", file=sys.stderr)
        cur.execute(f"EXEC {proc_fullname};")
        print(f"[EXEC_PROC] SUCCESS: {proc_fullname}", file=sys.stderr)
        return True
    except pyodbc.ProgrammingError as e:
        error_str = str(e)
        # Check for permission denied (error 229) or object not found (error 2812)
        if "[42000]" in error_str or "229" in error_str:
            # Permission denied
            msg = f"PERMISSION DENIED: {proc_fullname} - Check EXECUTE permissions"
            print(f"[EXEC_PROC] {msg}: {e}", file=sys.stderr)
            sql_log_line(conn_log, msg, element=proc_fullname, complement=f"Error: {str(e)[:500]}", schema_log=schema_log)
            return False
        elif "[2812]" in error_str or "2812" in error_str:
            # Procedure not found
            msg = f"PROCEDURE NOT FOUND: {proc_fullname}"
            print(f"[EXEC_PROC] {msg}: {e}", file=sys.stderr)
            sql_log_line(conn_log, msg, element=proc_fullname, complement=f"Error: {str(e)[:500]}", schema_log=schema_log)
            return False
        else:
            # Other SQL errors - re-raise
            print(f"[EXEC_PROC] SQL ERROR executing {proc_fullname}: {e}", file=sys.stderr)
            raise
    finally:
        cur.close()


def main() -> int:
    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Load config with better error reporting
    try:
        print(f"[CONFIG] Loading configuration...", file=sys.stderr)
        cfg = load_config()
        print(f"[CONFIG] Configuration loaded successfully", file=sys.stderr)
    except Exception as e:
        print(f"[CONFIG] ERROR: Failed to load configuration: {e}", file=sys.stderr)
        raise

    # Load database parameters with error reporting
    try:
        print(f"[CONFIG] Loading database parameters...", file=sys.stderr)
        db_stg = _get_sqlserver_param(cfg, "database_stg")
        db_dwh = _get_sqlserver_param(cfg, "database_dwh")
        schema_stg = _get_sqlserver_param(cfg, "schema_stg", required=False, default="stg")
        schema_log = _get_sqlserver_param(cfg, "schema_log", required=False, default="log")
        print(f"[CONFIG] Database parameters loaded: db_stg={db_stg}, db_dwh={db_dwh}, schema_stg={schema_stg}, schema_log={schema_log}", file=sys.stderr)
    except Exception as e:
        print(f"[CONFIG] ERROR: Failed to load database parameters: {e}", file=sys.stderr)
        raise

    # Load table names
    try:
        fulins_table = cfg.get("ESMA", "fulins_table", fallback=DEFAULT_FULINS_TABLE)
        dltins_table = cfg.get("ESMA", "dltins_table", fallback=DEFAULT_DLTINS_TABLE)
        print(f"[CONFIG] Table names loaded: fulins={fulins_table}, dltins={dltins_table}", file=sys.stderr)
    except Exception as e:
        print(f"[CONFIG] ERROR: Failed to load table names: {e}", file=sys.stderr)
        raise

    # Connect to STG database
    try:
        print(f"[DB] Connecting to STG database: {db_stg}...", file=sys.stderr)
        conn_stg = sql_conn(cfg, db_stg)
        print(f"[DB] Connected to STG database successfully", file=sys.stderr)
    except Exception as e:
        print(f"[DB] ERROR: Failed to connect to STG database {db_stg}: {e}", file=sys.stderr)
        raise

    try:
        sql_log_line(conn_stg, "BEGIN", element="RUN", complement=f"run_ts={run_ts} db_stg={db_stg} db_dwh={db_dwh} FORCE={FORCE}", schema_log=schema_log)
        sql_log_line(conn_stg, "CONFIG_OK", element="CONFIG_OK", complement=f"schema_stg={schema_stg} schema_log={schema_log} fulins={schema_stg}.{fulins_table} dltins={schema_stg}.{dltins_table}", schema_log=schema_log)

        total_rows = count_stg_total(conn_stg, schema_stg, fulins_table, dltins_table)
        sql_log_line(conn_stg, "CHECK_STG", element="CHECK_STG", complement=f"total_rows={total_rows} run_ts={run_ts}", schema_log=schema_log)

        if total_rows == 0 and not FORCE:
            sql_log_line(conn_stg, "SKIP_PROCS - STG empty", element="SKIP_PROCS", complement=f"run_ts={run_ts}", schema_log=schema_log)
            sql_log_line(conn_stg, "END", element="RUN", complement=f"run_ts={run_ts}", schema_log=schema_log)
            return 0

        # Proc STG
        sql_log_line(conn_stg, "CALL_PROC", element="CALL_PROC", complement=f"{PROC_STG} @ {db_stg}", schema_log=schema_log)
        proc_stg_success = exec_proc(conn_stg, conn_stg, PROC_STG, schema_log, run_ts)
        if proc_stg_success:
            sql_log_line(conn_stg, "PROC_OK", element="PROC_OK", complement=f"{PROC_STG} @ {db_stg}", schema_log=schema_log)
        else:
            sql_log_line(conn_stg, "PROC_SKIPPED", element="PROC_SKIPPED", complement=f"{PROC_STG} @ {db_stg} (no permissions)", schema_log=schema_log)

        # Proc MART
        conn_dwh = None
        try:
            conn_dwh = sql_conn(cfg, db_dwh)
            sql_log_line(conn_stg, "CONN_DWH_OK", element="CONN_DWH_OK", complement=f"db_dwh={db_dwh}", schema_log=schema_log)

            sql_log_line(conn_stg, "ROWCOUNT_BEFORE_MART", element="ROWCOUNT_BEFORE_MART", complement=f"tables={len(MART_TABLES_TO_COUNT)} run_ts={run_ts}", schema_log=schema_log)
            log_counts(conn_stg, conn_dwh, MART_TABLES_TO_COUNT, "BEFORE", run_ts, schema_log)

            sql_log_line(conn_stg, "CALL_PROC", element="CALL_PROC", complement=f"{PROC_MART} @ {db_dwh}", schema_log=schema_log)
            proc_mart_success = exec_proc(conn_dwh, conn_stg, PROC_MART, schema_log, run_ts)
            if proc_mart_success:
                sql_log_line(conn_stg, "PROC_OK", element="PROC_OK", complement=f"{PROC_MART} @ {db_dwh}", schema_log=schema_log)
            else:
                sql_log_line(conn_stg, "PROC_SKIPPED", element="PROC_SKIPPED", complement=f"{PROC_MART} @ {db_dwh} (no permissions)", schema_log=schema_log)

            sql_log_line(conn_stg, "ROWCOUNT_AFTER_MART", element="ROWCOUNT_AFTER_MART", complement=f"tables={len(MART_TABLES_TO_COUNT)} run_ts={run_ts}", schema_log=schema_log)
            log_counts(conn_stg, conn_dwh, MART_TABLES_TO_COUNT, "AFTER", run_ts, schema_log)

        finally:
            try:
                if conn_dwh is not None:
                    conn_dwh.close()
                    sql_log_line(conn_stg, "CONN_DWH_CLOSE", element="CONN_DWH_CLOSE", complement=f"db_dwh={db_dwh}", schema_log=schema_log)
            except Exception:
                pass

        sql_log_line(conn_stg, "END", element="RUN", complement=f"run_ts={run_ts}", schema_log=schema_log)
        return 0

    except Exception:
        sql_log_long(conn_stg, traceback.format_exc(), element="TRACEBACK", complement=f"run_ts={run_ts}", schema_log=schema_log)
        raise
    finally:
        try:
            conn_stg.close()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
