# 01-LOAD_LEI_FILE_v3.py
# Autonomous ETL helper for GLEIF Golden Copy: scrape latest .csv.zip links, download, extract CSV, log to SQL Server.
#
# NOTE: This v3 release only refactors configuration loading to use common.config_loader (ESMA standard).
# Business logic (scrape/download/extract/log) remains unchanged.

import os
import json
import re
import traceback
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urlparse
import sys

import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait


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


# -------------------------
# Constants
# -------------------------
run_ts = datetime.now()

DATASETS = {
    "lei_cdf": "gleif-goldencopy-lei2-golden-copy.csv.zip",
    "rr_cdf": "gleif-goldencopy-rr-golden-copy.csv.zip",
    "reporting_exceptions": "gleif-goldencopy-repex-golden-copy.csv.zip",
}

# Parse timestamp from GLEIF file URL:
# .../golden-copy-files/YYYY/MM/DD/<build>/<YYYYMMDD>-<HHMM>-....
DT_RE = re.compile(
    r"/golden-copy-files/(\d{4})/(\d{2})/(\d{2})/.*?/(\d{8})-(\d{4})-",
    re.IGNORECASE,
)


# -------------------------
# Helpers (business unchanged)
# -------------------------

def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def normalize(url: str) -> str:
    # remove trailing fragment marker like "#"
    return url.rstrip("#").strip()


def parse_dt(url: str) -> datetime:
    m = DT_RE.search(url)
    if not m:
        return datetime(1900, 1, 1)
    ymd = m.group(4)
    hm = m.group(5)
    return datetime.strptime(ymd + hm, "%Y%m%d%H%M")


def setup_driver(headless: bool = True) -> webdriver.Chrome:
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1400,900")
    return webdriver.Chrome(options=opts)


def filename_from_url(url: str) -> str:
    p = urlparse(url)
    name = Path(p.path).name
    return name if name else f"download_{int(datetime.utcnow().timestamp())}.zip"


def download_file(url: str, dest: Path, timeout: int = 180) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, stream=True, timeout=timeout) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
    return dest


def extract_csv_from_zip(zip_path: Path, out_dir: Path) -> List[Path]:
    import zipfile

    out_dir.mkdir(parents=True, exist_ok=True)
    extracted: List[Path] = []

    with zipfile.ZipFile(zip_path, "r") as z:
        members = z.namelist()
        csv_members = [m for m in members if m.lower().endswith(".csv")]

        for member in csv_members:
            out_path = out_dir / Path(member).name
            with z.open(member) as src, open(out_path, "wb") as dst:
                dst.write(src.read())
            extracted.append(out_path)

    return extracted


# -------------------------
# Config (refactor-only)
# -------------------------

def load_runtime_config():
    """
    ESMA standard: use common.config_loader.load_config() which loads:
      - config/config.ini if present, else
      - config/config.template.ini

    Required sections/keys:
      [GLEIF_URL] url
      [GLEIF_DOWNLOAD] directory
      [GLEIF_CSV] directory
      [SQLSERVER] server, database_stg, user, password (+ optional driver, schema_log)
    """
    cfg = load_config()

    # Required: GLEIF locations
    try:
        page_url = cfg.get("GLEIF_URL", "url").strip()
        download_dir = Path(cfg.get("GLEIF_DOWNLOAD", "directory")).expanduser()
        csv_dir = Path(cfg.get("GLEIF_CSV", "directory")).expanduser()
    except Exception as e:
        raise KeyError(
            "Missing GLEIF config keys. Expected:\n"
            "  [GLEIF_URL] url\n"
            "  [GLEIF_DOWNLOAD] directory\n"
            "  [GLEIF_CSV] directory\n"
            f"Underlying error: {e}"
        )

    # Required: SQL logging (same as ESMA)
    try:
        sql = {
            "server": cfg.get("SQLSERVER", "server"),
            "database": cfg.get("SQLSERVER", "database_stg"),
            "schema_log": cfg.get("SQLSERVER", "schema_log", fallback="log"),
            "user": cfg.get("SQLSERVER", "user"),
            "password": cfg.get("SQLSERVER", "password"),
            "driver": cfg.get("SQLSERVER", "driver", fallback="ODBC Driver 17 for SQL Server"),
        }
    except Exception as e:
        raise KeyError(
            "Missing SQLSERVER config keys. Expected:\n"
            "  [SQLSERVER] server\n"
            "  [SQLSERVER] database_stg\n"
            "  [SQLSERVER] user\n"
            "  [SQLSERVER] password\n"
            "  optional: driver, schema_log\n"
            f"Underlying error: {e}"
        )

    # Best-effort: show which config file was used (if loader exposes it)
    ini_path = Path(cfg.get("CONFIG", "path", fallback="config/config.ini")).expanduser()

    return page_url, download_dir, csv_dir, sql, ini_path


# -------------------------
# SQL Logging to log.ESMA_Load_Log (business unchanged)
# -------------------------

def sql_connect(sql: Dict[str, str]):
    """
    Returns (conn, error_message).
    If pyodbc isn't installed, conn is None and error_message explains why.
    """
    try:
        import pyodbc  # type: ignore
    except ImportError:
        return None, "pyodbc not installed (py -m pip install pyodbc). SQL logging disabled."

    conn_str = (
        f"DRIVER={{{sql['driver']}}};"
        f"SERVER={sql['server']};"
        f"DATABASE={sql['database']};"
        f"UID={sql['user']};"
        f"PWD={sql['password']};"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str, autocommit=True), None


def get_table_columns(conn, schema: str, table: str) -> List[str]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
        """,
        (schema, table),
    )
    cols = [r[0] for r in cur.fetchall()]
    cur.close()
    return cols


def ensure_fallback_log_table(conn, schema: str, table: str):
    """
    If log.ESMA_Load_Log doesn't exist, create a minimal fallback (so the script still runs).
    """
    cur = conn.cursor()
    cur.execute(
        """
        IF NOT EXISTS (
            SELECT 1
            FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE t.name = ? AND s.name = ?
        )
        BEGIN
            EXEC('
            CREATE TABLE [' + ? + '].[' + ? + '](
                id INT IDENTITY(1,1) PRIMARY KEY,
                run_ts DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
                script NVARCHAR(200) NULL,
                step NVARCHAR(100) NOT NULL,
                status NVARCHAR(20) NOT NULL,
                message NVARCHAR(MAX) NULL
            )')
        END
        """,
        (table, schema, schema, table),
    )
    cur.close()


def log_esma(conn, schema, step, status, message=None, extra=None, run_ts=None):
    """
    Insert into [schema].[ESMA_Load_Log] ensuring NOT NULL columns:
    - ScriptName
    - LaunchTimestamp
    """
    table = "ESMA_Load_Log"
    ensure_fallback_log_table(conn, schema, table)

    cols = get_table_columns(conn, schema, table)
    cols_l = {c.lower(): c for c in cols}

    script_name = Path(__file__).name

    if run_ts is None:
        run_ts = datetime.now()

    candidates = {
        "ScriptName": script_name,
        "LaunchTimestamp": run_ts,
        "Step": step,
        "Status": status,
        "Message": message,
        "Details": message,
    }

    if extra:
        for k, v in extra.items():
            candidates[k] = v

    payload = {}
    for k, v in candidates.items():
        if v is None:
            continue
        key = k.lower()
        if key in cols_l:
            payload[cols_l[key]] = v

    if not payload:
        return

    col_list = ", ".join(f"[{c}]" for c in payload.keys())
    placeholders = ", ".join("?" for _ in payload)
    values = list(payload.values())

    cur = conn.cursor()
    cur.execute(
        f"INSERT INTO [{schema}].[{table}] ({col_list}) VALUES ({placeholders})",
        values,
    )
    cur.close()


# -------------------------
# Scrape latest links (business unchanged)
# -------------------------

def get_latest_links(driver: webdriver.Chrome, page_url: str) -> Dict[str, Optional[str]]:
    driver.get(page_url)

    # Wait until at least one link containing ".csv.zip" appears in the DOM
    WebDriverWait(driver, 90).until(
        lambda d: d.execute_script(
            "return Array.from(document.querySelectorAll('a[href]'))"
            ".some(a => a.href && a.href.includes('.csv.zip'));"
        )
    )

    urls = driver.execute_script(
        "return Array.from(document.querySelectorAll('a[href]'))"
        ".map(a => a.href)"
        ".filter(h => h && h.includes('.csv.zip'));"
    )

    urls = sorted(set(normalize(u) for u in urls))

    latest: Dict[str, Optional[str]] = {}
    for key, needle in DATASETS.items():
        candidates = [u for u in urls if needle in u]
        candidates.sort(key=parse_dt, reverse=True)
        latest[key] = candidates[0] if candidates else None

    return latest


# -------------------------
# Main (business unchanged)
# -------------------------

def main():
    page_url, download_dir, csv_dir, sql, ini_path = load_runtime_config()

    # SQL logging
    conn, err = sql_connect(sql)
    if err:
        print(f"[WARN] {err}")
        conn = None

    run_ts_local = datetime.now()

    def log(step, status, message=None, extra=None):
        print(f"{now_str()} [{status}] {step} - {message or ''}".rstrip())
        if conn:
            try:
                log_esma(conn, sql["schema_log"], step, status, message, extra=extra, run_ts=run_ts_local)
            except Exception as e:
                print(f"{now_str()} [WARN] SQL log failed: {e}")

    driver = setup_driver(headless=True)

    try:
        log("CONFIG", "OK", f"Using config: {ini_path}")
        log("SCRAPE", "START", f"Open: {page_url}")

        latest = get_latest_links(driver, page_url)
        log("SCRAPE", "OK", json.dumps(latest, ensure_ascii=False))

        missing = [k for k, v in latest.items() if not v]
        if missing:
            log("SCRAPE", "WARN", f"Missing datasets: {missing}")

        # Download + extract
        run_dir = download_dir / datetime.now().strftime("%Y-%m-%d")
        for ds_key, url in latest.items():
            if not url:
                continue

            url_norm = normalize(url)
            zip_name = filename_from_url(url_norm)
            zip_path = run_dir / zip_name

            log("DOWNLOAD", "START", f"{ds_key}: {url_norm}")
            download_file(url_norm, zip_path)
            log("DOWNLOAD", "OK", str(zip_path), extra={"url": url_norm, "file": str(zip_path)})

            log("EXTRACT", "START", f"{ds_key}: {zip_path.name}")
            extracted = extract_csv_from_zip(zip_path, csv_dir)
            if not extracted:
                log("EXTRACT", "WARN", f"No CSV found in {zip_path.name}")
            else:
                for p in extracted:
                    log("EXTRACT", "OK", str(p), extra={"file": str(p)})

        log("RUN", "OK", "Completed successfully")

    except Exception as e:
        msg = f"{e}\n{traceback.format_exc()}"
        log("RUN", "ERROR", msg)
        raise

    finally:
        driver.quit()
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
