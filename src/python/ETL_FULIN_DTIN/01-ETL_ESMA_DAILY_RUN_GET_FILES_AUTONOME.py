#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
01-ETL_ESMA_DAILY_RUN_GET_FILES_AUTONOME.py
==========================================

Script AUTONOME : purge + détection des dates à récupérer + download + unzip.

Ce script remplace la logique "pilotée par paramètres" par une logique simple :
1) Purge des fichiers de données (data/*) au démarrage (sauf data/archive).
2) Lecture des dernières dates déjà chargées (FULL et DELTA) dans STG via SQL.
3) Recherche des dernières dates disponibles (FULL et DELTA) sur ESMA (SOLR FIRDS).
4) Téléchargement + extraction :
   - FULL : uniquement si un FULL plus récent existe.
   - DELTA : du jour suivant la référence (nouveau FULL sinon last_delta sinon last_full) jusqu'au dernier DELTA disponible.

Aucune création BSV/CSV, aucun chargement SQL des XML : ce script s'arrête après extraction.
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
    from common.config_loader import load_config, resolve_project_root
    print("[IMPORT] Successfully imported load_config and resolve_project_root", file=sys.stderr)
except ImportError as e:
    print(f"[IMPORT] FAILED to import from common.config_loader: {e}", file=sys.stderr)
    raise

import argparse
import configparser
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple
import re
import shutil
import zipfile

import pyodbc
import requests
SCRIPT_NAME = "01-ETL_ESMA_DAILY_RUN_GET_FILES_AUTONOME.py"
CONFIG_DIR_DEFAULT = None
ESMA_SOLR = "https://registers.esma.europa.eu/solr/esma_registers_firds_files/select"
HTTP_TIMEOUT = 120

# --- SQL last loaded (fourni par le user) ---
SQL_LAST_LOADED_FULL = """
select MAX(TRY_CONVERT(date, substring(SourceFileName,10,8)))
FROM stg.ESMA_FULINS_WIDE
WHERE SourceFileName LIKE 'FULINS_%'
"""

SQL_LAST_LOADED_DELTA = """
select MAX(TRY_CONVERT(date, substring(SourceFileName,8,8)))
FROM stg.ESMA_FULINS_WIDE
WHERE SourceFileName LIKE 'DLTINS_%'
"""


# ----------------------------
# Root + Config
# ----------------------------



def sql_conn(cfg: configparser.ConfigParser) -> pyodbc.Connection:
    driver = cfg["SQLSERVER"].get("driver", "ODBC Driver 17 for SQL Server")
    server = cfg["SQLSERVER"]["server"]
    database = cfg["SQLSERVER"].get("database_stg") or cfg["SQLSERVER"].get("database")
    if not database:
        raise KeyError("Il manque SQLSERVER.database_stg (ou database) dans bbs_server.local.ini")
    user = cfg["SQLSERVER"]["user"]
    password = cfg["SQLSERVER"]["password"]
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};DATABASE={database};"
        f"UID={user};PWD={password};"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str, autocommit=True)


def sql_log_line(conn: pyodbc.Connection, message: str, element: str = "", complement: str = "", file_name: str = "") -> None:
    sql = """
    INSERT INTO log.ESMA_Load_Log
        (ScriptName, LaunchTimestamp, StartTime, Message, FileName, Element, Complement)
    VALUES
        (?, SYSDATETIME(), SYSDATETIME(), ?, ?, ?, ?);
    """
    cur = conn.cursor()
    cur.execute(sql, (SCRIPT_NAME, message[:4000], file_name[:260], element[:200], complement[:4000]))
    cur.close()


def sql_scalar_date(conn: pyodbc.Connection, sql: str) -> Optional[date]:
    cur = conn.cursor()
    cur.execute(sql)
    row = cur.fetchone()
    cur.close()
    if not row:
        return None
    v = row[0]
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.date()
    return v


# ----------------------------
# Purge data
# ----------------------------

def purge_data_dir(data_dir: Path, conn: Optional[pyodbc.Connection]) -> None:
    """
    Purge toutes les données sous data/* SAUF :
    - data/archive
    - data/csv/BOURSORAMA
    """
    data_dir.mkdir(parents=True, exist_ok=True)

    archive_dir = (data_dir / "archive").resolve()
    boursorama_dir = (data_dir / "csv" / "BOURSORAMA").resolve()

    for child in data_dir.iterdir():
        try:
            child_resolved = child.resolve()

            # Protection explicite
            if child_resolved == archive_dir:
                continue

            if child_resolved == boursorama_dir or boursorama_dir in child_resolved.parents:
                continue

            if child.is_dir():
                shutil.rmtree(child, ignore_errors=True)
            else:
                child.unlink(missing_ok=True)

        except Exception as e:
            if conn:
                sql_log_line(
                    conn,
                    f"PURGE_WARN - {type(e).__name__}: {e}",
                    element="PURGE_WARN",
                    complement=str(child)
                )

    (data_dir / "downloaded").mkdir(parents=True, exist_ok=True)
    (data_dir / "extracted").mkdir(parents=True, exist_ok=True)


# ----------------------------
# ESMA SOLR helpers
# ----------------------------
def solr_search_by_date_range(start_ymd: str, end_ymd: str, rows: int = 500, max_rows: int = 5000) -> List[Dict]:
    """
    Liste des fichiers publiés entre start_ymd et end_ymd (inclus) via l'API SOLR ESMA,
    conformément à la doc ESMA : q=* et fq=publication_date:[... TO ...].
    """
    docs: List[Dict] = []
    start = 0
    fq = f"publication_date:[{start_ymd}T00:00:00Z TO {end_ymd}T23:59:59Z]"
    while True:
        params = {"q": "*", "fq": fq, "wt": "json", "rows": rows, "start": start, "sort": "publication_date desc"}
        r = requests.get(ESMA_SOLR, params=params, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        resp = data.get("response", {})
        batch = resp.get("docs", []) or []
        docs.extend(batch)

        num_found = int(resp.get("numFound", 0) or 0)
        if not batch:
            break
        start += rows
        if start >= num_found:
            break
        if len(docs) >= max_rows:
            break
    return docs


def doc_download_url(doc: Dict) -> Optional[str]:
    for k in ("download_link", "downloadLink", "file_url", "url", "download_url", "downloadUrl"):
        if doc.get(k):
            return doc[k]
    return None


def doc_filename(doc: Dict, fallback: str) -> str:
    return doc.get("file_name") or doc.get("fileName") or doc.get("name") or fallback


def download_file(url: str, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if out_path.exists() and out_path.stat().st_size > 0:
        return
    with requests.get(url, stream=True, timeout=HTTP_TIMEOUT) as r:
        r.raise_for_status()
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)


def extract_zip_xml_only(zip_path: Path, out_dir: Path) -> List[Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    extracted: List[Path] = []
    with zipfile.ZipFile(zip_path, "r") as z:
        for name in z.namelist():
            if name.lower().endswith(".xml"):
                target = out_dir / Path(name).name
                if target.exists() and target.stat().st_size > 0:
                    extracted.append(target)
                    continue
                with z.open(name) as src, open(target, "wb") as dst:
                    dst.write(src.read())
                extracted.append(target)
    return extracted


def yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")


def daterange(d1: date, d2: date):
    cur = d1
    while cur <= d2:
        yield cur
        cur += timedelta(days=1)


def fetch_fulins_for_date(pool_date: date, data_dir: Path, conn: pyodbc.Connection, run_ts: str) -> Tuple[int, int]:
    d = yyyymmdd(pool_date)
    sql_log_line(conn, f"ESMA SOLR FULL list for publication_date={pool_date.isoformat()}", element="FULL_QUERY", complement=f"run_ts={run_ts}")
    docs_all = solr_search_by_date_range(pool_date.isoformat(), pool_date.isoformat(), rows=1000, max_rows=50000)
    # garder uniquement les ZIP FULINS dont le nom contient la date de pool
    docs = [dct for dct in docs_all if (doc_filename(dct, '').upper().startswith('FULINS_') and f'_{d}_' in doc_filename(dct, '').upper())]

    download_dir = data_dir / "downloaded" / "FULINS" / d
    extract_dir = data_dir / "extracted" / "FULINS" / d

    zip_count = 0
    xml_count = 0

    for doc in docs:
        url = doc_download_url(doc)
        if not url:
            continue
        fname = doc_filename(doc, f"FULINS_{d}.zip")
        out_zip = download_dir / fname
        download_file(url, out_zip)
        zip_count += 1
        xmls = extract_zip_xml_only(out_zip, extract_dir)
        xml_count += len(xmls)

    return zip_count, xml_count


def fetch_dltins_for_date(dt: date, data_dir: Path, conn: pyodbc.Connection, run_ts: str) -> Tuple[int, int]:
    d = yyyymmdd(dt)
    sql_log_line(conn, f"ESMA SOLR DELTA list for publication_date={dt.isoformat()}", element="DELTA_QUERY", complement=f"run_ts={run_ts}")
    docs_all = solr_search_by_date_range(dt.isoformat(), dt.isoformat(), rows=2000, max_rows=50000)
    docs = [dct for dct in docs_all if (doc_filename(dct, '').upper().startswith('DLTINS_') and doc_filename(dct, '').upper().startswith(f'DLTINS_{d}_'))]

    download_dir = data_dir / "downloaded" / "DLTINS"
    extract_dir = data_dir / "extracted" / "DLTINS" / d

    zip_count = 0
    xml_count = 0

    for doc in docs:
        url = doc_download_url(doc)
        if not url:
            continue
        fname = doc_filename(doc, f"DLTINS_{d}.zip")
        out_zip = download_dir / fname
        download_file(url, out_zip)
        zip_count += 1
        xmls = extract_zip_xml_only(out_zip, extract_dir)
        xml_count += len(xmls)

    return zip_count, xml_count


# ----------------------------
# Latest available dates
# ----------------------------
FULINS_DATE_RE = re.compile(r"(?:^|/|\\)(FULINS_.+?_(\d{8})_.*)$", re.IGNORECASE)
DLTINS_DATE_RE = re.compile(r"(?:^|/|\\)(DLTINS_(\d{8})_.*)$", re.IGNORECASE)


def _extract_date_from_name(name: str, kind: str) -> Optional[date]:
    if not name:
        return None
    m = (FULINS_DATE_RE.search(name) if kind == "FULL" else DLTINS_DATE_RE.search(name))
    if not m:
        return None
    ds = m.group(2)
    try:
        return datetime.strptime(ds, "%Y%m%d").date()
    except Exception:
        return None


def get_latest_available_full_date(conn: pyodbc.Connection, run_ts: str) -> Optional[date]:
    """
    Détecte la dernière date 'pool' FULINS disponible en scannant les publications des N derniers jours.
    On utilise la mécanique ESMA recommandée : q=* + fq=publication_date:[... TO ...].
    """
    lookback_days = 45
    end = datetime.utcnow().date()
    start = end - timedelta(days=lookback_days)

    sql_log_line(conn, f"ESMA SOLR scan FULL last {lookback_days} days", element="FULL_LATEST_SCAN", complement=f"{start.isoformat()}..{end.isoformat()} run_ts={run_ts}")
    docs = solr_search_by_date_range(start.isoformat(), end.isoformat(), rows=1000, max_rows=200000)

    best: Optional[date] = None
    for doc in docs:
        name = doc_filename(doc, "")
        if not name:
            continue
        if not name.upper().startswith("FULINS_"):
            continue
        d = _extract_date_from_name(name, "FULL")
        if d and (best is None or d > best):
            best = d
    return best


def get_latest_available_delta_date(conn: pyodbc.Connection, run_ts: str) -> Optional[date]:
    """
    Détecte la dernière date DLTINS disponible en scannant les publications des N derniers jours.
    """
    lookback_days = 45
    end = datetime.utcnow().date()
    start = end - timedelta(days=lookback_days)

    sql_log_line(conn, f"ESMA SOLR scan DELTA last {lookback_days} days", element="DELTA_LATEST_SCAN", complement=f"{start.isoformat()}..{end.isoformat()} run_ts={run_ts}")
    docs = solr_search_by_date_range(start.isoformat(), end.isoformat(), rows=2000, max_rows=200000)

    best: Optional[date] = None
    for doc in docs:
        name = doc_filename(doc, "")
        if not name:
            continue
        if not name.upper().startswith("DLTINS_"):
            continue
        d = _extract_date_from_name(name, "DELTA")
        if d and (best is None or d > best):
            best = d
    return best


# ----------------------------
# Main
# ----------------------------
def main() -> int:
    # Aucun argument CLI : tout est calculé automatiquement
    run_ts = datetime.now().strftime('%Y%m%d_%H%M%S')

    script_dir = Path(__file__).resolve().parent
    root_dir = resolve_project_root()
    config_dir = root_dir / "config"


    cfg = load_config()
    ##print("[DEBUG] SQLSERVER keys:", list(cfg["SQLSERVER"].keys()))

    conn = sql_conn(cfg)

    data_dir = root_dir / "data"

    try:
        sql_log_line(conn, "BEGIN", element="GET_FILES", complement=f"run_ts={run_ts}")

        # STEP0 purge
        sql_log_line(conn, "STEP0 - Purge data directory (except data/archive)", element="STEP0", complement=f"data_dir={data_dir} run_ts={run_ts}")
        purge_data_dir(data_dir, conn)
        sql_log_line(conn, "STEP0_RESULT - Purge done", element="STEP0_RESULT", complement=f"run_ts={run_ts}")

        # STEP1 last loaded
        last_full = sql_scalar_date(conn, SQL_LAST_LOADED_FULL)
        last_delta = sql_scalar_date(conn, SQL_LAST_LOADED_DELTA)
        sql_log_line(conn, f"LAST_LOADED - FULL={last_full} DELTA={last_delta}", element="LAST_LOADED", complement=f"run_ts={run_ts}")

        # STEP2 latest available
        latest_full = get_latest_available_full_date(conn, run_ts)
        latest_delta = get_latest_available_delta_date(conn, run_ts)
        sql_log_line(conn, f"LATEST_AVAILABLE - FULL={latest_full} DELTA={latest_delta}", element="LATEST_AVAILABLE", complement=f"run_ts={run_ts}")

        # PLAN
        full_to_get: Optional[date] = None
        if latest_full is not None and (last_full is None or last_full < latest_full):
            full_to_get = latest_full

        delta_from: Optional[date] = None
        delta_to: Optional[date] = latest_delta

        if delta_to is not None:
            if full_to_get is not None:
                delta_from = full_to_get + timedelta(days=1)
            elif last_delta is not None:
                delta_from = last_delta + timedelta(days=1)
            elif last_full is not None:
                delta_from = last_full + timedelta(days=1)
            else:
                # fallback: on ne sait pas d'où partir -> on ne récupère que le dernier jour
                delta_from = delta_to

                sql_log_line(conn, f"PLAN - full_to_get={full_to_get} latest_delta={latest_delta} (rule: only latest delta day)", element="PLAN", complement=f"run_ts={run_ts}")

        # STEP3 FULL
        if full_to_get is not None:
            sql_log_line(conn, "STEP3 - Download/Extract FULL files", element="STEP3", complement=f"pool={full_to_get} run_ts={run_ts}")
            zc, xc = fetch_fulins_for_date(full_to_get, data_dir, conn, run_ts)
            sql_log_line(conn, f"STEP3_RESULT - FULL zips={zc} xmls={xc}", element="STEP3_RESULT", complement=f"pool={full_to_get} run_ts={run_ts}")
        else:
            sql_log_line(conn, "STEP3 - Skip FULL (already up-to-date)", element="STEP3_SKIP", complement=f"run_ts={run_ts}")

        # STEP4 DELTA (NOUVELLE REGLE) : on ne récupère QUE la date DELTA maximale disponible
        # Exemple : last_delta=2026-01-11, ESMA a 2026-01-11/12/13 -> on ne prend que 2026-01-13
        if latest_delta is None:
            sql_log_line(conn, "STEP4 - Skip DELTA (no latest_delta)", element="STEP4_SKIP", complement=f"run_ts={run_ts}")
        else:
            # référence de comparaison : last_delta prioritaire, sinon last_full (si jamais aucun delta chargé)
            ref = last_delta or last_full
            if ref is not None and latest_delta <= ref:
                sql_log_line(conn, "STEP4 - Skip DELTA (already up-to-date)", element="STEP4_SKIP", complement=f"latest={latest_delta} ref={ref} run_ts={run_ts}")
            else:
                sql_log_line(conn, "STEP4 - Download/Extract ONLY latest DELTA date", element="STEP4", complement=f"date={latest_delta} ref={ref} run_ts={run_ts}")
                zc, xc = fetch_dltins_for_date(latest_delta, data_dir, conn, run_ts)
                sql_log_line(conn, f"STEP4_RESULT - DELTA zips={zc} xmls={xc}", element="STEP4_RESULT", complement=f"date={latest_delta} run_ts={run_ts}")

        sql_log_line(conn, "END", element="END", complement=f"run_ts={run_ts}")
        return 0

    except Exception as e:
        try:
            sql_log_line(conn, f"ERROR - {type(e).__name__}: {e}", element="ERROR", complement=f"run_ts={run_ts}")
        except Exception:
            pass
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
