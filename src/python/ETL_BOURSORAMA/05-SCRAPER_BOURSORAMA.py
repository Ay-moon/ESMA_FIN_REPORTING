#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
05-SCRAPER_BOURSORAMA_v2.py
==========================

Same business logic as original script.
Only refactor: configuration loading via common.config_loader.load_config()
(no hardcoded D:\ paths, no scraper.local.ini fixed path).
"""

from __future__ import annotations

import configparser
import csv
import os
import re
import time
import unicodedata
import traceback
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

import pyodbc
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

import sys


# ============================================================
# BOOTSTRAP (so "common" imports work everywhere)
# ============================================================

REPO_ROOT: Path


def _bootstrap_sys_path() -> None:
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "config").exists() and (parent / "src" / "python").exists():
            global REPO_ROOT
            REPO_ROOT = parent
            sys.path.insert(0, str(parent / "src" / "python"))
            return
    raise RuntimeError("Repo root not found (expected 'config' and 'src/python').")

_bootstrap_sys_path()

from common.config_loader import load_config


# ----------------------------
# Paths / Config (refactor-only)
# ----------------------------
SCRIPT_NAME = Path(__file__).name


def read_config() -> configparser.ConfigParser:
    """
    Load project configuration.

    Priority order:
      1) env var KHL_CONFIG_INI (explicit absolute/relative path)
      2) <repo_root>/config/config.ini
      3) <repo_root>/config.ini
      4) <script_dir>/config.ini

    Fallback: common.config_loader.load_config() (repo default).
    """
    env_path = os.environ.get("KHL_CONFIG_INI")
    candidates: List[Path] = []
    if env_path:
        candidates.append(Path(env_path))
    candidates.extend(
        [
            REPO_ROOT / "config" / "config.ini",
            REPO_ROOT / "config.ini",
            Path(__file__).resolve().parent / "config.ini",
        ]
    )

    for p in candidates:
        try:
            if p and p.exists() and p.is_file():
                cfg = configparser.ConfigParser()
                cfg.read(p, encoding="utf-8")
                return cfg
        except Exception:
            pass

    # Repo-standard fallback
    return load_config()


def get_int(cfg: configparser.ConfigParser, section: str, key: str, default: int) -> int:
    try:
        return int(cfg.get(section, key))
    except Exception:
        return default


def get_str(cfg: configparser.ConfigParser, section: str, key: str, default: str = "") -> str:
    try:
        return cfg.get(section, key)
    except Exception:
        return default


def get_path(cfg: configparser.ConfigParser, section: str, key: str) -> Path:
    try:
        return Path(cfg.get(section, key)).expanduser().resolve()
    except Exception:
        raise KeyError(f"Missing config key: [{section}] {key}")


# ----------------------------
# SQL Logging
# ----------------------------
def sql_conn(cfg: configparser.ConfigParser) -> pyodbc.Connection:
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


def sql_log_line(conn, message: str, element: str = "", complement: str = "", file_name: str = "") -> None:
    sql = """
    INSERT INTO log.ESMA_Load_Log
        (ScriptName, LaunchTimestamp, StartTime, Message, FileName, Element, Complement)
    VALUES (?, SYSDATETIME(), SYSDATETIME(), ?, ?, ?, ?)
    """
    cur = conn.cursor()
    cur.execute(
        sql,
        (
            SCRIPT_NAME,
            str(message)[:4000],
            file_name[:260],
            element[:200],
            str(complement)[:4000],
        ),
    )
    cur.close()


# ----------------------------
# Utilities (strict text)
# ----------------------------
ISIN_RE = re.compile(r"\b[A-Z]{2}[A-Z0-9]{10}\b")


def normalize_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def clean_cell_text(s: str) -> str:
    if s is None:
        return ""
    s = str(s).replace("\u00a0", " ").replace("\u202f", " ")
    return normalize_ws(s)


def extract_isin(text: str) -> str:
    m = ISIN_RE.search(text or "")
    return m.group(0) if m else ""


def _norm_colname(s: str) -> str:
    s = (s or "").strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s


def parse_headers(structure: str) -> List[str]:
    parts = [p.strip() for p in (structure or "").split("|") if p.strip()]
    out: List[str] = []
    for p in parts:
        pu = p.upper()
        if pu in ("DATE_EXTRATION", "DATE_EXTRACTION"):
            out.append("DATE_EXTRACTION")
        elif pu in ("PRODUITTYPE", "PRODUIT_TYPE"):
            out.append("ProduitType")
        else:
            out.append(p)

    if "ISIN" not in [x.upper() for x in out]:
        out.insert(0, "ISIN")
    if "DATE_EXTRACTION" not in out:
        out.append("DATE_EXTRACTION")
    if "ProduitType" not in out:
        out.append("ProduitType")
    if "SOURCE_URL" not in out:
        out.append("SOURCE_URL")
    return out


# ----------------------------
# HTTP
# ----------------------------
def build_session(user_agent: str) -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": user_agent or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }
    )
    return s


def fetch_with_retry(session: requests.Session, url: str, max_retries: int, retry_sleep: int, timeout: int) -> str:
    last = None
    for attempt in range(1, max_retries + 1):
        try:
            bust = url + ("&" if "?" in url else "?") + f"_={int(time.time()*1000)}_{attempt}"
            r = session.get(bust, timeout=timeout)
            r.raise_for_status()
            return r.text
        except Exception as e:
            last = e
            if attempt < max_retries:
                time.sleep(retry_sleep)
    raise RuntimeError(f"Fetch failed ({max_retries} tries): {url} ({last})")


# ----------------------------
# HTML parsing (BeautifulSoup ONLY, strict text)
# ----------------------------
def _score_headers(headers: List[str], expected_headers: List[str]) -> int:
    expected_norm = {_norm_colname(h) for h in expected_headers if h and h.upper() not in ("ISIN", "DATE_EXTRACTION", "PRODUITTYPE", "SOURCE_URL")}
    headers_norm = {_norm_colname(h) for h in headers if h}
    return len(expected_norm.intersection(headers_norm))


def html_to_rows_best_table(html: str, expected_headers: List[str]) -> Tuple[List[Dict[str, str]], List[str]]:
    if not html or len(html) < 200:
        return [], []

    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")
    if not tables:
        return [], []

    best_table = None
    best_headers: List[str] = []
    best_score = -1

    for t in tables:
        header_cells = []

        thead = t.find("thead")
        if thead:
            tr = thead.find("tr")
            if tr:
                header_cells = tr.find_all(["th", "td"])

        if not header_cells:
            tr = t.find("tr")
            if tr:
                header_cells = tr.find_all(["th", "td"])

        headers = [clean_cell_text(h.get_text(" ", strip=True)) for h in header_cells]
        score = _score_headers(headers, expected_headers)

        if score > best_score:
            best_score = score
            best_table = t
            best_headers = headers

    if best_table is None or best_score <= 0:
        return [], []

    rows: List[Dict[str, str]] = []
    trs = best_table.find_all("tr")

    start_idx = 1 if len(trs) > 0 else 0

    for tr in trs[start_idx:]:
        tds = tr.find_all("td")
        if not tds:
            continue

        row_dict: Dict[str, str] = {}
        for i, td in enumerate(tds):
            col = best_headers[i] if i < len(best_headers) else f"Col{i+1}"
            txt = clean_cell_text(td.get_text(" ", strip=True))
            row_dict[col] = txt

        if any(v for v in row_dict.values()):
            rows.append(row_dict)

    return rows, best_headers


# ----------------------------
# Mapping row -> output columns (strict text)
# ----------------------------
SYNONYMS_NORM = {
    "produit": ["libelle", "nom", "designation", "instrument"],
    "ssjacent": ["sousjacent", "underlying", "actifsousjacent"],
    "barriere": ["knockout", "barrier", "seuil"],
    "achat": ["buy", "coursachat", "prixachat"],
    "vente": ["sell", "coursvente", "prixvente"],
    "levier": ["lever", "leverage", "gearing"],
    "maturite": ["maturity", "echeance", "expiry"],
    "var": ["variation", "change", "evolution", "var"],
    "emetteurs": ["emetteur", "issuer", "editeur"],
}


def map_row(expected_headers: List[str], produit_type: str, row_dict: Dict[str, str], html: str, extraction_ts: datetime, source_url: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    line_text = " ".join(clean_cell_text(v) for v in row_dict.values())
    out["ISIN"] = extract_isin(line_text) or extract_isin(html)

    col_map: Dict[str, str] = {}
    for c in row_dict.keys():
        n = _norm_colname(c)
        if n and n not in col_map:
            col_map[n] = c

    def find_source_col(target_header: str) -> Optional[str]:
        tn = _norm_colname(target_header)
        if tn in col_map:
            return col_map[tn]
        for syn in SYNONYMS_NORM.get(tn, []):
            if syn in col_map:
                return col_map[syn]
        for available_norm, original in col_map.items():
            if tn in available_norm or available_norm in tn:
                return original
        return None

    for h in expected_headers:
        if h in ("ISIN", "DATE_EXTRACTION", "ProduitType", "SOURCE_URL"):
            continue
        src = find_source_col(h)
        out[h] = clean_cell_text(row_dict.get(src, "")) if src else ""

    out["DATE_EXTRACTION"] = extraction_ts.strftime("%Y-%m-%d %H:%M:%S")
    out["SOURCE_URL"] = source_url
    out["ProduitType"] = produit_type
    return out


# ----------------------------
# Group spec from config
# ----------------------------
@dataclass
class GroupSpec:
    name: str
    root_url: str
    limit_pages: int
    csv_name_pattern: str
    produit_type: str
    headers: List[str]


def build_specs(cfg: configparser.ConfigParser) -> List[GroupSpec]:
    section = "SCRAPER_SITE_BOURSORAMA"

    def spec(name: str, url_key: str, limit_key: str, csv_key: str, struct_key: str, produit_type: str) -> GroupSpec:
        root_url = get_str(cfg, section, url_key, "")
        limit_pages = get_int(cfg, section, limit_key, 0)
        csv_name_pattern = get_str(cfg, section, csv_key, f"{name}_{{date}}_{{ts}}.csv")
        headers = parse_headers(get_str(cfg, section, struct_key, "ISIN|DATE_EXTRACTION|ProduitType"))
        return GroupSpec(
            name=name,
            root_url=root_url,
            limit_pages=limit_pages,
            csv_name_pattern=csv_name_pattern,
            produit_type=produit_type,
            headers=headers,
        )

    return [
        spec("turbo", "root_url_turbo", "limit_page_turbo", "csv_name_turbo", "csv_structure_turbo", "turbo"),
        spec("warrant", "root_url_warrant", "limit_page_warrant", "csv_name_warrant", "csv_structure_warrant", "warrant"),
        spec("leverage", "root_url_leverage", "limit_page_leverage", "csv_name_leverage", "csv_structure_leverage", "leverage"),
        spec("autre_leverage", "root_url_leverageautre", "limit_page_autre_leverage", "csv_name_autre_leverage", "csv_structure_autre_leverage", "autre_leverage"),
        spec("action", "root_url_cotation", "limit_page_action", "csv_name_action", "csv_structure_action", "ACTION"),
    ]


# ----------------------------
# Worker: fetch -> save html -> parse -> delete html -> return rows
# ----------------------------
def worker_one_page(
    spec: GroupSpec,
    page: int,
    session: requests.Session,
    max_retries: int,
    retry_sleep: int,
    timeout: int,
    delay_between_requests: int,
    tmp_html_dir: Path,
) -> Tuple[int, List[Dict[str, str]], int]:
    url = spec.root_url.format(page=page)
    source_url = url

    html = fetch_with_retry(session, url, max_retries=max_retries, retry_sleep=retry_sleep, timeout=timeout)
    page_extraction_ts = datetime.now()

    tmp_html_dir.mkdir(parents=True, exist_ok=True)
    tmp_file = tmp_html_dir / f"{spec.name}_page_{page}.html"
    tmp_file.write_text(html, encoding="utf-8", errors="ignore")

    try:
        raw_rows, _headers_detected = html_to_rows_best_table(html, spec.headers)

        mapped: List[Dict[str, str]] = []
        for rr in raw_rows:
            m = map_row(spec.headers, spec.produit_type, rr, html, page_extraction_ts, source_url)
            if m.get("ISIN"):
                mapped.append(m)

        return page, mapped, len(raw_rows)

    finally:
        try:
            tmp_file.unlink(missing_ok=True)
        except Exception:
            pass

        if delay_between_requests and delay_between_requests > 0:
            time.sleep(delay_between_requests)


# ----------------------------
# Main scrape per group
# ----------------------------
def scrape_group(
    conn,
    spec: GroupSpec,
    cfg: configparser.ConfigParser,
    run_start_ts: datetime,
    file_ts: datetime,
    output_dir: Path,
    tmp_html_dir: Path,
) -> Optional[Path]:
    if not spec.root_url or spec.limit_pages <= 0:
        return None

    parallelism = get_int(cfg, "SCRAPER_PARAM", "parallelism", 10)
    delay_between_requests = get_int(cfg, "SCRAPER_PARAM", "delay_between_requests", 2)
    max_retries = get_int(cfg, "SCRAPER_PARAM", "max_retries", 3)
    retry_sleep = get_int(cfg, "SCRAPER_PARAM", "error_retry_delay", 10)
    timeout = get_int(cfg, "SCRAPER_PARAM", "timeout_seconds", 30)
    user_agent = get_str(cfg, "SCRAPER_PARAM", "user_agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

    session = build_session(user_agent)

    date_str = run_start_ts.strftime("%Y-%m-%d")
    ts = file_ts.strftime("%H%M%S")
    csv_name = spec.csv_name_pattern.format(date=date_str, ts=ts)

    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / csv_name

    sql_log_line(conn, f"START group {spec.name}", element="GROUP_START", complement=f"pages=1..{spec.limit_pages}")

    lock = Lock()
    wrote_header = False
    total_rows = 0
    total_raw = 0

    with out_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=spec.headers, delimiter="|")

        pages = list(range(1, spec.limit_pages + 1))

        with ThreadPoolExecutor(max_workers=parallelism) as ex:
            futures = [
                ex.submit(
                    worker_one_page,
                    spec,
                    p,
                    session,
                    max_retries,
                    retry_sleep,
                    timeout,
                    delay_between_requests,
                    tmp_html_dir,
                )
                for p in pages
            ]

            for fut in as_completed(futures):
                try:
                    page, rows, raw_count = fut.result()

                    with lock:
                        if not wrote_header:
                            writer.writeheader()
                            wrote_header = True
                        for r in rows:
                            writer.writerow({h: r.get(h, "") for h in spec.headers})
                        total_rows += len(rows)
                        total_raw += raw_count

                    sql_log_line(conn, f"PAGE {page} OK", element=f"{spec.name}_PAGE", complement=f"raw={raw_count}; kept={len(rows)}")

                except Exception as e:
                    sql_log_line(conn, "PAGE ERROR", element=f"{spec.name}_PAGE_ERR", complement=str(e)[:2000])

    sql_log_line(
        conn,
        f"END group {spec.name}",
        element="GROUP_END",
        complement=f"raw_total={total_raw}; rows_total={total_rows}",
        file_name=str(out_path),
    )

    return out_path


def main() -> int:
    cfg = read_config()

    # Paths: support both legacy and unified config.ini
    # - preferred: [SCRAPER_OUTPUT] output_directory
    # - fallback:  [BOURSORAMA_PATHS] data_file_directory
    # - legacy:    [BOURSO_PATHS] output_dir
    output_dir: Optional[Path] = None
    for sec, key in [
        ("SCRAPER_OUTPUT", "output_directory"),
        ("BOURSORAMA_PATHS", "data_file_directory"),
        ("BOURSO_PATHS", "output_dir"),
    ]:
        try:
            output_dir = get_path(cfg, sec, key)
            break
        except Exception:
            continue
    if output_dir is None:
        raise KeyError("Missing output directory config. Expected one of: [SCRAPER_OUTPUT] output_directory / [BOURSORAMA_PATHS] data_file_directory / [BOURSO_PATHS] output_dir")

    # Temp HTML directory (optional)
    try:
        tmp_html_dir = get_path(cfg, "BOURSO_PATHS", "tmp_html_dir")
    except Exception:
        tmp_html_dir = (REPO_ROOT / "data" / "tmp_web" / "BOURSORAMA").resolve()

    conn = sql_conn(cfg)

    latence_min = get_int(cfg, "SCRAPER_PARAM", "Latence", 5)
    try:
        duree_val = float(get_str(cfg, "SCRAPER_PARAM", "Duree", "0").strip())
    except Exception:
        duree_val = 0.0

    window_start_ts = datetime.now()
    window_end_ts = window_start_ts + timedelta(hours=duree_val) if duree_val > 0 else window_start_ts

    try:
        sql_log_line(
            conn,
            "START Boursorama parallel scraper (strict text) - WINDOW MODE",
            element="RUN_START",
            complement=f"latence_min={latence_min} | duree_h={duree_val} | window_start={window_start_ts.strftime('%Y-%m-%d %H:%M:%S')} | window_end={window_end_ts.strftime('%Y-%m-%d %H:%M:%S')}",
        )

        run_index = 0
        all_results: List[Path] = []

        while True:
            now = datetime.now()
            if duree_val > 0 and now > window_end_ts:
                break

            run_index += 1
            run_start_ts = datetime.now()
            file_ts = run_start_ts

            sql_log_line(
                conn,
                f"ITERATION {run_index} START",
                element="RUN_ITER_START",
                complement=f"run_start={run_start_ts.strftime('%Y-%m-%d %H:%M:%S')}",
            )

            specs = build_specs(cfg)

            results: List[Path] = []
            for spec in specs:
                if not spec.root_url:
                    sql_log_line(conn, f"SKIP group {spec.name}", element="GROUP_SKIP", complement="root_url empty")
                    continue
                if spec.limit_pages <= 0:
                    sql_log_line(conn, f"SKIP group {spec.name}", element="GROUP_SKIP", complement="limit_pages<=0")
                    continue

                p = scrape_group(conn, spec, cfg, run_start_ts, file_ts, output_dir, tmp_html_dir)
                if p:
                    results.append(p)

            all_results.extend(results)

            sql_log_line(
                conn,
                f"ITERATION {run_index} END",
                element="RUN_ITER_END",
                complement=f"csv_files={len(results)}",
            )

            if duree_val <= 0:
                break

            next_start = datetime.now() + timedelta(minutes=latence_min)
            if next_start > window_end_ts:
                break

            time.sleep(max(0, (next_start - datetime.now()).total_seconds()))

        sql_log_line(
            conn,
            f"DONE: runs={run_index} | csv_total={len(all_results)}",
            element="RUN_END",
        )

        print("=" * 70)
        print("BOURSORAMA SCRAPER - DONE (window mode)")
        print(f"  runs exécutés = {run_index}")
        if all_results:
            tail = all_results[-10:]
            print("  derniers CSV produits :")
            for p in tail:
                try:
                    size_kb = p.stat().st_size // 1024
                    print(f"    OK: {p.name} ({size_kb} KB)")
                except Exception:
                    print(f"    OK: {p.name}")
        print("=" * 70)

        return 0

    except Exception as e:
        msg = f"ERROR: {e}\n{traceback.format_exc()}"
        print(msg)
        try:
            sql_log_line(conn, "RUN ERROR", element="RUN_ERROR", complement=str(e)[:3500])
        except Exception:
            pass
        return 1

    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
