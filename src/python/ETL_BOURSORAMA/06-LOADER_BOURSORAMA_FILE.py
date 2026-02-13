#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
06-LOADER_BOURSORAMA_FILE_v2.py

Business logic unchanged:
- read pipe-delimited files produced by scraper
- load rows into SQL Server staging table
- write load logs into SQL Server log table
- archive processed files only when all files succeed
"""

import csv
import os
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import configparser
import pyodbc

try:
    from dateutil import parser as dtparser
except ImportError:
    dtparser = None


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

from common.config_loader import load_config

REPO_ROOT: Path

# Reuse the repo root discovered in _bootstrap_sys_path (when running inside the ESMA repo)
try:
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "config").exists() and (parent / "src" / "python").exists():
            REPO_ROOT = parent
            break
    else:
        REPO_ROOT = here.parent
except Exception:
    REPO_ROOT = Path(__file__).resolve().parent


# ============================================================
# CONFIG
# ============================================================

def _as_yes_no(value: Optional[str], default: str) -> str:
    if value is None:
        return default
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return "yes"
    if normalized in {"0", "false", "no", "n", "off"}:
        return "no"
    return default


def _is_true(value: Optional[str]) -> bool:
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _quote_ident(name: str) -> str:
    name = (name or "").strip()
    if not name:
        raise ValueError("Empty SQL identifier")
    return "[" + name.replace("]", "]]") + "]"


def _fq_name(schema: str, table: str) -> str:
    return f"{_quote_ident(schema)}.{_quote_ident(table)}"


def _normalize_dt_sqlserver(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is not None:
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def load_runtime_config() -> Tuple[dict, Path, Path]:
    """
    Expected config sections/keys:

    [SQLSERVER]
    server = ...
    database_stg = ...
    schema_stg = stg
    schema_log = log
    user/password or trusted_connection = yes

    [BOURSO_DATA_FILE]
    directory = D:\\...\\data\\csv\\BOURSORAMA

    [BOURSO_ARCH_DIR]
    directory = D:\\...\\data\\archive\\BOURSORAMA
    """
    candidates = []
    env_path = os.environ.get("KHL_CONFIG_INI") or os.environ.get("ESMA_CONFIG_INI")
    if env_path:
        candidates.append(Path(env_path))
    candidates.extend(
        [
            REPO_ROOT / "config" / "config.ini",
            REPO_ROOT / "config.ini",
            Path(__file__).resolve().parent / "config.ini",
        ]
    )

    cfg: configparser.ConfigParser
    for p in candidates:
        try:
            if p and p.exists() and p.is_file():
                cfg = configparser.ConfigParser()
                cfg.read(p, encoding="utf-8")
                break
        except Exception:
            continue
    else:
        cfg = load_config()

    def pick_section(*names: str) -> Optional[str]:
        for n in names:
            if n in cfg:
                return n
        for n in names:
            for s in cfg.sections():
                if s.lower() == n.lower():
                    return s
        return None

    sql_section = pick_section("SQLSERVER")
    if not sql_section:
        raise ValueError("Missing section [SQLSERVER] in config.ini")

    sqlsec = cfg[sql_section]
    sql_cfg = {
        "server": sqlsec.get("server", "").strip(),
        "database": (
            sqlsec.get("database_stg", "").strip() or sqlsec.get("database", "").strip()
        ),
        "schema_stg": sqlsec.get("schema_stg", "stg").strip() or "stg",
        "schema_log": sqlsec.get("schema_log", "log").strip() or "log",
        "user": sqlsec.get("user", "").strip(),
        "password": sqlsec.get("password", "").strip(),
        "driver": sqlsec.get("driver", "ODBC Driver 17 for SQL Server").strip(),
        "encrypt": _as_yes_no(sqlsec.get("encrypt", "no"), default="no"),
        "trust_server_certificate": _as_yes_no(
            sqlsec.get("trust_server_certificate", "yes"), default="yes"
        ),
        "connection_timeout": sqlsec.get("connection_timeout", "30").strip() or "30",
    }

    missing = [k for k in ("server", "database") if not sql_cfg.get(k)]
    if missing:
        raise ValueError(f"Missing SQLSERVER keys: {missing}")

    use_trusted = _is_true(sqlsec.get("trusted_connection", ""))
    if not use_trusted and not (sql_cfg["user"] and sql_cfg["password"]):
        # Fallback to current Windows account when SQL credentials are not configured.
        use_trusted = True
    sql_cfg["use_trusted_connection"] = use_trusted

    data_dir: Optional[Path] = None
    arch_dir: Optional[Path] = None

    b_section = pick_section("BOURSORAMA_PATHS")
    if b_section:
        try:
            data_dir = Path(cfg.get(b_section, "data_file_directory")).expanduser().resolve()
            arch_dir = Path(cfg.get(b_section, "archive_directory")).expanduser().resolve()
        except Exception:
            data_dir = None
            arch_dir = None

    if data_dir is None or arch_dir is None:
        data_sec = pick_section("BOURSO_DATA_FILE")
        arch_sec = pick_section("BOURSO_ARCH_DIR")
        if data_sec and arch_sec:
            data_dir = Path(cfg.get(data_sec, "directory")).expanduser().resolve()
            arch_dir = Path(cfg.get(arch_sec, "directory")).expanduser().resolve()

    if data_dir is None or arch_dir is None:
        raise ValueError(
            "Missing Boursorama directories in config.ini. Expected either: "
            "[BOURSORAMA_PATHS] data_file_directory + archive_directory, "
            "or legacy [BOURSO_DATA_FILE]/[BOURSO_ARCH_DIR] with directory keys."
        )

    if not data_dir.exists():
        raise FileNotFoundError(f"BOURSO_DATA_FILE directory not found: {data_dir}")
    arch_dir.mkdir(parents=True, exist_ok=True)

    return sql_cfg, data_dir, arch_dir


# ============================================================
# TABLES / COLUMNS
# ============================================================

TARGET_TABLE = "stg_bourso_price_history"
LOG_TABLE = "finance_load_log"

TARGET_COLS = [
    "isin",
    "libelle",
    "produit",
    "produit_type",
    "source_url",
    "date_extraction",
    "dernier",
    "variation",
    "ouverture",
    "plus_haut",
    "plus_bas",
    "var_1janv",
    "volume",
    "achat",
    "vente",
    "sous_jacent",
    "ss_jacent",
    "borne_basse",
    "borne_haute",
    "barriere",
    "levier",
    "prix_exercice",
    "maturite",
    "delta",
    "emetteurs",
    "file_name",
]


# ============================================================
# MAPPING
# ============================================================

def _strip_accents(s: str) -> str:
    import unicodedata

    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")


def normalize_header(h: str) -> str:
    h = (h or "").strip()
    h = _strip_accents(h)
    h = h.replace("\ufeff", "")
    h = h.lower()
    h = h.replace("+", "plus_")
    h = h.replace("/", "_")
    h = h.replace(".", "")
    h = h.replace(" ", "_")
    h = h.replace("-", "_")
    while "__" in h:
        h = h.replace("__", "_")
    return h


HEADER_MAP = {
    "isin": "isin",
    "libelle": "libelle",
    "produit": "produit",
    "produittype": "produit_type",
    "produit_type": "produit_type",
    "source_url": "source_url",
    "date_extraction": "date_extraction",
    "dernier": "dernier",
    "var": "variation",
    "ouv": "ouverture",
    "plus_haut": "plus_haut",
    "plus_bas": "plus_bas",
    "var_1janv": "var_1janv",
    "vol": "volume",
    "sous_jacent": "sous_jacent",
    "ss_jacent": "ss_jacent",
    "borne_basse": "borne_basse",
    "borne_haute": "borne_haute",
    "achat": "achat",
    "vente": "vente",
    "barriere": "barriere",
    "levier": "levier",
    "maturite": "maturite",
    "prix_ex": "prix_exercice",
    "prix_exercice": "prix_exercice",
    "delta": "delta",
    "emetteurs": "emetteurs",
}


def parse_date_extraction(v: Optional[str]) -> Optional[datetime]:
    if v is None:
        return None
    v = str(v).strip()
    if not v:
        return None
    try:
        if dtparser is not None:
            dt = dtparser.parse(v)
        else:
            dt = datetime.fromisoformat(v)
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def guess_type_from_filename(fn: str) -> str:
    name = fn.lower()
    if "turbo" in name:
        return "turbo"
    if "warrant" in name:
        return "warrant"
    if "leverage" in name and ("autre" in name or "autres" in name):
        return "autre_leverage"
    if "autre" in name and "levier" in name:
        return "autre_leverage"
    if "leverage" in name:
        return "leverage"
    if "action" in name or "actions" in name:
        return "ACTION"
    return "unknown"


def detect_produit_type(norm_row: Dict[str, str], fallback: str) -> str:
    v = (norm_row.get("produittype") or norm_row.get("produit_type") or "").strip()
    return v if v else fallback


# ============================================================
# FILES
# ============================================================

def iter_input_files(input_dir: Path) -> List[Path]:
    patterns = ["*.csv", "*.txt", "*.tsv", "*.psv"]
    out: List[Path] = []
    for p in patterns:
        out.extend(sorted(input_dir.glob(p)))
    return out


def read_pipe_file(path: Path) -> List[Dict[str, str]]:
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="|")
        if not reader.fieldnames:
            return []
        return list(reader)


def transform_rows(
    rows: List[Dict[str, str]], file_name: str
) -> Tuple[List[Tuple], Optional[str], Optional[datetime], str]:
    out: List[Tuple] = []

    source_url = None
    snapshot_ts = None
    produit_type = guess_type_from_filename(file_name)

    if rows:
        norm0 = {normalize_header(k): (v.strip() if isinstance(v, str) else v) for k, v in rows[0].items()}
        source_url = (norm0.get("source_url") or "").strip() or None
        snapshot_ts = _normalize_dt_sqlserver(parse_date_extraction(norm0.get("date_extraction")))
        produit_type = detect_produit_type(norm0, produit_type)

    for r in rows:
        norm = {normalize_header(k): (v.strip() if isinstance(v, str) else v) for k, v in r.items()}
        ptype = detect_produit_type(norm, guess_type_from_filename(file_name))

        dt = _normalize_dt_sqlserver(parse_date_extraction(norm.get("date_extraction")))
        src_url = (norm.get("source_url") or "").strip() or None

        rec: Dict[str, object] = {c: None for c in TARGET_COLS}
        rec["produit_type"] = ptype
        rec["date_extraction"] = dt
        rec["source_url"] = src_url
        rec["file_name"] = file_name

        for k, v in norm.items():
            tgt = HEADER_MAP.get(k)
            if not tgt:
                continue
            if tgt in ("produit_type", "date_extraction", "source_url"):
                continue
            rec[tgt] = v

        out.append(tuple(rec[c] for c in TARGET_COLS))

    return out, source_url, snapshot_ts, produit_type


# ============================================================
# DB LOG + INSERT
# ============================================================

def connect_sqlserver(sql_cfg: dict) -> pyodbc.Connection:
    parts = [
        f"DRIVER={{{sql_cfg['driver']}}}",
        f"SERVER={sql_cfg['server']}",
        f"DATABASE={sql_cfg['database']}",
        f"Encrypt={sql_cfg['encrypt']}",
        f"TrustServerCertificate={sql_cfg['trust_server_certificate']}",
        f"Connection Timeout={sql_cfg['connection_timeout']}",
    ]
    if sql_cfg.get("use_trusted_connection"):
        parts.append("Trusted_Connection=yes")
    else:
        parts.append(f"UID={sql_cfg['user']}")
        parts.append(f"PWD={sql_cfg['password']}")
    conn_str = ";".join(parts) + ";"
    return pyodbc.connect(conn_str, autocommit=False)


def log_started(
    cur,
    log_schema: str,
    process_name: str,
    script_name: str,
    target_table: str,
    file_name: str,
    source_url: Optional[str],
    produit_type: Optional[str],
    snapshot_ts: Optional[datetime],
) -> int:
    q = f"""
        INSERT INTO {_fq_name(log_schema, LOG_TABLE)} (
            process_name, script_name, source_system, target_table,
            file_name, source_url, produit_type, snapshot_ts,
            status, load_start_ts
        )
        OUTPUT INSERTED.load_log_id
        VALUES (?, ?, 'BOURSORAMA', ?, ?, ?, ?, ?, 'STARTED', SYSUTCDATETIME())
    """

    cur.execute(
        q,
        (
            process_name,
            script_name,
            target_table,
            file_name,
            source_url,
            produit_type,
            _normalize_dt_sqlserver(snapshot_ts),
        ),
    )
    row = cur.fetchone()
    if not row:
        raise RuntimeError("Unable to retrieve load_log_id after STARTED insert")
    return int(row[0])


def log_finished(
    cur,
    log_schema: str,
    load_log_id: int,
    status: str,
    rows_read: int,
    rows_inserted: int,
    rows_rejected: int,
    error_message: Optional[str],
) -> None:
    q = f"""
        UPDATE {_fq_name(log_schema, LOG_TABLE)}
        SET status=?,
            load_end_ts=SYSUTCDATETIME(),
            rows_read=?,
            rows_inserted=?,
            rows_rejected=?,
            error_message=?
        WHERE load_log_id=?
    """

    cur.execute(
        q,
        (status, rows_read, rows_inserted, rows_rejected, error_message, load_log_id),
    )


def insert_rows(cur, stg_schema: str, rows: List[Tuple]) -> int:
    if not rows:
        return 0

    cols = ", ".join(_quote_ident(c) for c in TARGET_COLS)
    placeholders = ", ".join("?" for _ in TARGET_COLS)
    q = f"INSERT INTO {_fq_name(stg_schema, TARGET_TABLE)} ({cols}) VALUES ({placeholders})"

    prepared_rows = [
        tuple(_normalize_dt_sqlserver(v) if isinstance(v, datetime) else v for v in row)
        for row in rows
    ]
    try:
        cur.fast_executemany = True
    except Exception:
        pass

    cur.executemany(q, prepared_rows)
    return len(prepared_rows)


def ensure_target_tables(cur, stg_schema: str, log_schema: str) -> None:
    stg_schema_lit = stg_schema.replace("'", "''")
    log_schema_lit = log_schema.replace("'", "''")

    cur.execute(
        f"IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'{stg_schema_lit}') EXEC('CREATE SCHEMA {_quote_ident(stg_schema)}')"
    )
    cur.execute(
        f"IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'{log_schema_lit}') EXEC('CREATE SCHEMA {_quote_ident(log_schema)}')"
    )

    cur.execute(
        f"""
IF OBJECT_ID(N'{stg_schema_lit}.{TARGET_TABLE}', 'U') IS NULL
BEGIN
    CREATE TABLE {_fq_name(stg_schema, TARGET_TABLE)} (
        [stg_id] BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        [isin] NVARCHAR(100) NULL,
        [libelle] NVARCHAR(500) NULL,
        [produit] NVARCHAR(500) NULL,
        [produit_type] NVARCHAR(100) NOT NULL,
        [source_url] NVARCHAR(1000) NULL,
        [date_extraction] DATETIME2(0) NULL,
        [dernier] NVARCHAR(100) NULL,
        [variation] NVARCHAR(100) NULL,
        [ouverture] NVARCHAR(100) NULL,
        [plus_haut] NVARCHAR(100) NULL,
        [plus_bas] NVARCHAR(100) NULL,
        [var_1janv] NVARCHAR(100) NULL,
        [volume] NVARCHAR(100) NULL,
        [achat] NVARCHAR(100) NULL,
        [vente] NVARCHAR(100) NULL,
        [sous_jacent] NVARCHAR(500) NULL,
        [ss_jacent] NVARCHAR(500) NULL,
        [borne_basse] NVARCHAR(100) NULL,
        [borne_haute] NVARCHAR(100) NULL,
        [barriere] NVARCHAR(100) NULL,
        [levier] NVARCHAR(100) NULL,
        [prix_exercice] NVARCHAR(100) NULL,
        [maturite] NVARCHAR(100) NULL,
        [delta] NVARCHAR(100) NULL,
        [emetteurs] NVARCHAR(500) NULL,
        [load_ts] DATETIME2(0) NOT NULL CONSTRAINT DF_{TARGET_TABLE}_load_ts DEFAULT SYSUTCDATETIME(),
        [file_name] NVARCHAR(260) NOT NULL
    )
END
"""
    )

    cur.execute(
        f"""
IF OBJECT_ID(N'{log_schema_lit}.{LOG_TABLE}', 'U') IS NULL
BEGIN
    CREATE TABLE {_fq_name(log_schema, LOG_TABLE)} (
        [load_log_id] BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        [process_name] NVARCHAR(200) NOT NULL,
        [script_name] NVARCHAR(260) NULL,
        [source_system] NVARCHAR(100) NOT NULL CONSTRAINT DF_{LOG_TABLE}_source DEFAULT N'BOURSORAMA',
        [target_table] NVARCHAR(260) NOT NULL,
        [file_name] NVARCHAR(260) NULL,
        [source_url] NVARCHAR(1000) NULL,
        [produit_type] NVARCHAR(100) NULL,
        [snapshot_ts] DATETIME2(0) NULL,
        [load_start_ts] DATETIME2(0) NOT NULL CONSTRAINT DF_{LOG_TABLE}_start DEFAULT SYSUTCDATETIME(),
        [load_end_ts] DATETIME2(0) NULL,
        [status] NVARCHAR(30) NOT NULL CONSTRAINT DF_{LOG_TABLE}_status DEFAULT N'STARTED',
        [rows_read] BIGINT NULL,
        [rows_inserted] BIGINT NULL,
        [rows_rejected] BIGINT NULL,
        [error_message] NVARCHAR(MAX) NULL,
        [checksum] NVARCHAR(200) NULL,
        [params] NVARCHAR(MAX) NULL,
        [extra] NVARCHAR(MAX) NULL
    )
END
"""
    )


# ============================================================
# ARCHIVE
# ============================================================

def safe_move(src: Path, dst_dir: Path) -> Path:
    dst_dir.mkdir(parents=True, exist_ok=True)
    dst = dst_dir / src.name
    if not dst.exists():
        shutil.move(str(src), str(dst))
        return dst
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dst2 = dst_dir / f"{src.stem}_{stamp}{src.suffix}"
    shutil.move(str(src), str(dst2))
    return dst2


# ============================================================
# MAIN
# ============================================================

def main() -> int:
    sql_cfg, data_dir, arch_dir = load_runtime_config()
    stg_schema = sql_cfg["schema_stg"]
    log_schema = sql_cfg["schema_log"]

    files = iter_input_files(data_dir)
    print(f"[INFO] DATA_DIR = {data_dir}")
    print(f"[INFO] ARCH_DIR = {arch_dir}")
    print(f"[INFO] Fichiers trouves = {len(files)}")

    if not files:
        print("[INFO] Aucun fichier a charger")
        return 0

    process_name = "boursorama_stg_load"
    script_name = Path(__file__).name
    target_table_fq = f"{stg_schema}.{TARGET_TABLE}"

    failed: List[Tuple[str, str]] = []
    processed: List[Path] = []

    conn = connect_sqlserver(sql_cfg)
    try:
        with conn.cursor() as cur:
            ensure_target_tables(cur, stg_schema, log_schema)
        conn.commit()

        for fp in files:
            file_name = fp.name
            load_log_id = None
            rows_read = 0
            rows_inserted = 0
            rows_rejected = 0

            try:
                rows = read_pipe_file(fp)
                rows_read = len(rows)
                if rows_read == 0:
                    print(f"[WARN] {file_name}: vide/non lisible -> ignore")
                    continue

                transformed, source_url, snapshot_ts, produit_type = transform_rows(rows, file_name)

                with conn.cursor() as cur:
                    load_log_id = log_started(
                        cur,
                        log_schema,
                        process_name,
                        script_name,
                        target_table_fq,
                        file_name,
                        source_url,
                        produit_type,
                        snapshot_ts,
                    )
                    rows_inserted = insert_rows(cur, stg_schema, transformed)
                    log_finished(
                        cur,
                        log_schema,
                        load_log_id,
                        "SUCCESS",
                        rows_read,
                        rows_inserted,
                        rows_rejected,
                        None,
                    )

                conn.commit()
                processed.append(fp)
                print(f"[OK] {file_name}: read={rows_read}, inserted={rows_inserted}")

            except Exception as e:
                conn.rollback()
                err = f"{type(e).__name__}: {e}"
                failed.append((file_name, err))
                print(f"[ERR] {file_name}: {err}", file=sys.stderr)

                try:
                    with conn.cursor() as cur:
                        if load_log_id is None:
                            load_log_id = log_started(
                                cur,
                                log_schema,
                                process_name,
                                script_name,
                                target_table_fq,
                                file_name,
                                None,
                                guess_type_from_filename(file_name),
                                None,
                            )
                        log_finished(
                            cur,
                            log_schema,
                            load_log_id,
                            "FAILED",
                            rows_read,
                            rows_inserted,
                            rows_rejected,
                            err,
                        )
                    conn.commit()
                except Exception:
                    conn.rollback()

        if failed:
            print("\n[INFO] Au moins un fichier a echoue => aucun archivage effectue.")
            for fn, er in failed:
                print(f"  - {fn}: {er}")
            return 2

        print("\n[INFO] Tous les fichiers OK => archivage...")
        moved = 0
        for fp in processed:
            dst = safe_move(fp, arch_dir)
            moved += 1
            print(f"[ARCH] {fp.name} -> {dst}")
        print(f"[DONE] Archivage termine. Fichiers archives: {moved}")
        return 0

    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
