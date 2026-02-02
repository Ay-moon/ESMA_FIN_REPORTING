#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
06-LOADER_BOURSORAMA_FILE_v2.py

Same business logic as original loader.
Only refactor: configuration loading via common.config_loader.load_config()
(no bbs_server.local.ini discovery, no raw parsing trick).
"""

import csv
import os
import re
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import configparser
import psycopg
from psycopg import sql

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
    # _bootstrap_sys_path sets sys.path and can infer REPO_ROOT by walking parents
    # We recompute it here to keep this script standalone.
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
# CONFIG (refactor-only)
# ============================================================

def load_runtime_config() -> Tuple[dict, Path, Path]:
    """
    Expected config sections/keys:

    [POSTGRES]
    host = ...
    port = 5432
    database = ...
    user = ...
    password = ...
    schema = bi_master_securities

    [BOURSO_DATA_FILE]
    directory = D:\\...\\data\\csv\\BOURSORAMA

    [BOURSO_ARCH_DIR]
    directory = D:\\...\\data\\archive\\BOURSORAMA
    """
    # Prefer a unified config.ini if present (repo root or script folder), else fallback
    # to the repo standard loader.
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
        # section names are case-sensitive in configparser
        for n in names:
            for s in cfg.sections():
                if s.lower() == n.lower():
                    return s
        return None

    pg_section = pick_section("Postgres", "POSTGRES")
    if not pg_section:
        raise ValueError("Missing section [Postgres] (or [POSTGRES]) in config.ini")

    pgsec = cfg[pg_section]
    pg = {
        "host": pgsec.get("host", "").strip(),
        "port": int(pgsec.get("port", "5432").strip()),
        "dbname": pgsec.get("database", "").strip(),
        "user": pgsec.get("user", "").strip(),
        "password": pgsec.get("password", "").strip(),
        "schema": pgsec.get("schema", "bi_master_securities").strip(),
    }

    missing = [k for k in ("host", "dbname", "user", "password") if not pg.get(k)]
    if missing:
        raise ValueError(f"Missing POSTGRES keys: {missing}")

    # Input + archive directories: support both legacy and unified config.ini
    data_dir: Optional[Path] = None
    arch_dir: Optional[Path] = None

    # Unified
    b_section = pick_section("BOURSORAMA_PATHS")
    if b_section:
        try:
            data_dir = Path(cfg.get(b_section, "data_file_directory")).expanduser().resolve()
            arch_dir = Path(cfg.get(b_section, "archive_directory")).expanduser().resolve()
        except Exception:
            data_dir = None
            arch_dir = None

    # Legacy
    if data_dir is None or arch_dir is None:
        if pick_section("BOURSO_DATA_FILE") and pick_section("BOURSO_ARCH_DIR"):
            data_dir = Path(cfg.get(pick_section("BOURSO_DATA_FILE"), "directory")).expanduser().resolve()
            arch_dir = Path(cfg.get(pick_section("BOURSO_ARCH_DIR"), "directory")).expanduser().resolve()

    if data_dir is None or arch_dir is None:
        raise ValueError(
            "Missing Boursorama directories in config.ini. Expected either: "
            "[BOURSORAMA_PATHS] data_file_directory + archive_directory, "
            "or legacy [BOURSO_DATA_FILE]/[BOURSO_ARCH_DIR] with directory keys."
        )

    if not data_dir.exists():
        raise FileNotFoundError(f"BOURSO_DATA_FILE directory not found: {data_dir}")
    arch_dir.mkdir(parents=True, exist_ok=True)

    return pg, data_dir, arch_dir


# ============================================================
# TABLES / COLONNES (business unchanged)
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
# MAPPING (business unchanged)
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
# FILES (business unchanged)
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


def transform_rows(rows: List[Dict[str, str]], file_name: str) -> Tuple[List[Tuple], Optional[str], Optional[datetime], str]:
    out: List[Tuple] = []

    source_url = None
    snapshot_ts = None
    produit_type = guess_type_from_filename(file_name)

    if rows:
        norm0 = {normalize_header(k): (v.strip() if isinstance(v, str) else v) for k, v in rows[0].items()}
        source_url = (norm0.get("source_url") or "").strip() or None
        snapshot_ts = parse_date_extraction(norm0.get("date_extraction"))
        produit_type = detect_produit_type(norm0, produit_type)

    for r in rows:
        norm = {normalize_header(k): (v.strip() if isinstance(v, str) else v) for k, v in r.items()}
        ptype = detect_produit_type(norm, guess_type_from_filename(file_name))

        dt = parse_date_extraction(norm.get("date_extraction"))
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
# DB LOG + INSERT (business unchanged)
# ============================================================

def log_started(cur, schema: str, process_name: str, script_name: str,
                target_table: str, file_name: str, source_url: Optional[str],
                produit_type: Optional[str], snapshot_ts: Optional[datetime]) -> int:
    q = sql.SQL("""
        INSERT INTO {}.{} (
            process_name, script_name, source_system, target_table,
            file_name, source_url, produit_type, snapshot_ts,
            status, load_start_ts
        )
        VALUES (%s, %s, 'BOURSORAMA', %s, %s, %s, %s, %s, 'STARTED', NOW())
        RETURNING load_log_id
    """).format(sql.Identifier(schema), sql.Identifier(LOG_TABLE))

    cur.execute(q, (process_name, script_name, target_table, file_name, source_url, produit_type, snapshot_ts))
    return cur.fetchone()[0]


def log_finished(cur, schema: str, load_log_id: int, status: str,
                 rows_read: int, rows_inserted: int, rows_rejected: int,
                 error_message: Optional[str]) -> None:
    q = sql.SQL("""
        UPDATE {}.{}
        SET status=%s,
            load_end_ts=NOW(),
            rows_read=%s,
            rows_inserted=%s,
            rows_rejected=%s,
            error_message=%s
        WHERE load_log_id=%s
    """).format(sql.Identifier(schema), sql.Identifier(LOG_TABLE))

    cur.execute(q, (status, rows_read, rows_inserted, rows_rejected, error_message, load_log_id))


def insert_rows(cur, schema: str, rows: List[Tuple]) -> int:
    if not rows:
        return 0
    cols = sql.SQL(", ").join(map(sql.Identifier, TARGET_COLS))
    placeholders = sql.SQL(", ").join(sql.Placeholder() * len(TARGET_COLS))
    q = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
        sql.Identifier(schema),
        sql.Identifier(TARGET_TABLE),
        cols,
        placeholders,
    )
    cur.executemany(q, rows)
    return len(rows)


# ============================================================
# ARCHIVE (business unchanged)
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
# MAIN (business unchanged)
# ============================================================

def main() -> int:
    pg, data_dir, arch_dir = load_runtime_config()
    schema = pg["schema"]

    files = iter_input_files(data_dir)
    print(f"[INFO] DATA_DIR = {data_dir}")
    print(f"[INFO] ARCH_DIR = {arch_dir}")
    print(f"[INFO] Fichiers trouvés = {len(files)}")

    if not files:
        print("[INFO] Aucun fichier à charger")
        return 0

    process_name = "boursorama_stg_load"
    script_name = Path(__file__).name
    target_table_fq = f"{schema}.{TARGET_TABLE}"

    failed: List[Tuple[str, str]] = []
    processed: List[Path] = []

    conn = psycopg.connect(
        host=pg["host"], port=pg["port"], dbname=pg["dbname"],
        user=pg["user"], password=pg["password"], connect_timeout=15
    )
    try:
        conn.autocommit = False

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
                    print(f"[WARN] {file_name}: vide/non lisible -> ignoré")
                    continue

                transformed, source_url, snapshot_ts, produit_type = transform_rows(rows, file_name)

                with conn.cursor() as cur:
                    load_log_id = log_started(
                        cur, schema, process_name, script_name,
                        target_table_fq, file_name, source_url, produit_type, snapshot_ts
                    )
                    rows_inserted = insert_rows(cur, schema, transformed)
                    log_finished(cur, schema, load_log_id, "SUCCESS", rows_read, rows_inserted, rows_rejected, None)

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
                                cur, schema, process_name, script_name,
                                target_table_fq, file_name, None, guess_type_from_filename(file_name), None
                            )
                        log_finished(cur, schema, load_log_id, "FAILED", rows_read, rows_inserted, rows_rejected, err)
                    conn.commit()
                except Exception:
                    conn.rollback()

        if failed:
            print("\n[INFO] Au moins un fichier a échoué => aucun archivage effectué.")
            for fn, er in failed:
                print(f"  - {fn}: {er}")
            return 2

        print("\n[INFO] Tous les fichiers OK => archivage...")
        moved = 0
        for fp in processed:
            dst = safe_move(fp, arch_dir)
            moved += 1
            print(f"[ARCH] {fp.name} -> {dst}")
        print(f"[DONE] Archivage terminé. Fichiers archivés: {moved}")
        return 0

    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
