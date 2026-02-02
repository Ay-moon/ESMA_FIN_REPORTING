#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
ETL_ESMA_DAILY_RUN_AUTONOME.py
=============================

Orchestrateur DAILY ESMA – SIMPLE

Chaîne d'exécution :
1) 01-ETL_ESMA_DAILY_RUN_GET_FILES_AUTONOME.py
2) 02-ETL_ESMA_DAILY_BUILD_CSV_AUTONOME.py
3) 03-ETL_ESMA_DAILY_LOAD_STG_DLTINS_FULINS_AUTONOME.py
4) 04-ETL_ESMA_DAILY_RUN_PROCS_AUTONOME.py

Règles :
- Aucun paramètre CLI
- Stop immédiat si un script échoue
- Chaque script gère son propre logging SQL

Correctif (2026-01-27):
- Assure que les imports partagés (package `common`, etc.) fonctionnent même
  en exécution standalone via `subprocess` en injectant `src/python` dans PYTHONPATH.
"""

import os
import subprocess
import sys
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).resolve().parent

SCRIPTS = [
    "01-ETL_ESMA_DAILY_RUN_GET_FILES_AUTONOME.py",
    "02-ETL_ESMA_DAILY_BUILD_CSV_AUTONOME.py",
    "03-ETL_ESMA_DAILY_LOAD_STG_DLTINS_FULINS_AUTONOME.py",
    "04-ETL_ESMA_DAILY_RUN_PROCS_AUTONOME.py",
]

def resolve_project_root() -> Path:
    """Remonte depuis ce fichier jusqu'à trouver un dossier `config/` (repo root)."""
    here = Path(__file__).resolve()
    for parent in [here.parent] + list(here.parents):
        if (parent / "config").is_dir():
            return parent
    # fallback : deux niveaux au-dessus de src/python/...
    return here.parents[3] if len(here.parents) >= 4 else Path.cwd().resolve()

def build_env() -> dict:
    """Construit l'environnement subprocess avec PYTHONPATH incluant src/python."""
    env = os.environ.copy()
    project_root = resolve_project_root()
    src_python = project_root / "src" / "python"

    # On ajoute aussi BASE_DIR (le dossier ETL_FULIN_DTIN) pour les imports locaux éventuels
    extra_paths = [str(src_python), str(BASE_DIR)]

    current = env.get("PYTHONPATH", "").strip()
    parts = [p for p in current.split(os.pathsep) if p] if current else []
    # prepend to take precedence
    for p in reversed(extra_paths):
        if p not in parts:
            parts.insert(0, p)

    env["PYTHONPATH"] = os.pathsep.join(parts)
    return env

def run_script(script_path: Path, env: dict) -> None:
    print(f"[ETL] START {script_path.name}")
    result = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=BASE_DIR,
        env=env,
    )
    if result.returncode != 0:
        raise RuntimeError(f"FAILED: {script_path.name}")
    print(f"[ETL] OK    {script_path.name}")

def main() -> int:
    env = build_env()
    print(f"[ETL] DAILY RUN START {datetime.now():%Y-%m-%d %H:%M:%S}")
    print(f"[ETL] BASE_DIR      : {BASE_DIR}")
    print(f"[ETL] PYTHONPATH    : {env.get('PYTHONPATH','')}")
    for name in SCRIPTS:
        path = BASE_DIR / name
        if not path.exists():
            raise FileNotFoundError(f"Missing script: {path}")
        run_script(path, env)
    print(f"[ETL] DAILY RUN END   {datetime.now():%Y-%m-%d %H:%M:%S}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
