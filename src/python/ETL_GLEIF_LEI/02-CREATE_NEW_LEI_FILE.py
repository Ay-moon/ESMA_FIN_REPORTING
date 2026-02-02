import os
import re
import csv
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import sys

def _bootstrap_sys_path() -> None:
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "config").exists() and (parent / "src" / "python").exists():
            sys.path.insert(0, str(parent / "src" / "python"))
            return
    raise RuntimeError("Repo root not found (expected 'config' and 'src/python').")

_bootstrap_sys_path()

# --- NEW: shared config loader (ESMA standard) ---
from common.config_loader import load_config


# ==========================
# SETTINGS (no CLI params)
# ==========================

MAPPING_FILENAME = "LEI_MAPPING.cfg.txt"   # mapping file in the same folder as the script (or ./config)
OUTPUT_SUBFOLDER = "filtered"              # output folder inside GLEIF_CSV directory
ENCODING_IN = "utf-8-sig"
ENCODING_OUT = "utf-8-sig"

FILE_PATTERNS = {
    "lei2-golden": re.compile(r"gleif-goldencopy-lei2-.*\.csv$", re.IGNORECASE),
    "rr-golden": re.compile(r"gleif-goldencopy-rr-.*\.csv$", re.IGNORECASE),
    "repex-golden": re.compile(r"gleif-goldencopy-repex-.*\.csv$", re.IGNORECASE),
}


# ==========================
# Helpers
# ==========================

def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def die(msg: str, code: int = 1) -> None:
    print(f"{now_str()} [ERROR] {msg}")
    sys.exit(code)


def find_mapping_file() -> Path:
    script_dir = Path(__file__).resolve().parent
    candidates = [
        script_dir / MAPPING_FILENAME,
        script_dir / "config" / MAPPING_FILENAME,
    ]
    for p in candidates:
        if p.exists():
            return p
    raise FileNotFoundError(
        f"Mapping file not found. Expected one of:\n" +
        "\n".join(str(p.resolve()) for p in candidates)
    )


def pick_latest_csv(folder: Path, pattern: re.Pattern) -> Optional[Path]:
    candidates = [p for p in folder.glob("*.csv") if pattern.search(p.name)]
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_mtime)


def read_mapping(mapping_path: Path) -> Dict[str, List[Tuple[str, str]]]:
    """
    Reads mapping file with pipe delimiter:
      FILE|TABLE|old column|new col
    Returns:
      {file_key: [(old_col, new_col), ...]} in file order.
    """
    out: Dict[str, List[Tuple[str, str]]] = {}
    with mapping_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="|")
        required = {"FILE", "old column", "new col"}
        if not reader.fieldnames or not required.issubset(set(reader.fieldnames)):
            die(f"Mapping file header must contain at least: {sorted(required)}. Found: {reader.fieldnames}")

        for row in reader:
            file_key = (row.get("FILE") or "").strip()
            old_c = (row.get("old column") or "").strip()
            new_c = (row.get("new col") or "").strip()
            if not file_key or not old_c or not new_c:
                continue
            out.setdefault(file_key, []).append((old_c, new_c))
    return out


def extract_columns(
    src_csv: Path,
    dst_csv: Path,
    pairs: List[Tuple[str, str]],
) -> int:
    """
    Creates dst_csv with headers = new cols, values from src old cols.
    Fail-fast if any old col is missing in the source header.
    Returns number of written rows.
    """
    with src_csv.open("r", encoding=ENCODING_IN, newline="") as fin:
        reader = csv.DictReader(fin)
        if reader.fieldnames is None:
            die(f"No header found in source CSV: {src_csv}")

        src_headers = set(reader.fieldnames)

        missing = [old for (old, _) in pairs if old not in src_headers]
        if missing:
            die(
                f"Missing required columns in {src_csv.name}:\n" +
                "\n".join(missing)
            )

        out_headers = [new for (_, new) in pairs]

        dst_csv.parent.mkdir(parents=True, exist_ok=True)
        with dst_csv.open("w", encoding=ENCODING_OUT, newline="") as fout:
            writer = csv.DictWriter(
                fout,
                fieldnames=out_headers,
                quoting=csv.QUOTE_MINIMAL
            )
            writer.writeheader()

            n = 0
            for row in reader:
                out_row = {}
                for old, new in pairs:
                    v = row.get(old)
                    if v is not None:
                        v = v.strip()
                        if v == "":
                            v = None
                    out_row[new] = v
                writer.writerow(out_row)
                n += 1

    return n


def main():
    # --- NEW: load config via shared loader (ESMA standard) ---
    cfg = load_config()

    # We only need the CSV directory here (business unchanged)
    try:
        gleif_csv_dir = Path(cfg.get("GLEIF_CSV", "directory")).expanduser().resolve()
    except Exception:
        die("Missing config value: [GLEIF_CSV] directory (in config/config.ini or config.template.ini)")

    if not gleif_csv_dir.exists():
        die(f"GLEIF_CSV directory not found: {gleif_csv_dir}")

    mapping_path = find_mapping_file()
    mapping = read_mapping(mapping_path)

    # Ensure we have mappings for all 3 files
    for k in ("lei2-golden", "rr-golden", "repex-golden"):
        if k not in mapping:
            die(f"Mapping file does not contain entries for '{k}'. Check {mapping_path.name}.")

    # Locate latest source files
    sources: Dict[str, Path] = {}
    for file_key, pat in FILE_PATTERNS.items():
        p = pick_latest_csv(gleif_csv_dir, pat)
        if p is None:
            die(f"No matching source CSV found for {file_key} in {gleif_csv_dir}")
        sources[file_key] = p

    out_dir = gleif_csv_dir / OUTPUT_SUBFOLDER
    print(f"{now_str()} [OK] CONFIG - Using config: config/config.ini (or config.template.ini)")
    print(f"{now_str()} [OK] CONFIG - Using mapping: {mapping_path}")
    print(f"{now_str()} [OK] PATH   - Source folder: {gleif_csv_dir}")
    print(f"{now_str()} [OK] PATH   - Output folder: {out_dir}")

    # Extract (business unchanged)
    for file_key, src_path in sources.items():
        pairs = mapping[file_key]

        dst_name = src_path.stem + "__filtered.csv"
        dst_path = out_dir / dst_name

        print(f"{now_str()} [STEP] EXTRACT {file_key}: {src_path.name} -> {dst_path.name}")
        rows = extract_columns(src_path, dst_path, pairs)
        print(f"{now_str()} [OK]   WROTE {rows} rows | columns={len(pairs)}")

    print(f"{now_str()} [DONE] Created 3 filtered CSV files successfully.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        die(str(e))
