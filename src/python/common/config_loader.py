import configparser
from pathlib import Path

def resolve_project_root() -> Path:
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "config").exists():
            return parent
    raise RuntimeError("Project root not found: missing 'config' directory")

def load_config() -> configparser.ConfigParser:
    root = resolve_project_root()
    cfg_dir = root / "config"
    local_cfg = cfg_dir / "config.ini"
    template_cfg = cfg_dir / "config.template.ini"

    parser = configparser.ConfigParser()
    if local_cfg.exists():
        parser.read(local_cfg, encoding="utf-8")
        print(f"[CONFIG] using local config: {local_cfg}")
    elif template_cfg.exists():
        parser.read(template_cfg, encoding="utf-8")
        print(f"[CONFIG] using template config: {template_cfg}")
    else:
        raise FileNotFoundError("No config found: config/config.ini or config/config.template.ini")

    return parser
