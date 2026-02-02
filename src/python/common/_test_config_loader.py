from config_loader import load_config
cfg = load_config()
print(cfg.sections())
print(cfg["SQLSERVER"]["server"])
