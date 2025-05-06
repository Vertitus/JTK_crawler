# crawler/logger.py
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

def init_logger(cfg):
    log_path = Path(cfg.path)
    if not log_path.parent.exists():
        log_path.parent.mkdir(parents=True, exist_ok=True)

    handler = RotatingFileHandler(
        filename=cfg.path,
        maxBytes=cfg.max_bytes,
        backupCount=cfg.backup_count,
        encoding='utf-8'
    )
    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.addHandler(handler)
    # также лог в stdout (можно отключить)
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    # console.setFormatter(formatter)
    root.addHandler(console)

    logging.info("Logger initialized")
