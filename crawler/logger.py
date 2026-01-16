import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

class ProgressFilter(logging.Filter):
    """
    Фильтр для удаления сообщений о прогрессе из логов.
    """
    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage()
        # Фильтруем строки с 'Progress:' или '[Progress]'
        if 'Progress:' in msg or '[Progress]' in msg:
            return False
        return True


def init_logger(cfg):
    log_path = Path(cfg.path)
    if not log_path.parent.exists():
        log_path.parent.mkdir(parents=True, exist_ok=True)

    # Файл-логирование на INFO и выше
    file_handler = RotatingFileHandler(
        filename=cfg.path,
        maxBytes=cfg.max_bytes,
        backupCount=cfg.backup_count,
        encoding='utf-8'
    )
    file_formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    file_handler.setFormatter(file_formatter)
    file_handler.setLevel(logging.INFO)
    # Применяем фильтр к файловому логеру
    file_handler.addFilter(ProgressFilter())

    # Логирование в консоль на DEBUG и выше
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    console_handler.setFormatter(console_formatter)
    # Применяем фильтр к консольному логеру
    console_handler.addFilter(ProgressFilter())

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)      # корневой логгер на DEBUG
    root.addHandler(file_handler)
    root.addHandler(console_handler)

    root.info("Logger initialized")
