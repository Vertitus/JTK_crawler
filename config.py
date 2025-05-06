# config.py (с валидацией)
import yaml
from dataclasses import dataclass
from typing import Any
from typing import List, Tuple
from pathlib import Path

@dataclass
class CDXConfig:
    request_timeout: int
    max_pages: int
    backoff_factor: float
    target_domains_file: str

@dataclass
class LogConfig:
    path: str
    max_bytes: int
    backup_count: int

@dataclass
class FetchConfig:
    user_agents_file: str
    rate_limit: float

@dataclass
class StorageConfig:
    bloom_capacity: int
    bloom_error_rate: float
    cache_ttl_days: int
    cache_dir: str 

@dataclass
class ParserConfig:
    patterns_file: str     # Было: patterns_file
    url_filters: str       # Новое поле
    case_sensitive: bool

@dataclass
class SchedulerConfig:
    seeds: List[str]
    poison_pill: str
    max_concurrent: int
    max_depth: int
    queue_size: int



@dataclass
class Config:
    max_concurrent: int
    max_retries: int
    max_depth: int
    queue_size: int
    auto_save_interval: int
    batch_size: int
    cache_dir: str
    log: LogConfig
    fetch: FetchConfig
    storage: StorageConfig
    parser: ParserConfig
    scheduler: SchedulerConfig
    cdx: CDXConfig

def validate_positive(value, name):
    if value <= 0:
        raise ValueError(f"'{name}' must be positive, got {value}")

def validate_config(raw: dict):
    validate_positive(raw['max_concurrent'], 'max_concurrent')
    validate_positive(raw['max_retries'], 'max_retries')
    validate_positive(raw['max_depth'], 'max_depth')
    validate_positive(raw['queue_size'], 'queue_size')
    validate_positive(raw['auto_save_interval'], 'auto_save_interval')
    validate_positive(raw['batch_size'], 'batch_size')
    validate_positive(raw['fetch']['rate_limit'], 'fetch.rate_limit')
    validate_positive(raw['storage']['bloom_capacity'], 'storage.bloom_capacity')
    if not (0 < raw['storage']['bloom_error_rate'] < 1):
        raise ValueError("storage.bloom_error_rate must be between 0 and 1")

def load_config(path: str = 'config.yaml') -> Config:
    with open(path, 'r') as f:
        raw = yaml.safe_load(f)
    validate_config(raw)

    return Config(
        cdx=CDXConfig(**raw['cdx']),  # Добавьте эту строку
        max_concurrent=raw['max_concurrent'],
        max_retries=raw['max_retries'],
        max_depth=raw['max_depth'],
        queue_size=raw['queue_size'],
        auto_save_interval=raw['auto_save_interval'],
        batch_size=raw['batch_size'],
        cache_dir=raw['cache_dir'],
        log=LogConfig(**raw['log']),
        fetch=FetchConfig(**raw['fetch']),
        storage=StorageConfig(**raw['storage']),
        parser=ParserConfig(**raw['parser']),
        scheduler=SchedulerConfig(**raw['scheduler'])
    )
