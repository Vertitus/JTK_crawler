import hashlib
import mimetypes
import random

def sha256_hash(url: str) -> str:
    """
    Генерирует SHA-256 хэш для URL.
    """
    return hashlib.sha256(url.encode('utf-8')).hexdigest()

def is_valid_mime_type(content_type: str) -> bool:
    """
    Проверяет, является ли MIME-тип допустимым для обработки (например, text/html).
    """
    valid_mime_types = ['text/html']
    mime_type = content_type.split(';')[0].strip()  # Убираем параметры, если они есть
    return mime_type in valid_mime_types

def generate_filename_from_url(url: str) -> str:
    """
    Генерирует имя файла для URL с использованием SHA-256 хэша.
    """
    return sha256_hash(url) + '.html'

def rotate_user_agent(user_agents: list) -> str:
    return random.choice(user_agents) if user_agents else ""
