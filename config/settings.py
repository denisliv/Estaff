"""Конфигурация приложения."""

try:
    from pydantic_settings import BaseSettings
except ImportError:
    # Fallback для старых версий pydantic
    from pydantic import BaseSettings


class Settings(BaseSettings):
    """Настройки приложения."""

    # PostgreSQL
    db_host: str = "localhost"
    db_port: int = 5432
    db_name: str = "estaff"
    db_user: str = "postgres"
    db_password: str = "postgres"

    # Qdrant
    qdrant_url: str = "http://localhost:6333"
    qdrant_collection_name: str = "candidates"

    # LLM (Ollama)
    ollama_base_url: str = "http://localhost:11434"
    ollama_embedding_model: str = "qwen3-embedding:0.6b"
    ollama_llm_model: str = "gpt-oss:20b"
    ollama_api_key: str = "token-abc"

    # Данные
    data_dir: str = "data"
    data_file_pattern: str = "data_fake_{}.csv"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()
