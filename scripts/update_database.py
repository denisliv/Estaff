"""Скрипт для ежедневного обновления базы данных PostgreSQL."""

import logging
import sys
from pathlib import Path

# Добавляем корневую директорию проекта в путь
sys.path.insert(0, str(Path(__file__).parent.parent))

from services.data_loader import update_postgres_database

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    """Основная функция для обновления базы данных."""
    try:
        logger.info("Начинаю обновление базы данных PostgreSQL...")
        count = update_postgres_database()
        logger.info(f"Обновление завершено успешно. Обработано {count} записей.")
        return 0
    except Exception as e:
        logger.error(f"Ошибка при обновлении базы данных: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
