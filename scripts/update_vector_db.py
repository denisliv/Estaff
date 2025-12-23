"""Скрипт для ежедневного обновления векторной базы данных Qdrant."""

import logging
import sys
from pathlib import Path

# Добавляем корневую директорию проекта в путь
sys.path.insert(0, str(Path(__file__).parent.parent))

from services.data_loader import get_candidates_from_db
from services.resume_processor import ResumeProcessor
from services.vector_store import VectorStoreService

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    """Основная функция для обновления векторной БД."""
    try:
        logger.info("Начинаю обновление векторной базы данных Qdrant...")

        # 1. Загружаем кандидатов из PostgreSQL
        logger.info("Загрузка кандидатов из PostgreSQL...")
        df = get_candidates_from_db()
        logger.info(f"Загружено {len(df)} кандидатов из PostgreSQL")

        # 2. Обрабатываем резюме через LLM
        logger.info("Обработка резюме через LLM...")
        resume_processor = ResumeProcessor()
        parsed_results = resume_processor.process_resumes_batch(df)

        # 3. Конвертируем в документы
        logger.info("Конвертация в документы для Qdrant...")
        documents = resume_processor.convert_to_documents(parsed_results)

        # 4. Загружаем в Qdrant
        logger.info("Загрузка документов в Qdrant...")
        vector_store_service = VectorStoreService()
        vector_store_service.create_or_update_collection(documents)

        logger.info("Векторная БД успешно обновлена")
        return 0

    except Exception as e:
        logger.error(f"Ошибка при обновлении векторной БД: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
