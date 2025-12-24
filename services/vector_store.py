"""Сервис для работы с векторной БД Qdrant."""

import logging
from typing import List, Optional

import requests
from langchain_core.documents import Document
from langchain_ollama import OllamaEmbeddings
from langchain_qdrant import QdrantVectorStore
from qdrant_client import QdrantClient
from qdrant_client.http.models import FieldCondition, Filter, Range

from config.settings import settings

logger = logging.getLogger(__name__)


class VectorStoreService:
    """Сервис для работы с Qdrant векторной БД."""

    def __init__(self):
        """Инициализация сервиса векторной БД."""
        self.qdrant_url = settings.qdrant_url
        self.collection_name = settings.qdrant_collection_name
        self.embeddings = OllamaEmbeddings(
            base_url=settings.ollama_base_url,
            model=settings.ollama_embedding_model,
        )

    def check_connection(self) -> bool:
        """Проверяет соединение с Qdrant."""
        try:
            health = requests.get(f"{self.qdrant_url}/collections").json()
            if health.get("status") == "ok":
                logger.info("Соединение с Qdrant успешно установлено")
                return True
            else:
                logger.warning(
                    f"Qdrant доступен, но возвращает некорректный статус: {health}"
                )
                return False
        except Exception as e:
            logger.error(
                f"Не удалось подключиться к Qdrant по адресу {self.qdrant_url}: {e}"
            )
            raise ConnectionError(f"Не удалось подключиться к Qdrant: {e}")

    def get_collection_info(self) -> dict:
        """
        Получает информацию о коллекции, включая количество точек.
        
        Returns:
            dict: Словарь с информацией о коллекции (points_count, collection_name, exists)
        """
        try:
            client = QdrantClient(url=self.qdrant_url)
            
            # Пытаемся получить информацию о коллекции
            # Если коллекция не существует, get_collection выбросит исключение
            try:
                collection_info = client.get_collection(self.collection_name)
                return {
                    "points_count": collection_info.points_count,
                    "collection_name": self.collection_name,
                    "exists": True,
                }
            except Exception as collection_error:
                # Проверяем, является ли это ошибкой "коллекция не найдена"
                error_str = str(collection_error).lower()
                if "not found" in error_str or "does not exist" in error_str:
                    logger.warning(f"Коллекция {self.collection_name} не найдена")
                    return {
                        "points_count": 0,
                        "collection_name": self.collection_name,
                        "exists": False,
                    }
                else:
                    # Если это другая ошибка, пробрасываем её дальше
                    raise
                    
        except Exception as e:
            logger.error(f"Ошибка при получении информации о коллекции: {e}", exc_info=True)
            raise RuntimeError(f"Не удалось получить информацию о коллекции: {e}")

    def create_or_update_collection(
        self, documents: List[Document]
    ) -> QdrantVectorStore:
        """
        Создает или обновляет коллекцию в Qdrant с документами.
        """
        if not documents:
            raise ValueError("Список документов пуст")

        logger.info(f"Начинаю запись {len(documents)} документов в Qdrant...")

        try:
            self.check_connection()

            qdrant = QdrantVectorStore.from_documents(
                url=self.qdrant_url,
                collection_name=self.collection_name,
                embedding=self.embeddings,
                documents=documents,
            )

            logger.info(f"Успешно записано {len(documents)} документов в Qdrant")
            return qdrant

        except Exception as e:
            logger.error(f"Ошибка при записи данных в Qdrant: {e}", exc_info=True)
            raise RuntimeError(f"Ошибка при записи данных в Qdrant: {e}")

    def get_vector_store(self) -> QdrantVectorStore:
        """Получает экземпляр QdrantVectorStore для существующей коллекции."""
        self.check_connection()

        client = QdrantClient(url=self.qdrant_url)
        qdrant = QdrantVectorStore(
            client=client,
            collection_name=self.collection_name,
            embedding=self.embeddings,
        )

        return qdrant

    def search_with_filter(
        self,
        query: str,
        k: int = 5,
        experience_years_min: Optional[float] = None,
        grade: Optional[str] = None,
    ) -> List[tuple[Document, float]]:
        """
        Выполняет семантический поиск с фильтрами по метаданным.

        Args:
            query: Текстовый запрос для поиска
            k: Количество возвращаемых результатов
            experience_years_min: Минимальный опыт в годах
            grade: Требуемый грейд

        Returns:
            Список кортежей (Document, score)
        """
        qdrant = self.get_vector_store()

        # Построение фильтра
        filter_conditions = []

        if experience_years_min is not None:
            filter_conditions.append(
                FieldCondition(
                    key="metadata.experience_years",
                    range=Range(gte=experience_years_min),
                )
            )

        if grade is not None:
            filter_conditions.append(
                FieldCondition(key="metadata.grade", match={"value": grade})
            )

        qdrant_filter = None
        if filter_conditions:
            qdrant_filter = Filter(must=filter_conditions)

        try:
            logger.info(
                f"Выполняю поиск в Qdrant с параметрами: k={k}, experience_years_min={experience_years_min}, grade={grade}"
            )

            if qdrant_filter:
                logger.info("Применяю фильтры к поиску")
                docs_with_scores = qdrant.similarity_search_with_relevance_scores(
                    query, k=k, filter=qdrant_filter
                )
            else:
                logger.info("Поиск без фильтров")
                docs_with_scores = qdrant.similarity_search_with_relevance_scores(
                    query, k=k
                )

            logger.info(f"Найдено {len(docs_with_scores)} документов для запроса")
            return docs_with_scores

        except Exception as e:
            logger.error(f"Ошибка при поиске в Qdrant: {e}", exc_info=True)
            raise
