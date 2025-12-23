"""Сервис для поиска и оценки кандидатов."""

import json
import logging
from typing import List

import pandas as pd
from langchain_core.documents import Document
from langchain_openai import ChatOpenAI

from config.settings import settings
from models.api import CandidateEvaluation
from services.data_loader import get_candidates_from_db
from services.vector_store import VectorStoreService

logger = logging.getLogger(__name__)


class CandidateSearchService:
    """Сервис для поиска и оценки кандидатов по вакансии."""

    def __init__(self):
        """Инициализация сервиса поиска кандидатов."""
        self.vector_store_service = VectorStoreService()
        self.llm = ChatOpenAI(
            base_url=f"{settings.ollama_base_url}/v1",
            api_key=settings.ollama_api_key,
            model=settings.ollama_llm_model,
            temperature=0.2,
            top_p=0.8,
            max_tokens=2048,
            reasoning_effort="low",
        )

    def _build_candidate_context(self, doc: Document, candidate_row: pd.Series) -> str:
        """Строит контекст для одного кандидата."""
        name = candidate_row["fullname"]
        phone = candidate_row.get("mobile_phone", "не указан")
        location = candidate_row.get("location_name", "не указана")
        page_content = doc.page_content

        candidate_info = f"Имя: {name}\nТелефон: {phone}\nЛокация: {location}\nРезюме:\n{page_content}"
        return candidate_info

    def _build_evaluation_prompt(
        self, vacancy_description: str, candidates_contexts: List[str]
    ) -> str:
        """Строит промпт для оценки кандидатов."""
        full_context = (
            f"Описание вакансии:\n{vacancy_description}\n\n"
            f"Рассмотри следующих кандидатов и оцени каждого по следующим критериям:\n"
            f"- Хард-скиллы (технические навыки): оценка от 1 до 10\n"
            f"- Доменные навыки (опыт в отрасли/специализации): оценка от 1 до 10\n"
            f"- Общая релевантность кандидата вакансии: оценка от 1 до 10. Учитывай суммарный опыт, хард скилы, домен\n"
            f"- Объяснение, почему была поставлена такая оценка по общей релевантности\n"
            f"Верни ответ в формате JSON на русском языке: список объектов с полями name, phone, location, hard_skills_score, domain_skills_score, relevance_score, relevance_explanation.\n\n"
            f"Кандидаты:\n" + "\n\n---\n\n".join(candidates_contexts)
        )
        return full_context

    def _parse_llm_response(self, response_content: str) -> List[CandidateEvaluation]:
        """Парсит ответ LLM в список CandidateEvaluation."""
        try:
            # Попытка извлечь JSON из ответа
            content = response_content.strip()

            # Если ответ содержит markdown блоки кода, извлекаем JSON
            if "```json" in content:
                start = content.find("```json") + 7
                end = content.find("```", start)
                content = content[start:end].strip()
            elif "```" in content:
                start = content.find("```") + 3
                end = content.find("```", start)
                content = content[start:end].strip()

            # Парсинг JSON
            data = json.loads(content)

            # Если это список
            if isinstance(data, list):
                evaluations = [
                    CandidateEvaluation(**item) if isinstance(item, dict) else item
                    for item in data
                ]
            # Если это словарь с ключом candidates или другим
            elif isinstance(data, dict):
                if "candidates" in data:
                    evaluations = [
                        CandidateEvaluation(**item) for item in data["candidates"]
                    ]
                else:
                    # Пытаемся найти список в значениях
                    for value in data.values():
                        if isinstance(value, list):
                            evaluations = [
                                CandidateEvaluation(**item) for item in value
                            ]
                            break
                    else:
                        raise ValueError("Не найден список кандидатов в ответе")
            else:
                raise ValueError(f"Неожиданный формат ответа: {type(data)}")

            return evaluations

        except Exception as e:
            logger.error(f"Ошибка при парсинге ответа LLM: {e}")
            logger.error(f"Содержимое ответа: {response_content}")
            raise ValueError(f"Не удалось распарсить ответ LLM: {e}")

    def search_candidates(
        self,
        vacancy_description: str,
        k: int = 5,
        experience_years_min: float = None,
        grade: str = None,
    ) -> List[CandidateEvaluation]:
        """
        Ищет и оценивает кандидатов по описанию вакансии.

        Args:
            vacancy_description: Описание вакансии
            k: Количество кандидатов для возврата
            experience_years_min: Минимальный опыт в годах
            grade: Требуемый грейд

        Returns:
            Список оцененных кандидатов
        """
        logger.info(f"Начинаю поиск кандидатов по вакансии. k={k}")
        logger.info("Выполняю поиск в векторной базе данных...")

        # Поиск в векторной БД
        docs_with_scores = self.vector_store_service.search_with_filter(
            query=vacancy_description,
            k=k,
            experience_years_min=experience_years_min,
            grade=grade,
        )

        logger.info(f"Найдено {len(docs_with_scores)} документов в векторной БД")

        if not docs_with_scores:
            logger.warning("Не найдено кандидатов по запросу")
            return []

        # Загрузка полных данных кандидатов из PostgreSQL
        logger.info("Загружаю полные данные кандидатов из PostgreSQL...")
        df = get_candidates_from_db()
        logger.info(f"Загружено {len(df)} кандидатов из PostgreSQL")

        candidates_contexts = []
        processed_count = 0

        logger.info("Обрабатываю найденных кандидатов...")
        for idx, (doc, score) in enumerate(docs_with_scores, 1):
            candidate_id = doc.metadata.get("candidate_id")

            if candidate_id is None:
                logger.warning(
                    f"[{idx}/{len(docs_with_scores)}] В метаданных документа отсутствует candidate_id, пропускаю"
                )
                continue

            candidate_row = df[df["id"] == candidate_id]
            if candidate_row.empty:
                logger.warning(
                    f"[{idx}/{len(docs_with_scores)}] Кандидат с id={candidate_id} не найден в БД, пропускаю"
                )
                continue

            candidate_row = candidate_row.iloc[0]
            candidate_context = self._build_candidate_context(doc, candidate_row)
            candidates_contexts.append(candidate_context)
            processed_count += 1
            logger.info(
                f"[{idx}/{len(docs_with_scores)}] Обработан кандидат: {candidate_row['fullname']} (релевантность: {score:.3f})"
            )

        logger.info(
            f"Успешно обработано {processed_count} из {len(docs_with_scores)} кандидатов"
        )

        if not candidates_contexts:
            logger.warning("Не удалось получить контексты кандидатов")
            return []

        # Оценка кандидатов через LLM
        logger.info(
            f"Отправляю запрос к LLM для оценки {len(candidates_contexts)} кандидатов..."
        )
        evaluation_prompt = self._build_evaluation_prompt(
            vacancy_description, candidates_contexts
        )

        try:
            logger.info("Ожидаю ответ от LLM...")
            result = self.llm.invoke(evaluation_prompt)
            logger.info("Получен ответ от LLM, начинаю парсинг...")
            evaluations = self._parse_llm_response(result.content)
            logger.info(f"Успешно оценено {len(evaluations)} кандидатов")
            return evaluations

        except Exception as e:
            logger.error(f"Ошибка при оценке кандидатов через LLM: {e}", exc_info=True)
            raise
