"""Сервис для обработки резюме через LLM и создания метаданных."""

import logging
from typing import List

import pandas as pd
from langchain_core.documents import Document
from langchain_core.output_parsers import PydanticOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from tenacity import retry, stop_after_attempt, wait_fixed
from tqdm import tqdm

from config.settings import settings
from models.candidate import CandidateMetadata
from utils.utils import build_resume_row

logger = logging.getLogger(__name__)

# Системный промпт для извлечения метаданных из резюме
SYSTEM_PROMPT = """ 
Ты — эксперт по HR, рекрутингу и информационной экстракции в крупном банке. 
Твоя задача — преобразовать текстовое описание кандидата в строго валидный JSON формата CandidateMetadata.

ОБЩИЕ ТРЕБОВАНИЯ:
- Используй **только факты** из резюме. Не делай выводов, не придумывай данные.
- Ответ должен быть **строго валидным JSON**, полностью соответствующим схеме Pydantic.
- Структура, порядок и названия полей должны совпадать со схемой.
- В **embedding_text нельзя включать личные данные** (имя, телефон, email, ссылки, даты рождения и т.п.).
- Если данных нет — используй пустые строки или пустые массивы, но не придумывай значения.

------------------------------------
ШАГ 1 — АНАЛИЗ РЕЗЮМЕ:
------------------------------------
Изучи текст и выдели:
- ключевые профессиональные навыки (hard skills),
- отраслевые и функциональные домены (domain skills),
- фактически выполняемые профессиональные задачи (performed_tasks),
- используемые инструменты, программные продукты, системы и стандарты,
- уровень сложности задач,
- фактический опыт работы,
- образование и обучение,
- языки и уровень владения.

------------------------------------
ШАГ 2 — ИЗВЛЕЧЕНИЕ ПОЛЕЙ:
------------------------------------

candidate_id:
    - Укажи уникальный идентификатор кандидата из входного текста.

location_name:
    - Укажи локацию кандидата из входного текста, если отсутствует, то строго укажи **null**.
    
positions:
    - Укажи нормализованную желаемую должность и все релевантные позиции, подходящие по профессиональным навыкам, задачам, опыту и используемым инструментам, которые можно предложить кандидату.
    - Не ограничивайся формулировками из резюме: нормализуй и выбирай распространённые роли, которые действительно подходят кандидату.
    - Не используй нишевые, узкоспециализированные или редко встречающиеся должности.
    - Если мало информации, чтобы качественно определить релевантные позиции, то укажи только желаемую должность.
    - Для каждой позиции добавь распространённые общепринятые синонимы и допустимые английские и русские эквиваленты, например:
            "Бэкенд-разработчик (Backend Developer, Backend Engineer)",
            "Фронтенд-разработчик (Frontend Developer, Frontend Engineer)",
            "Python-разработчик (Python Developer)",
            "FullStack-разработчик (Full Stack Developer, Full Stack Engineer)",
            "Тестировщик (QA Engineer, QA Tester, QA Инженер)",
            "ML-инженер (ML Engineer, Machine Learning Engineer, Специалист по машинному обучению)",
            "Аналитик данных (Data Analyst)",
            "Специалист по Data Science (Data Scientist, Дата-сайентист)",
            "Data Engineer (Дата-инженер)",
            "DevOps-инженер (DevOps Engineer)",
            "Инженер по облачным технологиям (Cloud Engineer)",
            "Инженер по искусственному интеллекту (AI Engineer, AI-разработчик)"
    - Список должен быть исчерпывающим, но строго фактическим.
         
experience_years:
    - Суммарный релевантный опыт в годах для указанной желаемой должности.
    - Подсчитывай только суммарный релевантный опыт для указанной желаемой должности.
    - Релевантный опыт — это опыт на должностях и задачах, которые логически соответствуют желаемой должности.
    - Считай строго по фактам из резюме:
        - пример: "1 год 2 месяца" = 1.2 года
        - "8 месяцев" = 0.7 года
        - Итого: 1.2 + 0.7 = 1.9 года
    - Не придумывай опыт, если не указан.
    - Если релевантного опыта ноль или он отсутствует укажи 0.0.

grade:
  - Определи грейд кандидата для указанной желаемой должности по шкале: Intern → Junior → Middle → Senior → Lead → Head.
  - Основывайся на: фактическом опыте, сложности и характере выполняемых задач, степени автономности и зоне ответственности.
  - Не завышай и не занижай уровень — будь объективным.
  - Приоритет имеют: используемые инструменты и технологии, тип и сложность задач — **а не общий стаж или суммарный релевантный опыт в годах**.
  - Если желаемая позиция кандидата не имеет отношения к IT-сфере — строго укажи значение: **null**.

hard_skills:
    - Перечисли измеримые профессиональные навыки (hard skills):  
        - специализированные инструменты, программные продукты, системы, стандарты, методики, языки программирования, регуляторные базы, оборудование и другие конкретные компетенции, подтверждающие квалификацию кандидата.
        - Используй исходные названия, указанные в резюме. 
        - При наличии устойчивого русскоязычного или англоязычного эквивалента — добавь его в список.
        - Не включай общие формулировки (например, "умение работать в команде") — только конкретные, верифицируемые навыки.

domain_skills:
    - Определи отраслевые и функциональные домены (предметные области), в которых кандидат имеет опыт.
    - Для каждой выделенного домена укажи:
        - общепринятое название на русском языке
    - Не выдумывай домены — указывай только те, что прямо или косвенно подтверждаются текстом резюме.

performed_tasks:
    - Определи фактически выполняемые профессиональные задачи (функциональные направления), в которых кандидат имеет опыт.
    - Для каждой задачи укажи:
        - общепринятое название на русском языке
    - Не выдумывай задачи — указывай только те, что прямо или косвенно подтверждаются текстом резюме.

languages:
    - Языки с нормализованными уровнями (A1–C2, Native).

------------------------------------
ШАГ 3 — СТАНДАРТИЗАЦИЯ EMBEDDING_TEXT:
------------------------------------

Создай плотный, информативный текст по шаблону, добавляя релевантные синонимы и английские эквиваленты, необходимые для семантического поиска:

Основная профессиональная роль: <роль>. 
Релевантные должности: <нормализованные должности включая синонимы и допустимые английские эквиваленты>. 
Суммарный релевантный опыт: <X> лет. 
Основные навыки: <hard skills, включая англ. эквиваленты и общие синонимы>. 
Основные домены: <предметные области и отрасли>. 
Работал с задачами: <типовые задачи>. 
Владеет инструментами: <стек, технологии, программы, ПО, оборудование, платформы>. 
Языки: <языки>. 
Краткое описание экспертизы: 
<Напиши 2–3 ёмких предложения, строго на основе фактов из резюме. 
Объедини:
- основную профессиональную роль,
- суммарный релевантный опыт,
- ключевые отраслевые и функциональные домены,
- важнейшие hard skills и инструменты,
- типовые выполняемые задачи,
- при наличии — профильное образование или специализированные курсы.
Запрещено:
- использовать общие фразы («ответственный», «коммуникабельный», «опытный специалист»),
- делать выводы, не подтверждённые текстом,
- копировать фрагменты резюме дословно — переформулируй в обобщающем, но фактологическом стиле.
Цель: создать плотную, нейтральную, семантически насыщенную аннотацию для точного матчинга с вакансиями>.

Требования к embedding_text:
- Он должен быть семантически богатым, чтобы эффективно совпадать с вакансиями, описанными в свободной форме.
- Используй распространённые синонимы задач, ролей, доменов и технологий.
- Не выходи за рамки фактов.
- Не добавляй личные данные.
- Строго соблюдай структуру.

------------------------------------
ШАГ 4 — САМОПРОВЕРКА:
------------------------------------
Перед тем как вывести ответ, проверь:

✓ JSON полностью валиден  
✓ Все поля присутствуют  
✓ Нет персональных данных  
✓ positions соответствуют кандидату
✓ positions нормализованы и содержат синонимы на русском и английском языках 
✓ experience_years рассчитан корректно  
✓ grade соответствует уровню по опыту и выполняемым задачам 
✓ embedding_text строго по шаблону  
✓ Никакого лишнего текста вне JSON  

{format_instructions}
"""

USER_PROMPT = """
Проанализируй следующее резюме и извлеки структурированные данные строго по правилам:

{resume}
"""


class ResumeProcessor:
    """Класс для обработки резюме через LLM."""

    def __init__(self):
        """Инициализация процессора резюме."""
        self.llm = ChatOpenAI(
            base_url=f"{settings.ollama_base_url}/v1",
            api_key=settings.ollama_api_key,
            model=settings.ollama_llm_model,
            temperature=0.3,
            top_p=0.8,
            max_tokens=1024,
            reasoning_effort="low",
            timeout=120,
        )
        self.output_parser = PydanticOutputParser(pydantic_object=CandidateMetadata)
        self.format_instructions = self.output_parser.get_format_instructions()
        self.prompt = ChatPromptTemplate.from_messages(
            [("system", SYSTEM_PROMPT), ("user", USER_PROMPT)]
        )

    def _fix_json_with_llm(self, broken_json_text: str) -> str:
        """Исправляет некорректный JSON с помощью LLM."""
        system_fix_prompt = """ 
        Ты AI помощник, который исправляет JSON.
        Верни **валидный** JSON, строго соответствующий этой Pydantic-схеме.
        {format_instructions}
        """
        user_fix_prompt = """ 
        Исправь этот текст так, чтобы результат был корректным JSON:
        {broken_json_text}
        """
        prompt = ChatPromptTemplate.from_messages(
            [("system", system_fix_prompt), ("user", user_fix_prompt)]
        )

        messages = prompt.format_messages(
            format_instructions=self.format_instructions,
            broken_json_text=broken_json_text,
        )
        fixed = self.llm.invoke(messages)
        return fixed.content

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
    def _call_llm_and_parse(self, messages) -> CandidateMetadata:
        """Вызывает LLM и парсит результат в CandidateMetadata."""
        result = self.llm.invoke(messages)
        raw_text = result.content

        try:
            return self.output_parser.parse(raw_text)
        except Exception as e:
            logger.warning(
                f"Первичный парсинг не удался. Пытаюсь исправить JSON... Ошибка: {e}"
            )

            fixed_json = self._fix_json_with_llm(raw_text)

            try:
                return self.output_parser.parse(fixed_json)
            except Exception as e2:
                logger.error(f"Исправление JSON также не удалось: {e2}")
                logger.error(f"Исправленный JSON кандидат:\n{fixed_json}")
                raise

    def process_resume(self, resume_text: str) -> CandidateMetadata:
        """Обрабатывает одно резюме и возвращает метаданные."""
        messages = self.prompt.format_messages(
            format_instructions=self.format_instructions, resume=resume_text
        )
        return self._call_llm_and_parse(messages)

    def process_resumes_batch(self, df: pd.DataFrame) -> List[dict]:
        """
        Обрабатывает батч резюме из DataFrame.
        Ожидает, что в df есть колонка 'html_resume'.
        """
        logger.info(f"Начинаю обработку {len(df)} резюме через LLM...")

        parsed_results = []
        df["html_resume"] = df.apply(build_resume_row, axis=1)

        for resume in tqdm(df["html_resume"], desc="Обработка резюме"):
            try:
                parsed = self.process_resume(resume)
                parsed_results.append(parsed.model_dump())
            except Exception as e:
                logger.error(f"Ошибка при обработке резюме: {e}", exc_info=True)
                logger.warning("Пропускаю это резюме и продолжаю обработку...")

        logger.info(f"Успешно обработано {len(parsed_results)} из {len(df)} резюме")
        return parsed_results

    def convert_to_documents(self, parsed_results: List[dict]) -> List[Document]:
        """
        Конвертирует результаты обработки в список Document для Qdrant.
        """
        documents = []

        for idx, item in enumerate(parsed_results):
            if not item:
                logger.warning(f"Пустой объект в позиции {idx}. Пропускаю.")
                continue

            if not isinstance(item, dict):
                logger.error(
                    f"Элемент №{idx} должен быть dict, но получен тип {type(item)}. Пропускаю."
                )
                continue

            embedding_text = item.get("embedding_text")

            if embedding_text is None:
                logger.error(
                    f"В объекте №{idx} отсутствует ключ 'embedding_text'. Пропускаю."
                )
                continue

            if not isinstance(embedding_text, str):
                logger.error(
                    f"Значение 'embedding_text' в объекте №{idx} должно быть строкой. Пропускаю."
                )
                continue

            metadata = {k: v for k, v in item.items() if k != "embedding_text"}

            try:
                doc = Document(page_content=embedding_text, metadata=metadata)
                documents.append(doc)
            except Exception as e:
                logger.error(f"Ошибка при создании Document для объекта №{idx}: {e}")
                continue

        if not documents:
            raise ValueError(
                "После обработки нет валидных документов для загрузки в Qdrant."
            )

        logger.info(f"Создано {len(documents)} документов для загрузки в Qdrant")
        return documents
