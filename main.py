import warnings
from pathlib import Path
from typing import List, Optional

import pandas as pd
import requests
from langchain_core.documents import Document
from langchain_core.output_parsers import PydanticOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_ollama import OllamaEmbeddings
from langchain_openai import ChatOpenAI
from langchain_qdrant import QdrantVectorStore
from pydantic import BaseModel, Field
from qdrant_client import QdrantClient
from tenacity import retry, stop_after_attempt, wait_fixed
from tqdm import tqdm

from db.db_manager import DB_CONFIG, fetch_candidates, insert_candidates
from utils.utils import build_resume_row, deserialize_cell, extract_html_resume

pd.set_option("display.max_columns", None)
warnings.filterwarnings("ignore")

data_dir = Path("data")

dfs = []

for x in range(1, 26):
    file_path = data_dir / f"data_fake_{x}.csv"
    if file_path.is_file():
        df_tmp = pd.read_csv(file_path)
        df_tmp["data"] = df_tmp["data"].apply(deserialize_cell)
        dfs.append(df_tmp)
    else:
        print(f"Файл {file_path} не найден.")

df = pd.concat(dfs, ignore_index=True)

df["html"] = df["data"].apply(extract_html_resume)
df = df[df["html"].notna()].reset_index(drop=True).copy()
df = df.drop_duplicates(subset=["id"], keep="last")

insert_candidates(df, db_config=DB_CONFIG)

df = fetch_candidates(db_config=DB_CONFIG)
df["html_resume"] = df.apply(build_resume_row, axis=1)


class LanguageSkill(BaseModel):
    name: str = Field(description="Название языка")
    level: Optional[str] = Field(
        description="Уровень владения: 'A1', 'A2', 'B1', 'B2', 'C1', 'C2', 'Native' или null",
        pattern=r"^(A[12]|B[12]|C[12]|Native)?$",
    )


class CandidateMetadata(BaseModel):
    candidate_id: int = Field(description="Уникальный идентификатор кандидата")
    location_name: Optional[str] = Field(description="Локация кандидата")

    positions: List[str] = Field(
        default_factory=list,
        description="Список релевантных должностей, которые могут быть предложены кандидату",
    )
    experience_years: Optional[float] = Field(
        ge=0,
        description="Суммарный релевантный опыт в годах для указанной желаемой должности",
    )
    grade: Optional[str] = Field(
        description="Уровень специалиста: Intern, Junior, Middle, Senior, Lead, Head или null",
        pattern=r"^(Intern|Junior|Middle|Senior|Lead|Head)?$",
    )
    hard_skills: List[str] = Field(
        default_factory=list,
        description="Технические навыки и инструменты",
    )
    domain_skills: List[str] = Field(
        default_factory=list,
        description="Отраслевые и функциональные домены (предметные области)",
    )
    performed_tasks: List[str] = Field(
        default_factory=list,
        description="Фактически выполняемые профессиональные задачи",
    )
    languages: List[LanguageSkill] = Field(
        default_factory=list, description="Языки с нормализованными уровнями владения"
    )
    embedding_text: str = Field(
        description="Очищенный текст для генерации эмбеддинга: без личных данных, с ключевыми навыками и опытом"
    )


output_parser = PydanticOutputParser(pydantic_object=CandidateMetadata)
format_instructions = output_parser.get_format_instructions()

system_prompt = """ 
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

user_prompt = """
Проанализируй следующее резюме и извлеки структурированные данные строго по правилам:

{resume}
"""

prompt = ChatPromptTemplate.from_messages(
    [("system", system_prompt), ("user", user_prompt)]
)

llm = ChatOpenAI(
    base_url="http://localhost:11434/v1",
    api_key="token-abc",
    model="gpt-oss:20b",
    temperature=0.3,
    top_p=0.8,
    max_tokens=1024,
    reasoning_effort="low",
    timeout=120,
)


def fix_json_with_llm(format_instructions: str, broken_json_text: str) -> str:
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
        format_instructions=format_instructions, broken_json_text=broken_json_text
    )
    fixed = llm.invoke(messages)
    return fixed.content


@retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
def call_llm_and_parse(messages):
    result = llm.invoke(messages)
    raw_text = result.content

    try:
        return output_parser.parse(raw_text)
    except Exception as e:
        print("Primary parse failed. Attempting automatic JSON repair...")
        print(f"Original error: {e}")

        fixed_json = fix_json_with_llm(raw_text)

        try:
            return output_parser.parse(fixed_json)
        except Exception as e2:
            print(f"JSON repair also failed: {e2}")
            print(f"Repaired JSON candidate:\n{fixed_json}")
            raise


parsed_results = []

for resume in tqdm(df["html_resume"]):
    messages = prompt.format_messages(
        format_instructions=format_instructions, resume=resume
    )

    try:
        parsed = call_llm_and_parse(messages)
        parsed_results.append(parsed.model_dump())
    except Exception:
        print("Failed even after repair and retries")

        QDRANT_URL = "http://localhost:6333"
COLLECTION_NAME = "candidates"

documents = []

print("Проверяю соединение с Qdrant...")
try:
    health = requests.get(f"{QDRANT_URL}/collections").json()
    if not health.get("status") == "ok":
        print(
            "Предупреждение: Qdrant доступен, но возвращает некорректный статус:",
            health,
        )
except Exception as e:
    raise ConnectionError(
        f"Не удалось подключиться к Qdrant по адресу {QDRANT_URL}: {e}"
    )

print("Начинаю обработку файлов...")

for idx, item in enumerate(tqdm(parsed_results)):
    if not item:
        print(f"[WARN] Пустой объект в позиции {idx}. Пропускаю.")
        continue

    if not isinstance(item, dict):
        print(
            f"[ERROR] Элемент №{idx} должен быть dict, но получен тип {type(item)}. Пропускаю."
        )
        continue

    embedding_text = item.get("embedding_text")

    if embedding_text is None:
        print(f"[ERROR] В объекте №{idx} отсутствует ключ 'embedding_text'. Пропускаю.")
        continue

    if not isinstance(embedding_text, str):
        print(
            f"[ERROR] Значение 'embedding_text' в объекте №{idx} должно быть строкой. Пропускаю."
        )
        continue

    metadata = {k: v for k, v in item.items() if k != "embedding_text"}

    try:
        doc = Document(page_content=embedding_text, metadata=metadata)
        documents.append(doc)
    except Exception as e:
        print(f"[ERROR] Ошибка при создании Document для объекта №{idx}: {e}")
        continue


if not documents:
    raise ValueError("После обработки нет валидных документов для загрузки в Qdrant.")


print(f"Готово к записи: {len(documents)} документов.")

try:
    embeddings = OllamaEmbeddings(
        base_url="http://localhost:11434",
        model="qwen3-embedding:0.6b",
    )
except Exception as e:
    raise RuntimeError(f"Не удалось инициализировать OllamaEmbeddings: {e}")


print("Начинаю запись в Qdrant...")

try:
    qdrant = QdrantVectorStore.from_documents(
        url=QDRANT_URL,
        collection_name=COLLECTION_NAME,
        embedding=embeddings,
        documents=documents,
    )
except Exception as e:
    raise RuntimeError(f"Ошибка при записи данных в Qdrant: {e}")

print("Данные успешно записаны в Qdrant.")


docs_found = qdrant.similarity_search("Юрист", k=3)
for doc in docs_found:
    print(doc.page_content)


retriever = qdrant.as_retriever(search_type="similarity", search_kwargs={"k": 2})

docs = retriever.invoke("ML инженер со знанием AI, LLM")
for doc in docs:
    print(doc.page_content)


QDRANT_URL = "http://localhost:6333"
COLLECTION_NAME = "candidates"

client = QdrantClient(url=QDRANT_URL)

embeddings = OllamaEmbeddings(
    base_url="http://localhost:11434",
    model="qwen3-embedding:0.6b",
)

qdrant = QdrantVectorStore(
    client=client,
    collection_name=COLLECTION_NAME,
    embedding=embeddings,
)

from qdrant_client.http.models import FieldCondition, Filter, Range

query = "ML инженер со знанием AI, LLM. Работа в банковской сфере. Разработка AI-ассистента."
qdrant_filter = Filter(
    must=[
        FieldCondition(key="metadata.experience_years", range=Range(gte=1)),
        # FieldCondition(key="metadata.grade", match={"value": "Senior"}),
    ]
)

docs_with_scores = qdrant.similarity_search_with_relevance_scores(
    query, k=5, filter=qdrant_filter
)


candidates_contexts = []

for doc, score in docs_with_scores:
    candidate_id = doc.metadata["candidate_id"]

    candidate_row = df[df["id"] == candidate_id]
    if candidate_row.empty:
        continue
    candidate_row = candidate_row.iloc[0]

    name = candidate_row["fullname"]
    phone = candidate_row.get("mobile_phone", "не указан")
    location = candidate_row.get("location_name", "не указана")
    page_content = doc.page_content

    candidate_info = (
        f"Имя: {name}\nТелефон: {phone}\nЛокация: {location}\nРезюме:\n{page_content}"
    )
    candidates_contexts.append(candidate_info)

full_context = (
    f"Описание вакансии:\n{query}\n\n"
    f"Рассмотри следующих кандидатов и оцени каждого по следующим критериям:\n"
    f"- Хард-скиллы (технические навыки): оценка от 1 до 10\n"
    f"- Доменные навыки (опыт в отрасли/специализации): оценка от 1 до 10\n"
    f"- Общая релевантность кандидата вакансии: оценка от 1 до 10. Учитывай суммарный опыт, хард скилы, домен\n"
    f"- Объяснение, почему была поставлена такая оценка по общей релевантности\n"
    f"Верни ответ в формате JSON на русском языке: список объектов с полями name, phone, location, hard_skills, domain_skills, relevance.\n\n"
    f"Кандидаты:\n" + "\n\n---\n\n".join(candidates_contexts)
)

llm = ChatOpenAI(
    base_url="http://localhost:11434/v1",
    api_key="token-abc",
    model="gpt-oss:20b",
    temperature=0.2,
    top_p=0.8,
    max_tokens=2048,
    reasoning_effort="low",
)

result = llm.invoke(full_context)

print(result.content)
