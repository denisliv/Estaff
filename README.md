# Estaff HR Service

Сервис для поиска и оценки кандидатов по вакансиям на основе RAG (Retrieval-Augmented Generation) с использованием LangChain и Qdrant.

## Описание

Сервис предназначен для HR-специалистов и позволяет:
- Искать наиболее релевантных кандидатов по описанию вакансии
- Оценивать кандидатов по различным критериям (hard skills, domain skills, общая релевантность)
- Обновлять базу данных кандидатов из CSV файлов
- Создавать и обновлять векторные представления резюме для семантического поиска

## Архитектура

Проект структурирован следующим образом:

```
.
├── api/                    # FastAPI роуты
│   ├── __init__.py
│   └── routes.py          # API эндпоинты
├── config/                 # Конфигурация
│   ├── __init__.py
│   └── settings.py        # Настройки приложения
├── db/                     # Работа с БД
│   └── db_manager.py      # Управление PostgreSQL
├── models/                 # Pydantic модели
│   ├── __init__.py
│   ├── candidate.py       # Модели кандидатов
│   └── api.py             # Модели для API
├── services/               # Бизнес-логика
│   ├── __init__.py
│   ├── data_loader.py     # Загрузка данных в PostgreSQL
│   ├── resume_processor.py # Обработка резюме через LLM
│   ├── vector_store.py    # Работа с Qdrant
│   └── candidate_search.py # Поиск и оценка кандидатов
├── scripts/                # Скрипты для обновления данных
│   ├── update_database.py      # Обновление PostgreSQL
│   └── update_vector_db.py     # Обновление Qdrant
├── utils/                  # Утилиты
│   └── utils.py           # Вспомогательные функции
├── migrations/             # Миграции БД
│   └── migrations.py
├── app.py                  # Главное приложение FastAPI
└── requirements.txt        # Зависимости
```

## Установка

1. Установите зависимости:
```bash
pip install -r requirements.txt
```

2. Создайте файл `.env` в корне проекта (опционально):
```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=estaff
DB_USER=postgres
DB_PASSWORD=postgres

QDRANT_URL=http://localhost:6333
QDRANT_COLLECTION_NAME=candidates

OLLAMA_BASE_URL=http://localhost:11434
OLLAMA_EMBEDDING_MODEL=qwen3-embedding:0.6b
OLLAMA_LLM_MODEL=gpt-oss:20b
OLLAMA_API_KEY=token-abc
```

3. Убедитесь, что запущены:
   - PostgreSQL
   - Qdrant
   - Ollama (с нужными моделями)

4. Создайте таблицы в PostgreSQL:
```bash
python migrations/migrations.py
```

## Запуск

### Запуск API сервера

```bash
uvicorn app:app --reload --host 0.0.0.0 --port 8000
```

API будет доступно по адресу: http://localhost:8000

Документация API: http://localhost:8000/docs

### Обновление базы данных

#### Обновление PostgreSQL из CSV файлов:

```bash
python scripts/update_database.py
```

#### Обновление векторной БД Qdrant:

```bash
python scripts/update_vector_db.py
```

Рекомендуется настроить ежедневное выполнение этих скриптов через cron или планировщик задач.

## API Эндпоинты

### POST /api/v1/search

Поиск и оценка кандидатов по описанию вакансии.

**Запрос:**
```json
{
  "description": "ML инженер со знанием AI, LLM. Работа в банковской сфере. Разработка AI-ассистента.",
  "experience_years_min": 1.0,
  "grade": "Senior",
  "k": 5
}
```

**Ответ:**
```json
{
  "candidates": [
    {
      "name": "Иван Иванов",
      "phone": "+7 (999) 123-45-67",
      "location": "Москва",
      "hard_skills_score": 8,
      "domain_skills_score": 7,
      "relevance_score": 8,
      "relevance_explanation": "Кандидат имеет опыт работы с ML и AI..."
    }
  ],
  "total_found": 5
}
```

### POST /api/v1/update-database

Запускает обновление базы данных PostgreSQL из CSV файлов (выполняется в фоне).

### POST /api/v1/update-vector-db

Запускает обновление векторной базы данных Qdrant (выполняется в фоне).

### GET /api/v1/health

Проверка здоровья сервиса.

## Основные модули

### data_loader.py

Сервис для загрузки данных из CSV файлов в PostgreSQL:
- `load_data_from_csv()` - загрузка данных из CSV
- `preprocess_data()` - предобработка данных
- `update_postgres_database()` - обновление БД

### resume_processor.py

Сервис для обработки резюме через LLM:
- `ResumeProcessor` - класс для обработки резюме
- `process_resumes_batch()` - обработка батча резюме
- `convert_to_documents()` - конвертация в документы для Qdrant

### vector_store.py

Сервис для работы с Qdrant:
- `VectorStoreService` - класс для работы с векторной БД
- `create_or_update_collection()` - создание/обновление коллекции
- `search_with_filter()` - поиск с фильтрами

### candidate_search.py

Сервис для поиска и оценки кандидатов:
- `CandidateSearchService` - класс для поиска кандидатов
- `search_candidates()` - основной метод поиска

## Конфигурация

Все настройки находятся в `config/settings.py` и могут быть переопределены через переменные окружения или файл `.env`.

## Разработка

Старый код из `main.py` сохранен для справки. Вся новая функциональность реализована через модульную архитектуру с четким разделением ответственности.

