"""API роуты для FastAPI приложения."""

import logging
import queue

import psycopg2
from fastapi import (
    APIRouter,
    BackgroundTasks,
    HTTPException,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.responses import HTMLResponse, JSONResponse

from models.api import VacancySearchRequest, VacancySearchResponse
from services.candidate_search import CandidateSearchService
from services.data_loader import (
    get_candidates_from_db,
    get_db_config,
    update_postgres_database,
)
from services.resume_processor import ResumeProcessor
from services.vector_store import VectorStoreService

logger = logging.getLogger(__name__)

# Очередь для логов (для WebSocket)
log_queue: queue.Queue = queue.Queue()

router = APIRouter()

# Инициализация сервисов
candidate_search_service = CandidateSearchService()
resume_processor = ResumeProcessor()
vector_store_service = VectorStoreService()


@router.post("/search", response_model=VacancySearchResponse)
async def search_candidates(request: VacancySearchRequest):
    """
    Поиск и оценка кандидатов по описанию вакансии.

    Принимает описание вакансии и параметры поиска, возвращает список
    наиболее релевантных кандидатов с оценками по различным критериям.
    """
    try:
        logger.info(
            f"Получен запрос на поиск кандидатов: {request.description[:100]}..."
        )

        candidates = candidate_search_service.search_candidates(
            vacancy_description=request.description,
            k=request.k,
            experience_years_min=request.experience_years_min,
            grade=request.grade,
        )

        return VacancySearchResponse(
            candidates=candidates,
            total_found=len(candidates),
        )

    except Exception as e:
        logger.error(f"Ошибка при поиске кандидатов: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"Ошибка при поиске кандидатов: {str(e)}"
        )


@router.post("/update-database")
async def update_database(background_tasks: BackgroundTasks):
    """
    Запускает обновление базы данных PostgreSQL из CSV файлов.

    Операция выполняется в фоновом режиме, так как может занять продолжительное время.
    """
    try:
        logger.info("Запуск обновления базы данных...")

        # Выполняем в фоне
        background_tasks.add_task(update_postgres_database)

        return JSONResponse(
            status_code=202,
            content={
                "message": "Обновление базы данных запущено в фоновом режиме",
                "status": "processing",
            },
        )

    except Exception as e:
        logger.error(f"Ошибка при запуске обновления БД: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"Ошибка при запуске обновления БД: {str(e)}"
        )


@router.post("/update-vector-db")
async def update_vector_database(background_tasks: BackgroundTasks):
    """
    Запускает обновление векторной базы данных Qdrant.

    Процесс включает:
    1. Загрузку кандидатов из PostgreSQL
    2. Обработку резюме через LLM для создания метаданных
    3. Загрузку в векторную БД Qdrant

    Операция выполняется в фоновом режиме, так как может занять продолжительное время.
    """
    try:
        logger.info("Запуск обновления векторной базы данных...")

        def update_vector_db_task():
            """Задача для фонового выполнения."""
            try:
                # 1. Загружаем кандидатов из БД
                df = get_candidates_from_db()
                logger.info(f"Загружено {len(df)} кандидатов из PostgreSQL")

                # 2. Обрабатываем резюме через LLM
                parsed_results = resume_processor.process_resumes_batch(df)

                # 3. Конвертируем в документы
                documents = resume_processor.convert_to_documents(parsed_results)

                # 4. Загружаем в Qdrant
                vector_store_service.create_or_update_collection(documents)

                logger.info("Векторная БД успешно обновлена")

            except Exception as e:
                logger.error(f"Ошибка при обновлении векторной БД: {e}", exc_info=True)
                raise

        background_tasks.add_task(update_vector_db_task)

        return JSONResponse(
            status_code=202,
            content={
                "message": "Обновление векторной базы данных запущено в фоновом режиме",
                "status": "processing",
            },
        )

    except Exception as e:
        logger.error(f"Ошибка при запуске обновления векторной БД: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Ошибка при запуске обновления векторной БД: {str(e)}",
        )


@router.get("/candidate/resume")
async def get_candidate_resume(name: str, phone: str):
    """
    Получает HTML резюме кандидата по имени и телефону.

    Args:
        name: Имя кандидата
        phone: Телефон кандидата
    """
    try:
        db_config = get_db_config()

        # Используем параметризованные запросы для безопасности
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT html FROM candidates WHERE fullname = %s AND mobile_phone = %s LIMIT 1",
                    (name, phone),
                )
                result = cur.fetchone()

                if result is None or result[0] is None:
                    raise HTTPException(status_code=404, detail="Резюме не найдено")

                html_resume = result[0]
                return HTMLResponse(content=html_resume)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Ошибка при получении резюме: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"Ошибка при получении резюме: {str(e)}"
        )


@router.websocket("/ws/logs")
async def websocket_logs(websocket: WebSocket):
    """WebSocket эндпоинт для получения логов в реальном времени."""
    await websocket.accept()
    try:
        while True:
            # Отправляем логи из очереди
            try:
                log_message = log_queue.get_nowait()
                await websocket.send_json(log_message)
            except queue.Empty:
                # Если очередь пуста, ждем немного
                import asyncio

                await asyncio.sleep(0.1)
    except WebSocketDisconnect:
        logger.info("WebSocket клиент отключился")


@router.get("/health")
async def health_check():
    """Проверка здоровья сервиса."""
    try:
        # Проверяем соединение с Qdrant
        vector_store_service.check_connection()

        return JSONResponse(
            status_code=200,
            content={
                "status": "healthy",
                "qdrant": "connected",
            },
        )

    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return JSONResponse(
            status_code=503,
            content={
                "status": "unhealthy",
                "error": str(e),
            },
        )
