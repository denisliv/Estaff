"""API роуты для FastAPI приложения."""

import asyncio
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

# Очередь для логов (для WebSocket) - ограниченная очередь для предотвращения переполнения
log_queue: queue.Queue = queue.Queue(maxsize=1000)

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

        # Выполняем поиск в отдельном потоке, чтобы не блокировать event loop
        # Это позволяет WebSocket отправлять логи в реальном времени
        loop = asyncio.get_event_loop()
        candidates = await loop.run_in_executor(
            None,
            lambda: candidate_search_service.search_candidates(
                vacancy_description=request.description,
                k=request.k,
                experience_years_min=request.experience_years_min,
                grade=request.grade,
            ),
        )

        logger.info(f"Поиск завершен. Найдено кандидатов: {len(candidates)}")
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
        # Отправляем все логи из очереди сразу после подключения
        initial_logs_sent = 0
        while True:
            try:
                log_message = log_queue.get_nowait()
                await websocket.send_json(log_message)
                initial_logs_sent += 1
            except queue.Empty:
                break

        # Теперь отправляем новые логи по мере их поступления
        while True:
            try:
                # Проверяем очередь на наличие новых логов
                log_message = log_queue.get_nowait()
                await websocket.send_json(log_message)
            except queue.Empty:
                # Если очередь пуста, ждем немного перед следующей проверкой
                await asyncio.sleep(
                    0.05
                )  # Небольшая задержка для снижения нагрузки на CPU
    except WebSocketDisconnect:
        logger.info("WebSocket клиент отключился")
    except Exception as e:
        logger.error(f"Ошибка в WebSocket соединении: {e}", exc_info=True)


@router.get("/health")
async def health_check():
    """Проверка здоровья сервиса."""
    health_status = {
        "status": "healthy",
        "qdrant": "disconnected",
        "postgres": "disconnected",
    }
    status_code = 200
    
    # Проверяем соединение с Qdrant
    try:
        vector_store_service.check_connection()
        health_status["qdrant"] = "connected"
    except Exception as e:
        logger.error(f"Qdrant health check failed: {e}")
        health_status["qdrant"] = f"error: {str(e)}"
        health_status["status"] = "unhealthy"
        status_code = 503
    
    # Проверяем соединение с PostgreSQL
    try:
        db_config = get_db_config()
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        health_status["postgres"] = "connected"
    except Exception as e:
        logger.error(f"PostgreSQL health check failed: {e}")
        health_status["postgres"] = f"error: {str(e)}"
        health_status["status"] = "unhealthy"
        status_code = 503
    
    return JSONResponse(
        status_code=status_code,
        content=health_status,
    )


@router.get("/collection/status")
async def get_collection_status():
    """Получает статус коллекции Qdrant, включая количество точек."""
    try:
        collection_info = vector_store_service.get_collection_info()
        return JSONResponse(
            status_code=200,
            content={
                "status": "ok",
                "collection_name": collection_info["collection_name"],
                "points_count": collection_info.get("points_count", 0),
                "exists": collection_info.get("exists", True),
            },
        )
    except Exception as e:
        logger.error(f"Ошибка при получении статуса коллекции: {e}", exc_info=True)
        return JSONResponse(
            status_code=200,  # Возвращаем 200, чтобы UI мог обработать ошибку
            content={
                "status": "error",
                "error": str(e),
                "points_count": 0,
            },
        )
