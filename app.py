"""Главное приложение FastAPI."""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.routes import router

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Жизненный цикл приложения."""
    # Startup
    logger.info("Запуск приложения Estaff HR Service...")
    yield
    # Shutdown
    logger.info("Остановка приложения...")


app = FastAPI(
    title="Estaff HR Service",
    description="Сервис для поиска и оценки кандидатов по вакансиям на основе RAG",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # В продакшене указать конкретные домены
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Подключение роутеров
app.include_router(router, prefix="/api/v1", tags=["HR Service"])


@app.get("/")
async def root():
    """Корневой эндпоинт."""
    return {
        "service": "Estaff HR Service",
        "version": "1.0.0",
        "description": "Сервис для поиска и оценки кандидатов по вакансиям",
    }
