"""Главное приложение FastAPI."""

import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from api.routes import log_queue, router
from utils.log_handler import WebSocketLogHandler

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

# Добавляем WebSocket обработчик для логов
ws_handler = WebSocketLogHandler(log_queue)
ws_handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
ws_handler.setFormatter(formatter)

# Добавляем обработчик ко всем логгерам
root_logger = logging.getLogger()
root_logger.addHandler(ws_handler)

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

# Подключение статических файлов
static_dir = Path(__file__).parent / "static"
static_dir.mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")


@app.get("/", response_class=HTMLResponse)
async def root():
    """Главная страница с UI."""
    html_file = static_dir / "index.html"
    if html_file.exists():
        return html_file.read_text(encoding="utf-8")
    return HTMLResponse(content="""
    <html>
        <head><title>Estaff HR Service</title></head>
        <body>
            <h1>Estaff HR Service</h1>
            <p>UI доступен по адресу /static/index.html</p>
        </body>
    </html>
    """)
