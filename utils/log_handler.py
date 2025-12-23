"""Кастомный обработчик логов для WebSocket."""

import logging
import queue


class ApplicationLogFilter(logging.Filter):
    """Фильтр для показа только логов из модулей приложения."""
    
    # Модули приложения, логи которых нужно показывать
    ALLOWED_MODULES = {
        "api",
        "services",
        "utils",
        "db",
        "config",
        "models",
        "scripts",
    }
    
    # Библиотеки, логи которых нужно скрыть
    EXCLUDED_MODULES = {
        "fastapi",
        "uvicorn",
        "langchain",
        "qdrant",
        "pandas",
        "psycopg2",
        "requests",
        "httpx",
        "asyncio",
        "aiofiles",
    }
    
    def filter(self, record: logging.LogRecord) -> bool:
        """Фильтрует логи по имени модуля."""
        # Получаем имя логгера (например, "services.candidate_search" или "fastapi")
        logger_name = record.name
        
        # Разбиваем на части
        parts = logger_name.split(".")
        root_module = parts[0] if parts else ""
        
        # Исключаем логи из библиотек
        if root_module in self.EXCLUDED_MODULES:
            return False
        
        # Показываем только логи из модулей приложения
        if root_module in self.ALLOWED_MODULES:
            return True
        
        # Если имя логгера начинается с одного из наших модулей, показываем
        for allowed in self.ALLOWED_MODULES:
            if logger_name.startswith(allowed + ".") or logger_name == allowed:
                return True
        
        return False


class WebSocketLogHandler(logging.Handler):
    """Обработчик логов, который отправляет логи в очередь для WebSocket."""

    def __init__(self, log_queue: queue.Queue, maxsize: int = 1000):
        super().__init__()
        self.log_queue = log_queue
        # Добавляем фильтр для показа только логов приложения
        self.addFilter(ApplicationLogFilter())
        # Убеждаемся, что очередь имеет максимальный размер для предотвращения переполнения
        if hasattr(log_queue, "maxsize") and log_queue.maxsize == 0:
            # Если очередь неограниченная, это нормально
            pass

    def emit(self, record: logging.LogRecord):
        """Отправляет лог в очередь немедленно."""
        try:
            # Проверяем фильтр перед обработкой
            if not self.filter(record):
                return
            
            # Форматируем запись сразу
            formatted_time = (
                self.format(record).split(" - ")[0]
                if " - " in self.format(record)
                else ""
            )

            log_entry = {
                "timestamp": formatted_time,
                "level": record.levelname,
                "message": record.getMessage(),
                "module": record.name,  # Используем полное имя логгера для лучшей идентификации
            }
            # Неблокирующая отправка в очередь
            try:
                self.log_queue.put_nowait(log_entry)
            except queue.Full:
                # Если очередь переполнена, удаляем старые записи и добавляем новую
                try:
                    # Пытаемся удалить старую запись
                    self.log_queue.get_nowait()
                    # Теперь добавляем новую
                    self.log_queue.put_nowait(log_entry)
                except queue.Empty:
                    # Если не получилось, просто пропускаем
                    pass
        except Exception:
            # Игнорируем ошибки в обработчике логов, чтобы не нарушить основное логирование
            pass

    def flush(self):
        """Принудительно отправляет все буферизованные логи."""
        # Для немедленной отправки ничего не делаем, так как логи отправляются сразу
        pass
