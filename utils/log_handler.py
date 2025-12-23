"""Кастомный обработчик логов для WebSocket."""

import logging
import queue
from typing import Optional


class WebSocketLogHandler(logging.Handler):
    """Обработчик логов, который отправляет логи в очередь для WebSocket."""
    
    def __init__(self, log_queue: queue.Queue):
        super().__init__()
        self.log_queue = log_queue
    
    def emit(self, record: logging.LogRecord):
        """Отправляет лог в очередь."""
        try:
            log_entry = {
                "timestamp": self.format(record).split(" - ")[0] if " - " in self.format(record) else "",
                "level": record.levelname,
                "message": record.getMessage(),
                "module": record.module,
            }
            # Неблокирующая отправка в очередь
            try:
                self.log_queue.put_nowait(log_entry)
            except queue.Full:
                # Если очередь переполнена, просто пропускаем
                pass
        except Exception:
            # Игнорируем ошибки в обработчике логов
            pass

