"""Кастомный обработчик логов для WebSocket."""

import logging
import queue


class WebSocketLogHandler(logging.Handler):
    """Обработчик логов, который отправляет логи в очередь для WebSocket."""

    def __init__(self, log_queue: queue.Queue, maxsize: int = 1000):
        super().__init__()
        self.log_queue = log_queue
        # Убеждаемся, что очередь имеет максимальный размер для предотвращения переполнения
        if hasattr(log_queue, "maxsize") and log_queue.maxsize == 0:
            # Если очередь неограниченная, это нормально
            pass

    def emit(self, record: logging.LogRecord):
        """Отправляет лог в очередь немедленно."""
        try:
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
                "module": record.module,
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
