"""Сервис для загрузки данных из CSV файлов в PostgreSQL."""

import logging
from pathlib import Path
from typing import Dict

import pandas as pd

from config.settings import settings
from db.db_manager import fetch_candidates, insert_candidates
from utils.utils import deserialize_cell, extract_html_resume

logger = logging.getLogger(__name__)


def get_db_config() -> Dict[str, str]:
    """Возвращает конфигурацию БД из settings."""
    return {
        "host": settings.db_host,
        "port": settings.db_port,
        "database": settings.db_name,
        "user": settings.db_user,
        "password": settings.db_password,
    }


def load_data_from_csv() -> pd.DataFrame:
    """
    Загружает данные из CSV файлов в директории data_dir.
    Ожидает файлы с паттерном data_file_pattern (например, data_fake_1.csv, data_fake_2.csv, ...).
    """
    data_dir = Path(settings.data_dir)
    dfs = []

    logger.info(f"Начинаю загрузку данных из {data_dir}...")

    for x in range(1, 26):  # Поддерживаем до 25 файлов
        file_path = data_dir / settings.data_file_pattern.format(x)
        if file_path.is_file():
            logger.info(f"Загружаю файл: {file_path}")
            df_tmp = pd.read_csv(file_path)
            df_tmp["data"] = df_tmp["data"].apply(deserialize_cell)
            dfs.append(df_tmp)
        else:
            logger.debug(f"Файл {file_path} не найден, пропускаю.")

    if not dfs:
        raise ValueError(f"Не найдено ни одного файла данных в {data_dir}")

    df = pd.concat(dfs, ignore_index=True)
    logger.info(f"Загружено {len(df)} записей из {len(dfs)} файлов")

    return df


def preprocess_data(df: pd.DataFrame) -> pd.DataFrame:
    """Предобработка данных: извлечение HTML резюме и удаление дубликатов."""
    logger.info("Начинаю предобработку данных...")

    df["html"] = df["data"].apply(extract_html_resume)
    df = df[df["html"].notna()].reset_index(drop=True).copy()
    logger.info(f"После фильтрации по наличию HTML: {len(df)} записей")

    df = df.drop_duplicates(subset=["id"], keep="last")
    logger.info(f"После удаления дубликатов: {len(df)} записей")

    return df


def update_postgres_database() -> int:
    """
    Обновляет базу данных PostgreSQL новыми данными из CSV файлов.
    Возвращает количество обработанных записей.
    """
    try:
        df = load_data_from_csv()
        df = preprocess_data(df)

        db_config = get_db_config()
        insert_candidates(df, db_config=db_config)

        logger.info(f"Успешно обновлена база данных. Обработано {len(df)} записей")
        return len(df)

    except Exception as e:
        logger.error(f"Ошибка при обновлении базы данных: {e}", exc_info=True)
        raise


def get_candidates_from_db() -> pd.DataFrame:
    """Получает всех кандидатов из PostgreSQL."""
    db_config = get_db_config()
    return fetch_candidates(db_config=db_config)
