import logging

import pandas as pd
import psycopg2
from psycopg2 import extras

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "estaff",
    "user": "postgres",
    "password": "postgres",
}


def insert_candidates(df: pd.DataFrame, db_config):
    """
    Вставляет данные из pandas DataFrame в таблицу candidates.
    Колонки DF должны строго соответствовать схеме таблицы.
    """
    columns = [
        "id",
        "fullname",
        "gender_id",
        "age",
        "location_name",
        "mobile_phone",
        "email",
        "desired_position_name",
        "profession_name_1",
        "profession_name_2",
        "exp_years",
        "cv_summary_desc",
        "last_job_position_name",
        "last_job_finished",
        "last_comment",
        "html",
        "creation_date",
    ]

    if not set(columns).issubset(df.columns):
        missing = set(columns) - set(df.columns)
        logger.error(f"Отсутствуют колонки в DataFrame: {missing}")
        raise ValueError(f"Отсутствуют колонки: {missing}")

    df = df[columns].copy()

    if df["creation_date"].dtype == "datetime64[ns]":
        df["creation_date"] = df["creation_date"].dt.strftime("%Y-%m-%d")

    int_cols = ["gender_id", "age", "exp_years"]
    for col in int_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        df[col] = df[col].astype(object)

    df = df.where(pd.notna(df), None)

    conn = None
    try:
        logger.info(f"Подключение к БД для вставки {len(df)} записей...")
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()

        insert_query = """
        INSERT INTO candidates (
            id, fullname, gender_id, age, location_name, mobile_phone, email,
            desired_position_name, profession_name_1, profession_name_2,
            exp_years, cv_summary_desc, last_job_position_name,
            last_job_finished, last_comment, html, creation_date
        ) VALUES %s
        ON CONFLICT (id) DO UPDATE SET
            fullname = EXCLUDED.fullname,
            gender_id = EXCLUDED.gender_id,
            age = EXCLUDED.age,
            location_name = EXCLUDED.location_name,
            mobile_phone = EXCLUDED.mobile_phone,
            email = EXCLUDED.email,
            desired_position_name = EXCLUDED.desired_position_name,
            profession_name_1 = EXCLUDED.profession_name_1,
            profession_name_2 = EXCLUDED.profession_name_2,
            exp_years = EXCLUDED.exp_years,
            cv_summary_desc = EXCLUDED.cv_summary_desc,
            last_job_position_name = EXCLUDED.last_job_position_name,
            last_job_finished = EXCLUDED.last_job_finished,
            last_comment = EXCLUDED.last_comment,
            html = EXCLUDED.html,
            creation_date = EXCLUDED.creation_date;
        """

        data_tuples = [tuple(row) for row in df.to_numpy()]

        extras.execute_values(
            cur, insert_query, data_tuples, template=None, page_size=1000
        )
        conn.commit()

        logger.info(f"Успешно вставлено {cur.rowcount} записей в таблицу candidates.")

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Ошибка при вставке данных: {e}")
        raise
    finally:
        if conn:
            cur.close()
            conn.close()
        logger.info("Соединение с БД закрыто.")


def fetch_candidates(
    db_config: dict, columns: list = None, where_clause: str = None, limit: int = None
) -> pd.DataFrame:
    """
    Загружает данные из таблицы candidates PostgreSQL в pandas DataFrame.
    """
    default_columns = [
        "id",
        "fullname",
        "gender_id",
        "age",
        "location_name",
        "mobile_phone",
        "email",
        "desired_position_name",
        "profession_name_1",
        "profession_name_2",
        "exp_years",
        "cv_summary_desc",
        "last_job_position_name",
        "last_job_finished",
        "last_comment",
        "html",
        "creation_date",
    ]

    selected_columns = columns if columns is not None else default_columns

    allowed_columns = set(default_columns)
    for col in selected_columns:
        if (
            not col.replace("_", "").replace(" ", "").isalnum()
            or col not in allowed_columns
        ):
            raise ValueError(f"Недопустимое имя колонки: {col}")

    columns_str = ", ".join(selected_columns)
    query = f"SELECT {columns_str} FROM candidates"

    if where_clause:
        query += f" WHERE {where_clause}"
    if limit is not None:
        query += f" LIMIT {limit}"

    with psycopg2.connect(**db_config) as conn:
        df = pd.read_sql(query, conn)
    return df
