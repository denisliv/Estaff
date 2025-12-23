import logging

import psycopg2
from psycopg2 import OperationalError

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


def table_exists(cursor, table_name):
    """Проверяет, существует ли таблица в схеме public."""
    cursor.execute(
        """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_name = %s
        );
    """,
        (table_name,),
    )
    return cursor.fetchone()[0]


def create_tables():
    conn = None
    try:
        logger.info("Подключение к базе данных PostgreSQL...")
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        cur = conn.cursor()
        logger.info("Подключение успешно.")

        table_name = "candidates"
        if table_exists(cur, table_name):
            logger.info(f"Таблица '{table_name}' уже существует.")
        else:
            create_query = """
            CREATE TABLE candidates (
                id BIGINT PRIMARY KEY,
                fullname TEXT,
                gender_id SMALLINT,
                age SMALLINT,
                location_name TEXT,
                mobile_phone TEXT,
                email TEXT,
                desired_position_name TEXT,
                profession_name_1 TEXT,
                profession_name_2 TEXT,
                exp_years SMALLINT,
                cv_summary_desc TEXT,
                last_job_position_name TEXT,
                last_job_finished BOOLEAN,
                last_comment TEXT,
                html TEXT,
                creation_date DATE
            );
            """
            cur.execute(create_query)
            logger.info(f"Таблица '{table_name}' успешно создана.")

        # Таблица users
        table_name = "users"
        if table_exists(cur, table_name):
            logger.info(f"Таблица '{table_name}' уже существует.")
        else:
            create_query = """
            CREATE TABLE users (
                id SERIAL PRIMARY KEY,
                fullname TEXT NOT NULL,
                role TEXT NOT NULL,
                login TEXT UNIQUE NOT NULL,
                password TEXT NOT NULL,
                creation_date DATE
            );
            """
            cur.execute(create_query)
            logger.info(f"Таблица '{table_name}' успешно создана.")

    except OperationalError as e:
        logger.error(f"Ошибка подключения к базе данных: {e}")
    except Exception as e:
        logger.error(f"Неожиданная ошибка при создании таблиц: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()
            logger.info("Соединение с базой данных закрыто.")


if __name__ == "__main__":
    create_tables()
