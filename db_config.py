import mysql.connector
import logging
import threading

local = threading.local()

db_logger = logging.getLogger("db_queries")
db_logger.setLevel(logging.DEBUG)

file_handler = logging.FileHandler("database.log", encoding='utf-8')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(logging.Formatter('%(message)s'))
db_logger.addHandler(file_handler)

error_handler = logging.FileHandler("database_error.log", encoding='utf-8')
error_handler.setLevel(logging.ERROR)
error_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
db_logger.addHandler(error_handler)

db_logger.propagate = False

DB_CONFIG = dict(
    host="localhost",
    user="root",
    password="actowiz",
    database="ubereats",
    port=3306
)


def escape_value(val):
    if val is None:
        return "NULL"
    val = str(val).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{val}'"


def connect():
    try:
        conn = getattr(local, "conn", None)
        if conn is None or not conn.is_connected():
            local.conn = mysql.connector.connect(**DB_CONFIG)
        return local.conn
    except Exception as e:
        db_logger.error(f"Error in connect(): {e}", exc_info=True)
        return None


def create(table_name: str):
    try:
        query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                restarent_id            VARCHAR(100) NOT NULL,
                restaurant_name         VARCHAR(255),
                slug                    VARCHAR(255),
                phone_number            VARCHAR(50),
                open_or_close           TINYINT(1),
                hours                   TEXT,
                currency                VARCHAR(10),
                image_url               TEXT,
                street_address          VARCHAR(255),
                city                    VARCHAR(100),
                country                 VARCHAR(100),
                postalcode              VARCHAR(20),
                region                  VARCHAR(100),
                location_type           VARCHAR(50),
                range_of_dilivery       VARCHAR(100),
                timing_of_dilivery      VARCHAR(255),
                category                TEXT,
                menu                    LONGTEXT,
                PRIMARY KEY (restarent_id)
            );"""
        conn = connect()
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        cursor.close()
    except Exception as e:
        db_logger.error(f"Error in create(): {e}", exc_info=True)


def insert_into_db(table_name: str, data: dict):
    try:
        cols = ",".join(list(data.keys()))
        vals = ",".join(['%s'] * len(data.keys()))
        query = f"INSERT IGNORE INTO {table_name} ({cols}) VALUES ({vals})"
        conn = connect()
        cursor = conn.cursor()
        cursor.execute(query, tuple(data.values()))
        conn.commit()
        cursor.close()
    except Exception as e:
        db_logger.error(f"Error in insert_into_db(): {e}", exc_info=True)


def batch_insert(table_name: str, rows: list):
    if not rows:
        return
    rows = [r for r in rows if r.get("restarent_id") is not None]
    if not rows:
        return
    try:
        cols = ",".join(rows[0].keys())
        vals = ",".join(['%s'] * len(rows[0].keys()))
        query = f"INSERT IGNORE INTO {table_name} ({cols}) VALUES ({vals})"

        conn = connect()
        cursor = conn.cursor()

        cursor.executemany(query, [tuple(r.values()) for r in rows])
        conn.commit()

        inserted_count = cursor.rowcount

        inserted_rows = rows[:inserted_count]
        for r in inserted_rows:
            escaped_vals = ",".join(escape_value(v) for v in r.values())
            log_query = f"INSERT IGNORE INTO {table_name} ({cols}) VALUES ({escaped_vals});"
            db_logger.info(log_query)

        cursor.close()
    except Exception as e:
        db_logger.error(f"Error in batch_insert(): {e}", exc_info=True)


if __name__ == "__main__":
    connect()
    print("DB connection OK")