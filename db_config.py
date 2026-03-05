import mysql.connector

conn = None

def connect():
    global conn
    if conn is None or not conn.is_connected():
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="actowiz",
            database="ubereats"
        )
    return conn

def create(table_name: str):
    query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            restarent_id VARCHAR(50) NOT NULL,
            restaurant_name VARCHAR(255),
            slug VARCHAR(255),
            phone_number VARCHAR(50),
            open_or_close TINYINT(1),
            hours TEXT,
            currency VARCHAR(10),
            image_url TEXT,
            street_address VARCHAR(255),
            city VARCHAR(100),
            country VARCHAR(100),
            postalcode VARCHAR(20),
            region VARCHAR(100),
            location_type VARCHAR(50),
            range_of_dilivery VARCHAR(100),
            timing_of_dilivery VARCHAR(100),
            category TEXT,
            menu LONGTEXT,
            PRIMARY KEY (restarent_id)
        );"""
    conn = connect()
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()

def insert_into_db(table_name: str, data: dict):
    cols = ",".join(list(data.keys()))
    vals = ",".join(['%s'] * len(data.keys()))
    query = f"""INSERT IGNORE INTO {table_name} ({cols}) VALUES ({vals})"""
    conn = connect()
    cursor = conn.cursor()
    cursor.execute(query, tuple(data.values()))
    conn.commit()

def batch_insert(table_name: str, rows: list[dict]):
    if not rows:
        return
    rows = [r for r in rows if r.get("restarent_id") is not None]
    if not rows:
        return
    cols = ",".join(list(rows[0].keys()))
    vals = ",".join(['%s'] * len(rows[0].keys()))
    query = f"""INSERT IGNORE INTO {table_name} ({cols}) VALUES ({vals})"""
    conn = connect()
    cursor = conn.cursor()
    cursor.executemany(query, [tuple(r.values()) for r in rows])
    conn.commit()

if __name__ == "__main__":
    connect()