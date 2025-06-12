from config import conn


def create_table() -> None:
    """
    Создает таблицы SQL если их нет
    :return:
        None
    """
    start_cursor = conn.cursor()
    start_cursor.execute("""
        CREATE TABLE IF NOT EXISTS pre_groups (
            group_id INTEGER PRIMARY KEY AUTOINCREMENT, 
            group_username TEXT UNIQUE)""")

    start_cursor.execute("""
        CREATE TABLE IF NOT EXISTS groups (
            group_id INTEGER,
            group_username TEXT,
            user_id INTEGER)""")

    start_cursor.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            user_id INTEGER PRIMARY KEY,
            session_string TEXT)""")

    start_cursor.execute("""
        CREATE TABLE IF NOT EXISTS broadcasts ( 
            user_id INTEGER, 
            group_id INTEGER, 
            session_string TEXT, 
            broadcast_text TEXT, 
            interval_minutes INTEGER,
            is_active BOOLEAN)""")
    start_cursor.execute("""
        CREATE TABLE IF NOT EXISTS send_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            group_id INTEGER,
            group_name TEXT,
            sent_at TEXT,
            message_text TEXT);""")

    conn.commit()
    start_cursor.close()


def delete_table() -> None:
    """
    После остановки бота удаляет невыполненные задачи из базы данных
    :return:
        None
    """
    end_cursor = conn.cursor()
    end_cursor.execute("""DELETE FROM broadcasts """)
    conn.commit()
    end_cursor.close()
