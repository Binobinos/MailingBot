import logging
import sqlite3

from telethon import TelegramClient

from config import (API_ID, API_HASH, BOT_TOKEN, LOG_LEVEL, LOG_FORMAT)

logging.basicConfig(format=LOG_FORMAT, level=LOG_LEVEL)

bot: TelegramClient = TelegramClient("bot", API_ID, API_HASH).start(bot_token=BOT_TOKEN)

conn = sqlite3.connect("sessions.db")


if __name__ == "__main__":
    logging.info("🚀 Бот запущен...")
    start_cursor = conn.cursor()
    start_cursor.execute("""
    CREATE TABLE IF NOT EXISTS pre_groups (
        group_id INTEGER PRIMARY KEY AUTOINCREMENT, 
        group_username TEXT UNIQUE)""")

    start_cursor.execute("""
    CREATE TABLE IF NOT EXISTS groups (
        group_id INTEGER , 
        group_username TEXT,
        user_id INTEGER,
        UNIQUE(user_id, group_username))""")

    start_cursor.execute("""
    CREATE TABLE IF NOT EXISTS sessions (
        user_id INTEGER PRIMARY KEY,
        session_string TEXT)""")

    start_cursor.execute("""
    CREATE TABLE IF NOT EXISTS broadcasts ( 
        id INTEGER PRIMARY KEY AUTOINCREMENT, 
        user_id INTEGER, group_id INTEGER, 
        session_string TEXT, 
        broadcast_text TEXT, 
        interval_minutes INTEGER,
        is_active BOOLEAN,
        FOREIGN KEY (user_id) REFERENCES users(id),
        FOREIGN KEY (group_id) REFERENCES groups(id))""")

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
    bot.run_until_disconnected()
    end_cursor = conn.cursor()
    end_cursor.execute("""DELETE FROM broadcasts WHERE is_active = ?""", (True,))
    end_cursor.execute("""DELETE FROM broadcasts WHERE is_active = ?""", (False,))
    end_cursor.close()
    conn.close()
