import logging

from func.db_func import create_table, delete_table
from handlers import *
from config import (BOT_TOKEN, LOG_LEVEL, LOG_FORMAT, LOG_FILE, LOG_ENCODING, conn, bot)

logging.basicConfig(filename=LOG_FILE, format=LOG_FORMAT, level=LOG_LEVEL, encoding=LOG_ENCODING)


if __name__ == "__main__":
    delete_table()
    bot.start(bot_token=BOT_TOKEN)
    logging.info(f"🚀 Бот запущен... ")
    create_table()
    bot.run_until_disconnected()
    delete_table()
    conn.close()
