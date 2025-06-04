import logging
from typing import Dict

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from decouple import config
from telethon import TelegramClient
from telethon.events import NewMessage, CallbackQuery

# Конфиг
API_ID = int(config("API_ID"))
API_HASH = config("API_HASH")
BOT_TOKEN = config("BOT_TOKEN")
ADMIN_ID_LIST = [8007729923, 7260719976, 5118442410]  # Вставить ID разрешенных телеграмм аккаунтов через запятую

# Логирование
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
LOG_LEVEL = logging.INFO

# Аннотирование
__CallbackQuery = NewMessage.Event
__Message = CallbackQuery.Event

# Общие переменные
phone_waiting: Dict[int, bool] = {}  # Список пользователей ожидающие подтверждения телефона
code_waiting = {}
password_waiting = {}
user_clients: Dict[int, TelegramClient] = {}  # c
broadcast_all_state = {}  # key = admin_id -> шаги мастера
# сохраняем текст для каждой группы, когда запускаем broadcastALL
broadcast_all_text = {}  # key = (user_id, group_id) -> text
scheduler: AsyncIOScheduler = AsyncIOScheduler()
user_sessions_deleting = {}
user_sessions = {}
user_sessions_phone = {}
user_states = {}
broadcast_jobs = {}
