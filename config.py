import logging
from typing import Dict, Tuple, List, Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from decouple import config
from telethon import TelegramClient
from telethon.events import NewMessage, CallbackQuery

# Конфиг
API_ID: int = int(config("API_ID"))
API_HASH: str = config("API_HASH")
BOT_TOKEN: str = config("BOT_TOKEN")
ADMIN_ID_LIST: List[int] = [8007729923, 7260719976, 5118442410]  # <-- Вставить ID разрешенных телеграмм аккаунтов через запятую

# Логирование
LOG_FORMAT: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
LOG_LEVEL: logging.INFO = logging.INFO
LOG_FILE: Optional[str] = None  # Файл "logging.log"
LOG_ENCODING: str = "utf-8"

# Аннотирование
__CallbackQuery = NewMessage.Event
__Message = CallbackQuery.Event
__Dict_int_str = Dict[int, str]
__Dict_all_str = Dict[str, str]
__Dict_int_dict = Dict[int, dict]


phone_waiting: Dict[int, bool] = {}  # Список пользователей ожидающие подтверждения телефона
code_waiting: __Dict_int_str = {}
password_waiting: __Dict_int_dict = {}
user_clients: Dict[int, TelegramClient] = {}  # c
broadcast_all_state: __Dict_int_dict = {}  # key = admin_id -> шаги мастера
broadcast_all_text: __Dict_int_str = {}  # key = (user_id, group_id) -> text
scheduler: AsyncIOScheduler = AsyncIOScheduler()
user_sessions_deleting: Dict[int, __Dict_all_str] = {}
user_sessions = {}
user_sessions_phone: Dict[Tuple[int, int], __Dict_all_str] = {}
user_states = {}
