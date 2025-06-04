import logging

from telethon import Button
from telethon.events import NewMessage

from config import __Message, ADMIN_ID_LIST
from main import bot


@bot.on(NewMessage(pattern="/start"))
async def start(event: __Message) -> None:
    """
    Обрабатывает команду /start
    """
    logging.info(f"Нажата команда /start")
    if event.sender_id in ADMIN_ID_LIST:
        buttons = [
            [Button.inline("➕ Добавить аккаунт 👤", b"add_account"),
             Button.inline("➕ Добавить группу 👥", b"add_groups")],
            [Button.inline("👤 Мои аккаунты", b"my_accounts"), Button.inline("📑 Мои группы", b"my_groups")],
            [Button.inline("🕗 История рассылки", b"show_history")]
        ]
        await event.respond("👋 Добро пожаловать, Админ!", buttons=buttons)
    else:
        await event.respond("⛔ Запрещено!")
