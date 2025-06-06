import logging

from telethon import Button
from config import callback_message, ADMIN_ID_LIST, New_Message, bot


@bot.on(New_Message(pattern="/start"))
async def start(event: callback_message) -> None:
    """
    Обрабатывает команду /start
    """
    logging.info(f"Нажата команда /start")
    if event.sender_id in ADMIN_ID_LIST:
        buttons = [
            [Button.inline("➕ Добавить аккаунт 👤", b"add_account"),
             Button.inline("➕ Добавить группу 👥", b"add_groups")],
            [Button.inline("👤 Мои аккаунты", b"my_accounts")],
            [Button.inline("🕗 История рассылки", b"show_history")]
        ]
        await event.respond("👋 Добро пожаловать, Админ!", buttons=buttons)
    else:
        await event.respond("⛔ Запрещено!")
