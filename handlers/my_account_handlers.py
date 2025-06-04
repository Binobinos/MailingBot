from telethon import Button, TelegramClient
from telethon.events import CallbackQuery
from telethon.sessions import StringSession

from config import __CallbackQuery, API_ID, API_HASH
from func.func import get_active_broadcast_groups, gid_key
from main import bot, conn


@bot.on(CallbackQuery(data=b"my_accounts"))
async def my_accounts(event: __CallbackQuery) -> None:
    """
    Выводит список аккаунтов
    """
    cursor = conn.cursor()
    cursor.execute("SELECT user_id, session_string FROM sessions")
    accounts = cursor.fetchall()
    cursor.close()
    if not accounts:
        await event.respond("❌ У вас нет добавленных аккаунтов.")
        return

    buttons = []
    for user_id, session_string in accounts:
        session = StringSession(session_string)
        client = TelegramClient(session, API_ID, API_HASH)
        await client.connect()
        try:
            me = await client.get_me()
            username = me.first_name if me.first_name else "Без ника"
            buttons.append([Button.inline(f"👤 {username}", f"account_info_{user_id}")])
        except Exception as e:
            buttons.append([Button.inline(f"⚠ Ошибка при загрузке аккаунта {e}", f"error_{user_id}")])
    await event.respond("📱 **Список ваших аккаунтов:**", buttons=buttons)


@bot.on(CallbackQuery(data=lambda data: data.decode().startswith("account_info_")))
async def handle_account_button(event: __CallbackQuery) -> None:
    user_id = int(event.data.decode().split("_")[2])
    cursor = conn.cursor()
    row = cursor.execute(
        "SELECT session_string FROM sessions WHERE user_id = ?", (user_id,)
    ).fetchone()
    if not row:
        await event.respond("⚠ Не удалось найти аккаунт.")
        return

    session_string = row[0]
    client = TelegramClient(StringSession(session_string), API_ID, API_HASH)
    await client.connect()
    try:
        me = await client.get_me()
        username = me.first_name or "Без имени"
        phone = me.phone or "Не указан"
        groups = cursor.execute("SELECT group_id, group_username FROM groups WHERE user_id = ?", (user_id,))

        active_gids = get_active_broadcast_groups(user_id)
        lines = []
        for group in groups:
            lines.append(f"{'✅' if gid_key(group[0]) in active_gids else '❌'} {group[1]}")
        group_list = "\n".join(lines)
        if not group_list:
            group_list = "У пользователя нет групп."

        mass_active = "🟢 ВКЛ" if active_gids else "🔴 ВЫКЛ"
        buttons = [
            [
                Button.inline("📋 Список групп", f"listOfgroups_{user_id}")
            ],
            [Button.inline("🚀 Начать рассылку во все чаты", f"broadcastAll_{user_id}"),
             Button.inline("❌ Остановить общую рассылку", f"StopBroadcastAll_{user_id}")],
            [Button.inline("❌ Удалить аккаунт", "delete_account")]
        ]

        await event.respond(
            f"📢 **Меню для аккаунта {username}:**\n"
            f"🚀 **Массовая рассылка:** {mass_active}\n\n"
            f"📌 **Имя:** {username}\n"
            f"📞 **Номер:** `+{phone}`\n\n"
            f"📝 **Список групп:**\n{group_list}",
            buttons=buttons
        )
    finally:
        await client.disconnect()
        cursor.close()
