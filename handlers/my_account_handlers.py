import logging

from telethon import Button, TelegramClient
from telethon.sessions import StringSession

from config import callback_query, API_ID, API_HASH, Query, bot, conn
from func.func import get_active_broadcast_groups, broadcast_status_emoji


@bot.on(Query(data=b"my_accounts"))
async def my_accounts(event: callback_query) -> None:
    """
    Выводит список аккаунтов
    """
    try:
        cursor = conn.cursor()
        buttons = []
        accounts_found = False

        for user_id, session_string in cursor.execute("SELECT user_id, session_string FROM sessions"):
            accounts_found = True
            client = None
            try:
                client = TelegramClient(StringSession(session_string), API_ID, API_HASH)
                await client.connect()
                me = await client.get_me()
                username = me.first_name if me.first_name else "Без ника"
                buttons.append([Button.inline(f"👤 {username}", f"account_info_{user_id}")])
            except Exception:
                buttons.append([Button.inline("⚠ Ошибка при загрузке аккаунта", f"error_{user_id}")])
            finally:
                if client:
                    await client.disconnect()

        cursor.close()

        if not accounts_found:
            await event.respond("❌ У вас нет добавленных аккаунтов")
            return

        await event.respond("📱 **Список ваших аккаунтов:**", buttons=buttons)

    except Exception as e:
        logging.error(f"Error in my_accounts: {e}")
        await event.respond("⚠ Произошла ошибка при получении списка аккаунтов")


@bot.on(Query(data=lambda data: data.decode().startswith("account_info_")))
async def handle_account_button(event: callback_query) -> None:
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
            lines.append(f"{broadcast_status_emoji(user_id, int(group[0]))} {group[1]}")
        group_list = "\n".join(lines)
        if not group_list:
            group_list = "У пользователя нет групп."

        mass_active = "🟢 ВКЛ" if active_gids else "🔴 ВЫКЛ"
        buttons = [
            [
                Button.inline("📋 Список групп", f"listOfgroups_{user_id}")
            ],
            [Button.inline("🚀 Начать рассылку во все чаты", f"broadcast_All_{user_id}"),
             Button.inline("❌ Остановить общую рассылку", f"StopBroadcastAll_{user_id}")],
            [Button.inline("✔ Обновить информацию о группах", f"add_all_groups_{user_id}", )],
            [Button.inline("❌ Удалить этот аккаунт", f"delete_account_{user_id}")]
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
