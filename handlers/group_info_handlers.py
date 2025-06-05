import logging
import sqlite3

from telethon import Button, TelegramClient
from telethon.sessions import StringSession

from config import callback_query, API_ID, API_HASH, broadcast_all_text, scheduler, Query
from func.func import get_active_broadcast_groups, gid_key
from main import bot, conn


@bot.on(Query(data=lambda data: data.decode().startswith("listOfgroups_")))
async def handle_groups_list(event: callback_query) -> None:
    user_id = int(event.data.decode().split("_")[1])
    cursor = conn.cursor()
    row = cursor.execute("SELECT session_string FROM sessions WHERE user_id = ?", (user_id,)).fetchone()
    if not row:
        await event.respond("⚠ Не удалось найти аккаунт.")
        return

    session_string = row[0]
    client = TelegramClient(StringSession(session_string), API_ID, API_HASH)
    await client.connect()
    try:
        dialogs = cursor.execute("SELECT group_id, group_username FROM groups WHERE user_id = ?", (user_id,))
        active = get_active_broadcast_groups(user_id)

        buttons = []
        for dialog in dialogs:
            print(dialog)
            buttons.append(
                [Button.inline(f"{"✅" if dialog[0] in active else "❌"} {dialog[1]}",
                               f"group_info_{user_id}_{gid_key(dialog[0])}")]
            )
        cursor.close()
        if not buttons:
            await event.respond("У аккаунта нет групп.")
            return

        await event.respond("📋 Список групп, в которых вы состоите:", buttons=buttons)
    finally:
        await client.disconnect()


# ---------- меню конкретной группы ----------
@bot.on(Query(data=lambda data: data.decode().startswith("group_info_")))
async def handle_group_info(event: callback_query) -> None:
    # в callback-данных: group_info_<user_id>_<group_id>
    data: str = event.data.decode().split("_")
    user_id: int = int(data[2])
    group_id: int = int(data[3])

    cursor: sqlite3.Cursor = conn.cursor()
    row = cursor.execute("SELECT session_string FROM sessions WHERE user_id = ?", (user_id,)).fetchone()
    if not row:
        await event.respond("⚠ Не удалось найти аккаунт.")
        return

    session_string: str = row[0]
    client: TelegramClient = TelegramClient(StringSession(session_string), API_ID, API_HASH)
    await client.connect()
    account_name: str = "Без имени"
    group_name: str = "Нет названия"
    try:
        cursor.execute(
            "SELECT group_username FROM groups WHERE user_id = ? AND group_id = ?",
            (user_id, group_id)
        )
        group_row = cursor.fetchone()
        if group_row:
            ent = await client.get_entity(group_row[0])
            group_name = ent.title
    except Exception as error:
        logging.error(f"Ошибка {error}")
    try:
        account_name = (await client.get_me()).first_name or "Без имени"
    except ValueError:
        logging.info(f"Не корректное имя у аккаунта")
    finally:
        await client.disconnect()

    # --- данные индивидуальной рассылки из таблицы ---
    cursor.execute("""SELECT broadcast_text, interval_minutes FROM broadcasts WHERE user_id = ? AND group_id = ?""",
                   (user_id, gid_key(group_id)))
    broadcast_data = cursor.fetchone()
    cursor.close()
    # --- проверяем job-ы в APScheduler ---
    jid_one = f"broadcast_{user_id}_{gid_key(group_id)}"
    jid_all = f"broadcastALL_{user_id}_{gid_key(group_id)}"

    has_one = scheduler.get_job(jid_one)
    has_all = scheduler.get_job(jid_all)

    # ---------- статус ----------
    if has_one:
        status = "✅ Индивидуальная"
    elif has_all:
        status = "✅ Массовая"
    else:
        status = "⛔ Остановлена"

    # ---------- что выводить в блоке «Текущий текст» ----------
    if has_all:
        txt = broadcast_all_text.get((user_id, gid_key(group_id)), "—")
        text_display = f"📩 **Текущий текст (массовая):**\n{txt}"
    elif broadcast_data:
        broadcast_text, interval_minutes = broadcast_data
        text_display = (
            f"📩 **Текущий текст:**\n{broadcast_text}\n"
            f"⏳ **Интервал:** {interval_minutes} минут"
        )
    else:
        text_display = (
            "📩 **Текущий текст:** ❌ Не задан\n"
            "⏳ **Интервал:** ❌ Не задан"
        )

    # ---------- клавиатура ----------
    keyboard = [
        [Button.inline("📝 Текст и Интервал рассылки",
                       f"BroadcastTextInterval_{user_id}_{gid_key(group_id)}")],
        [Button.inline("✅ Начать/возобновить рассылку",
                       f"StartResumeBroadcast_{user_id}_{gid_key(group_id)}")],
        [Button.inline("⛔ Остановить рассылку",
                       f"StopAccountBroadcast_{user_id}_{gid_key(group_id)}")]
    ]

    # ---------- ответ ----------
    await event.respond(
        f"📢 **Меню рассылки для группы {group_name} "
        f"от аккаунта {account_name}:**\n\n"
        f"{text_display}\n"
        f"🟢 **Статус рассылки:** {status}",
        buttons=keyboard
    )
