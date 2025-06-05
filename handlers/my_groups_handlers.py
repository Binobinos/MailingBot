import logging

from telethon import Button, TelegramClient
from telethon.sessions import StringSession
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.types import Channel, Chat

from config import callback_query, API_ID, API_HASH, Query
from func.func import broadcast_status_emoji
from main import bot, conn


@bot.on(Query(data=b"my_groups"))
async def my_groups(event: callback_query) -> None:
    cursor = conn.cursor()
    cursor.execute("SELECT group_id, group_username FROM pre_groups", )
    groups = cursor.fetchall()
    cursor.close()
    if not groups:
        await event.respond("❌ У вас нет добавленных групп.")
        return
    buttons = [
        [Button.inline("➕ Добавить все аккаунты в эти группы", b"add_all_accounts_to_groups"),
         Button.inline("✔ Добавить все группы", b"add_all_groups", )],
        [Button.inline("❌ Удалить группу", b"delete_group")],
    ]

    message = "📑 **Список добавленных групп:**\n"
    for group in groups:
        print(group)
        message += f"{broadcast_status_emoji(event.sender_id, group[0])} {group[1]}\n"
    await event.respond(message, buttons=buttons)


@bot.on(Query(data=b"add_all_accounts_to_groups"))
async def add_all_accounts_to_groups(event: callback_query) -> None:
    cursor = conn.cursor()
    cursor.execute("SELECT user_id, session_string FROM sessions")
    accounts = cursor.fetchall()

    cursor.execute("SELECT group_id, group_username FROM pre_groups")
    groups = cursor.fetchall()
    if not accounts:
        await event.respond("❌ Нет добавленных аккаунтов.")
        return

    if not groups:
        await event.respond("❌ Нет добавленных групп.")
        return

    for account in accounts:
        session = StringSession(account[1])
        client = TelegramClient(session, API_ID, API_HASH)
        await client.connect()
        try:
            for group in groups:
                try:
                    await client(JoinChannelRequest(group[1]))
                except Exception as e:
                    logging.error(f"Ошибка {e}")
                cursor.execute("""INSERT OR IGNORE INTO groups 
                                        (user_id, group_id, group_username) 
                                        VALUES (?, ?, ?)""", (account[0], group[0], group[1]))
                logging.info(f"Добавляем в базу данных группу ({account[0], group[0], group[1]})")
        except Exception as e:
            await event.respond(f"⚠ Ошибка при добавлении аккаунта: {e}")
    group_list = "\n".join([f"📌 {group[1]}" for group in groups])
    await event.respond(f"✅ Аккаунты успешно добавлены в следующие группы:\n{group_list}")
    conn.commit()
    cursor.close()


@bot.on(Query(data=b"add_all_groups"))
async def add_all_accounts_to_groups(event: callback_query) -> None:
    cursor = conn.cursor()
    cursor.execute("SELECT user_id, session_string FROM sessions")
    accounts = cursor.fetchall()

    cursor.execute("SELECT group_id, group_username FROM pre_groups")
    groups = cursor.fetchall()
    if not accounts:
        await event.respond("❌ Нет добавленных аккаунтов.")
        return

    if not groups:
        await event.respond("❌ Нет добавленных групп.")
        return

    for account in accounts:
        session = StringSession(account[1])
        client = TelegramClient(session, API_ID, API_HASH)
        await client.connect()
        for group in await client.get_dialogs():
            ent = group.entity
            if not isinstance(ent, (Channel, Chat)):
                logging.info(f"пропускаем задачу {ent} так как данный чат Личный диалог или бот")
                continue

            if isinstance(ent, Channel) and ent.broadcast and not ent.megagroup:
                logging.info(f"пропускаем задачу {ent} так как данный чат витрина-канал")
                continue
            logging.info(f"Добавляем группу")
            cursor.execute(f"""INSERT OR IGNORE INTO pre_groups 
                                        (group_id, group_username) 
                                        VALUES (?, ?)""", (ent.id, ent.title))
    conn.commit()
    cursor.close()
    await event.respond("✅ Все группы добавленны")
