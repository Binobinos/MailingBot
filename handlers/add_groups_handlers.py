from sqlite3 import IntegrityError

from telethon.events import CallbackQuery, NewMessage

from config import __CallbackQuery, __Message, user_sessions
from main import bot, conn


@bot.on(CallbackQuery(data=b"add_groups"))
async def manage_groups(event: __CallbackQuery) -> None:
    user_sessions[event.sender_id] = {"step": "awaiting_group_username"}
    await event.respond("📲 Напишите @username группы, чтобы добавить её в базу данных:")


@bot.on(NewMessage(func=lambda event: (user_state := user_sessions.pop(event.sender_id, None)) and user_state[
    "step"] == "awaiting_group_username"))
async def handle_group_input(event: __Message) -> None:
    group_username = event.text.strip()

    if group_username.startswith("@") and " " not in group_username:
        cursor = conn.cursor()
        try:
            ids = await bot.get_entity(group_username)
            cursor.execute("INSERT INTO pre_groups (group_username, group_id) VALUES (?, ?)",
                           (group_username, ids.id))
            conn.commit()
            await event.respond(f"✅ Группа {group_username} успешно добавлена в базу данных!")
        except IntegrityError:
            await event.respond("⚠ Эта группа уже существует в базе данных.")
        finally:
            cursor.close()
    else:
        await event.respond("⚠ Ошибка! Неправильный формат. Попробуйте снова, нажав кнопку.")
