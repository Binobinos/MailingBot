from sqlite3 import IntegrityError

from config import callback_query, callback_message, user_sessions, New_Message, Query, bot, conn


@bot.on(Query(data=b"add_groups"))
async def manage_groups(event: callback_query) -> None:
    user_sessions[event.sender_id] = {"step": "awaiting_group_username"}
    await event.respond("📲 Напишите @username группы, чтобы добавить её в базу данных:")


@bot.on(New_Message(func=lambda event: (user_state := user_sessions.pop(event.sender_id, None)) and user_state[
    "step"] == "awaiting_group_username"))
async def handle_group_input(event: callback_message) -> None:
    group_username: str = event.text.strip()

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
