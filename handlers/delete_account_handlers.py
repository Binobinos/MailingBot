import logging

from config import callback_query, callback_message, user_sessions_phone, Query
from main import bot, conn


@bot.on(Query(data=b"delete_account"))
async def handle_delete_account(event: callback_query) -> None:
    user_sessions_phone[event.sender_id] = {"step": "awaiting_phone"}
    await event.respond("📲 Введите номер телефона аккаунта, который нужно удалить:")


@bot.on(Query(func=lambda event: (user_state := user_sessions_phone.get(event.sender_id)) and user_state[
    "step"] == "awaiting_phone"))
async def handle_user_input(event: callback_message):
    phone_number = event.text.strip()
    if phone_number.startswith("+") and phone_number[1:].isdigit():
        cursor = conn.cursor()
        cursor.execute("SELECT user_id FROM sessions WHERE session_string = ?", (phone_number,))
        user = cursor.fetchone()

        if user:
            cursor.execute("DELETE FROM sessions WHERE session_string = ?", (phone_number,))
            conn.commit()
            logging.info(f"✅ Аккаунт с номером {phone_number} успешно удален.")
            await event.respond(f"✅ Аккаунт с номером {phone_number} успешно удален.")
        else:
            logging.warning(f"Аккаунт {user} не найден")
            await event.respond("⚠ Этот аккаунт не найден в базе данных.")

        user_sessions_phone.pop(event.sender_id, None)
        cursor.close()
    else:
        logging.error(f"Не корректный ввод телефонного номера")
        await event.respond("⚠ Пожалуйста, введите корректный номер телефона, начиная с '+'.")
