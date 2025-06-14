import logging

from telethon import TelegramClient
from telethon.errors import FloodWaitError, SessionPasswordNeededError
from telethon.sessions import StringSession
from telethon.tl.functions.auth import SendCodeRequest

from config import (callback_query, callback_message, phone_waiting, code_waiting, password_waiting, user_clients,
                    API_ID,
                    API_HASH, broadcast_all_state, user_states, New_Message, Query, bot, conn)


@bot.on(Query(data=b"add_account"))
async def add_account(event: callback_query) -> None:
    """
    Добавляет аккаунт
    """
    logging.info(f"Выбрана кнопка добавления аккаунта. подтверждение телефона и отправка кода")
    user_id: int = event.sender_id
    phone_waiting[user_id] = True
    await event.respond("📲 Напишите номер телефона аккаунта в формате: `+79000000000`")


@bot.on(New_Message(func=lambda e: e.sender_id in phone_waiting and e.text.startswith("+") and e.text[1:].isdigit()))
async def send_code_for_phone(event: callback_message) -> None:
    """
    Отправляет код на телефон
    """
    user_id: int = event.sender_id
    phone_number: str = event.text.strip()
    logging.info(f"Отправляю {user_id} на телефон {phone_number} код подтверждения")
    user_clients[user_id] = TelegramClient(StringSession(), API_ID, API_HASH)
    await user_clients[user_id].connect()

    await event.respond("⏳ Отправляю код подтверждения...")

    try:
        await user_clients[user_id].send_code_request(phone_number)
        code_waiting[user_id] = phone_number
        del phone_waiting[user_id]
        await event.respond("✅ Код отправлен! Введите его сюда:")
        logging.info(f"Код отправлен")
    except Exception as e:
        if isinstance(e, (SendCodeRequest, FloodWaitError)):
            sec_time = int(str(e).split()[3])
            message = (f"⚠ Телеграмм забанил за быстрые запросы. "
                       f"Подождите {(a := sec_time // 3600)} Часов {(b := ((sec_time - a * 3600) // 60))}"
                       f" Минут {sec_time - a * 3600 - b * 60} Секунд")
            await event.respond(message)
            logging.error(message)
        else:
            phone_waiting.pop(user_id, None)
            user_clients.pop(user_id, None)
            logging.error(f"⚠ Произошла ошибка: {e}")
            await event.respond(f"⚠ Произошла ошибка: {e}\nПопробуйте снова, нажав 'Добавить аккаунт'.")


@bot.on(New_Message(
    func=lambda e: e.sender_id in code_waiting and e.text.isdigit() and e.sender_id not in broadcast_all_state))
async def get_code(event: callback_message) -> None:
    """
    Проверяет код от пользователя
    """
    code = event.text.strip()
    user_id = event.sender_id
    phone_number = code_waiting[user_id]
    cursor = conn.cursor()
    try:
        await user_clients[user_id].sign_in(phone_number, code)
        session_string = user_clients[user_id].session.save()
        me = await user_clients[user_id].get_me()

        cursor.execute("INSERT INTO sessions (user_id, session_string) VALUES (?, ?)", (me.id, session_string))
        conn.commit()

        del code_waiting[user_id]
        del user_clients[user_id]
        await event.respond("✅ Авторизация прошла успешно!")
    except SessionPasswordNeededError:
        password_waiting[user_id] = {"waiting": True, "last_message_id": event.message.id}
        await event.respond("⚠ Этот аккаунт защищен паролем. Отправьте пароль:")
    except Exception as e:
        del code_waiting[user_id]
        user_clients.pop(user_id, None)
        logging.error(f"Ошибка: {e}, Неверный код")
        await event.respond(f"❌ Неверный код или ошибка: {e}\nПопробуйте снова, нажав 'Добавить аккаунт'.")
    finally:
        cursor.close()


@bot.on(New_Message(func=lambda
        e: e.sender_id in password_waiting and e.sender_id not in user_states and e.sender_id not in broadcast_all_state))
async def get_password(event: callback_message) -> None:
    user_id = event.sender_id
    if password_waiting[user_id]["waiting"] and event.message.id > password_waiting[user_id]["last_message_id"]:
        password = event.text.strip()
        cursor = conn.cursor()
        try:
            await user_clients[user_id].sign_in(password=password)
            me = await user_clients[user_id].get_me()
            session_string = user_clients[user_id].session.save()

            cursor.execute("INSERT INTO sessions (user_id, session_string) VALUES (?, ?)", (me.id, session_string))
            conn.commit()

            del password_waiting[user_id]
            del user_clients[user_id]
            await event.respond("✅ Авторизация с паролем прошла успешно!")
        except Exception as e:
            await event.respond(f"⚠ Ошибка при вводе пароля: {e}\nПопробуйте снова, нажав 'Добавить аккаунт'.")
        finally:
            cursor.close()
