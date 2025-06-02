import logging
import sqlite3
from telethon import TelegramClient, events, Button
from telethon.sessions import StringSession
from telethon.errors import SessionPasswordNeededError
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.types import Channel, Chat          #  ← ДОБАВИЛИ
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
from dotenv import load_dotenv
import os
import random
from datetime import datetime

def gid_key(value: int) -> int:
    """Возвращает abs(id).  Для супергрупп (-100...) и обычных чатов получается один и тот же «ключ»."""
    return abs(value)

def broadcast_status_emoji(user_id: int, group_id: int) -> str:
    key = gid_key(group_id)
    jid_one = f"broadcast_{user_id}_{key}"
    jid_all = f"broadcastALL_{user_id}_{key}"
    return "✅" if scheduler.get_job(jid_one) or scheduler.get_job(jid_all) else "❌"

def get_active_broadcast_groups(user_id: int) -> set[int]:
    active = set()
    for job in scheduler.get_jobs():
        if job.id.startswith(f"broadcastALL_{user_id}_"):
            try:
                gid_raw = int(job.id.split("_")[2])
                active.add(gid_key(gid_raw))
            except (IndexError, ValueError):
                continue
    return active




load_dotenv()

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID"))

broadcast_all_state = {}      # key = admin_id -> шаги мастера

# сохраняем текст для каждой группы, когда запускаем broadcastALL
broadcast_all_text = {}        # key = (user_id, group_id) -> text



scheduler = AsyncIOScheduler()

conn = sqlite3.connect("sessions.db")
cursor = conn.cursor()

cursor.execute("CREATE TABLE IF NOT EXISTS groups (group_id INTEGER PRIMARY KEY AUTOINCREMENT, group_username TEXT UNIQUE)")
cursor.execute("CREATE TABLE IF NOT EXISTS sessions (user_id INTEGER PRIMARY KEY, session_string TEXT)")
cursor.execute("CREATE TABLE IF NOT EXISTS broadcasts ( id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, group_id INTEGER, session_string TEXT, broadcast_text TEXT, interval_minutes INTEGER, is_active BOOLEAN,FOREIGN KEY (user_id) REFERENCES users(id),FOREIGN KEY (group_id) REFERENCES groups(id));")
conn.commit()

bot = TelegramClient("bot", API_ID, API_HASH).start(bot_token=BOT_TOKEN)

auto_client = TelegramClient(StringSession(), API_ID, API_HASH)

@bot.on(events.NewMessage(pattern="/start"))
async def start(event):
    if event.sender_id == ADMIN_ID:
        buttons = [
            [Button.inline("➕ Добавить аккаунты", b"add_account")],
            [Button.inline("📢 Добавить группы", b"groups")],
            [Button.inline("👤 Мои аккаунты", b"my_accounts")],
            [Button.inline("📑 Мои группы", b"my_groups")]
        ]
        await event.respond("👋 Добро пожаловать, Админ!", buttons=buttons)
    else:
        await event.respond("⛔ Запрещено!")

phone_waiting = {}
code_waiting = {}
password_waiting = {}
user_clients = {}

client = TelegramClient(StringSession(), API_ID, API_HASH)

@bot.on(events.CallbackQuery(data=b"add_account"))
async def add_account(event):
    user_id = event.sender_id
    phone_waiting[user_id] = True
    await event.respond("📲 Напишите номер телефона аккаунта в формате: `+380668887766`")

@bot.on(events.NewMessage(func=lambda e: e.sender_id in phone_waiting and e.text.startswith("+") and e.text[1:].isdigit()))
async def get_phone(event):
    user_id = event.sender_id
    phone_number = event.text.strip()

    user_clients[user_id] = TelegramClient(StringSession(), API_ID, API_HASH)
    await user_clients[user_id].connect()

    await event.respond("⏳ Отправляю код подтверждения...")

    try:
        await user_clients[user_id].send_code_request(phone_number)
        code_waiting[user_id] = phone_number
        del phone_waiting[user_id]
        await event.respond("✅ Код отправлен! Введите его сюда:")
    except Exception as e:
        phone_waiting.pop(user_id, None)
        user_clients.pop(user_id, None)
        await event.respond(f"⚠ Произошла ошибка: {e}\nПопробуйте снова, нажав 'Добавить аккаунт'.")

@bot.on(events.NewMessage(func=lambda e: e.sender_id in code_waiting and e.text.isdigit() and e.sender_id not in broadcast_all_state))
async def get_code(event):
    code = event.text.strip()
    user_id = event.sender_id
    phone_number = code_waiting[user_id]

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
        await event.respond(f"❌ Неверный код или ошибка: {e}\nПопробуйте снова, нажав 'Добавить аккаунт'.")

@bot.on(events.NewMessage(func=lambda e: e.sender_id in password_waiting and e.sender_id not in user_states and e.sender_id not in broadcast_all_state))
async def get_password(event):
    user_id = event.sender_id

    if password_waiting[user_id]["waiting"] and event.message.id > password_waiting[user_id]["last_message_id"]:
        password = event.text.strip()
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

user_sessions_account_spam = {}
active_spam = {}

active_broadcasts = {}

@bot.on(events.CallbackQuery(data=b"my_accounts"))
async def my_accounts(event):
    cursor.execute("SELECT user_id, session_string FROM sessions")
    accounts = cursor.fetchall()

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
            buttons.append([Button.inline(f"⚠ Ошибка при загрузке аккаунта", f"error_{user_id}")])

    await event.respond("📱 **Список ваших аккаунтов:**", buttons=buttons)

@bot.on(events.CallbackQuery(data=lambda data: data.decode().startswith("account_info_")))
async def handle_account_button(event):
    user_id = int(event.data.decode().split("_")[2])

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
        me       = await client.get_me()
        username = me.first_name or "Без имени"
        phone    = me.phone or "Не указан"

        dialogs  = await client.get_dialogs()
        groups   = [d for d in dialogs if d.is_group]

        active_gids = get_active_broadcast_groups(user_id)

        if groups:
            lines = [
                f"{'✅' if gid_key(g.id) in active_gids else '❌'} {g.name}"
                for g in groups
            ]
            group_list = "\n".join(lines)
        else:
            group_list = "У пользователя нет групп."

        mass_active = "🟢 ВКЛ" if active_gids else "🔴 ВЫКЛ"


        buttons = [
            [
                Button.inline("📋 Список групп", f"listOfgroups_{user_id}"),
                Button.inline("🚀 Начать рассылку во все чаты", f"broadcastAll_{user_id}")
            ]
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



# ---------- МЕНЮ «Рассылка во все чаты» ----------
@bot.on(events.CallbackQuery(data=lambda d: d.decode().startswith("broadcastAll_")))
async def broadcast_all_menu(event):
    admin_id = event.sender_id
    target_user_id = int(event.data.decode().split("_")[1])
    # запоминаем аккаунт, с которого шлём
    broadcast_all_state[admin_id] = {"user_id": target_user_id}

    keyboard = [
        [Button.inline("⏲️ Интервал во все группы", f"sameIntervalAll_{target_user_id}")],
        [Button.inline("🎲 Разный интервал (25-35)", f"diffIntervalAll_{target_user_id}")]
    ]
    await event.respond("Выберите режим отправки:", buttons=keyboard)


# ---------- одинаковый интервал ----------
@bot.on(events.CallbackQuery(data=lambda d: d.decode().startswith("sameIntervalAll_")))
async def same_interval_start(event):
    admin_id = event.sender_id
    uid = int(event.data.decode().split("_")[1])
    broadcast_all_state[admin_id] = {"user_id": uid, "mode": "same", "step": "text"}
    await event.respond("📝 Пришлите текст рассылки для **всех** групп этого аккаунта:")


# ---------- случайный интервал ----------
@bot.on(events.CallbackQuery(data=lambda d: d.decode().startswith("diffIntervalAll_")))
async def diff_interval_start(event):
    admin_id = event.sender_id
    uid = int(event.data.decode().split("_")[1])
    broadcast_all_state[admin_id] = {"user_id": uid, "mode": "diff", "step": "text"}
    await event.respond("📝 Пришлите текст рассылки, потом спрошу границы интервала:")


# ---------- мастер-диалог (текст → интервалы) ----------
@bot.on(events.NewMessage(func=lambda e: e.sender_id in broadcast_all_state))
async def broadcast_all_dialog(event):
    st = broadcast_all_state[event.sender_id]

    # шаг 1 — получили текст
    if st["step"] == "text":
        st["text"] = event.text
        if st["mode"] == "same":
            st["step"] = "interval"
            await event.respond("⏲️ Введите интервал (минуты, одно число):")
        else:
            st["step"] = "min"
            await event.respond("🔢 Минимальный интервал (мин):")
        return

    # одинаковый интервал
    if st["mode"] == "same" and st["step"] == "interval":
        try:
            mins = int(event.text)
            if mins <= 0:
                raise ValueError
        except ValueError:
            await event.respond("⚠ Должно быть положительное число.")
            return
        await schedule_account_broadcast(st["user_id"], st["text"], mins, None)
        await event.respond(f"✅ Запустил: каждые {mins} мин.")
        broadcast_all_state.pop(event.sender_id, None)
        return

    # случайный интервал — шаг 2 (min)
    if st["mode"] == "diff" and st["step"] == "min":
        try:
            st["min"] = int(event.text)
            if st["min"] <= 0:
                raise ValueError
        except ValueError:
            await event.respond("⚠ Число > 0.")
            return
        st["step"] = "max"
        await event.respond("🔢 Максимальный интервал (мин):")
        return

    # случайный интервал — шаг 3 (max) + запуск
    if st["mode"] == "diff" and st["step"] == "max":
        try:
            max_m = int(event.text)
            if max_m <= st["min"]:
                raise ValueError
        except ValueError:
            await event.respond("⚠ Максимум должен быть больше минимума.")
            return
        await schedule_account_broadcast(st["user_id"], st["text"], st["min"], max_m)
        await event.respond(f"✅ Запустил: случайно каждые {st['min']}-{max_m} мин.")
        broadcast_all_state.pop(event.sender_id, None)

@bot.on(events.CallbackQuery(data=lambda data: data.decode().startswith("listOfgroups_")))
async def handle_groups_list(event):
    user_id = int(event.data.decode().split("_")[1])

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
        dialogs = await client.get_dialogs()
        active  = get_active_broadcast_groups(user_id)

        buttons = []
        for d in dialogs:
            if d.is_group:
                mark = "✅" if gid_key(d.id) in active else "❌"
                buttons.append(
                    [Button.inline(f"{mark} {d.name}", f"group_info_{user_id}_{d.id}")]
                )


        if not buttons:
            await event.respond("У аккаунта нет групп.")
            return

        await event.respond("📋 Список групп, в которых вы состоите:", buttons=buttons)
    finally:
        await client.disconnect()


broadcast_jobs = {}

# ---------- меню конкретной группы ----------
@bot.on(events.CallbackQuery(data=lambda data: data.decode().startswith("group_info_")))
async def handle_group_info(event):
    # в callback-данных: group_info_<user_id>_<group_id>
    user_id, group_id = map(int, event.data.decode().split("_")[2:])

    # --- вытаскиваем сессию из БД ---
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
        group        = await client.get_entity(group_id)
        account_name = (await client.get_me()).first_name or "Без имени"
    finally:
        await client.disconnect()

    # --- данные индивидуальной рассылки из таблицы ---
    cursor.execute(
        "SELECT broadcast_text, interval_minutes "
        "FROM broadcasts WHERE user_id = ? AND group_id = ?",
        (user_id, group_id)
    )
    broadcast_data = cursor.fetchone()

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
                       f"broadcasttextinterval_{user_id}_{group_id}")],
        [Button.inline("✅ Начать/возобновить рассылку",
                       f"startresumebroadcast_{user_id}_{group_id}")],
        [Button.inline("⛔ Остановить рассылку",
                       f"stop_accountbroadcast_{user_id}_{group_id}")]
    ]

    # ---------- ответ ----------
    await event.respond(
        f"📢 **Меню рассылки для группы {group.title} "
        f"от аккаунта {account_name}:**\n\n"
        f"{text_display}\n"
        f"🟢 **Статус рассылки:** {status}",
        buttons=keyboard
    )
  

user_states = {}

@bot.on(events.CallbackQuery(data=lambda data: data.decode().startswith("broadcasttextinterval_")))
async def handle_broadcast_text_interval(event):
    data = event.data.decode()
    user_id, group_id = map(int, data.split("_")[1:])

    async with bot.conversation(event.sender_id) as conv:
        user_states[event.sender_id] = "text_and_interval_waiting"

        await event.respond("📝 Пожалуйста, отправьте текст для рассылки.")
        new_broadcast_text_event = await conv.wait_event(events.NewMessage(from_users=event.sender_id))
        new_broadcast_text = new_broadcast_text_event.text

        await event.respond("⏳ Пожалуйста, отправьте интервал рассылки в минут.")
        try:
            new_interval_minutes_event = await conv.wait_event(events.NewMessage(from_users=event.sender_id))
            new_interval_minutes = int(new_interval_minutes_event.text)

            cursor.execute("SELECT * FROM broadcasts WHERE user_id = ? AND group_id = ?", (user_id, group_id))
            existing_row = cursor.fetchone()

            if existing_row:
                update_broadcast_data(user_id, group_id, new_broadcast_text, new_interval_minutes)
                await event.respond(f"✅ Текст рассылки успешно обновлен на: {new_broadcast_text}\n⏳ Интервал рассылки обновлен на {new_interval_minutes} минут.")
            else:
                create_broadcast_data(user_id, group_id, new_broadcast_text, new_interval_minutes)
                await event.respond(f"✅ Текст рассылки и интервал были успешно добавлены:\n{new_broadcast_text}\n⏳ Интервал рассылки — {new_interval_minutes} минут.")

            del user_states[event.sender_id]

        except ValueError:
            await event.respond("⚠ Пожалуйста, введите корректное число минут для интервала.")

            del user_states[event.sender_id]

def create_broadcast_data(user_id, group_id, broadcast_text, interval_minutes):
    cursor.execute("""
        INSERT INTO broadcasts (user_id, group_id, broadcast_text, interval_minutes, is_active)
        VALUES (?, ?, ?, ?, ?)
    """, (user_id, group_id, broadcast_text, interval_minutes, False))
    conn.commit()

def update_broadcast_data(user_id, group_id, broadcast_text, interval_minutes):
    cursor.execute("""
        UPDATE broadcasts
        SET broadcast_text = ?, interval_minutes = ?
        WHERE user_id = ? AND group_id = ?
    """, (broadcast_text, interval_minutes, user_id, group_id))
    conn.commit()

@bot.on(events.CallbackQuery(data=lambda data: data.decode().startswith("startresumebroadcast_")))
async def start_resume_broadcast(event):
    data = event.data.decode()
    parts = data.split("_")

    if len(parts) < 3:
        await event.respond("⚠ Произошла ошибка при обработке данных. Попробуйте еще раз.")
        return

    try:
        user_id = int(parts[1])
        group_id = int(parts[2])
    except ValueError as e:
        await event.respond(f"⚠ Ошибка при извлечении данных: {e}")
        return

    job_id = f"broadcast_{user_id}_{group_id}"
    existing_job = scheduler.get_job(job_id)

    if existing_job:
        await event.respond("⚠ Рассылка уже активна для этой группы.")
        return

    cursor.execute("SELECT broadcast_text, interval_minutes FROM broadcasts WHERE user_id = ? AND group_id = ?", (user_id, group_id))
    row = cursor.fetchone()

    if row:
        broadcast_text, interval_minutes = row
        if not broadcast_text or not interval_minutes or interval_minutes <= 0:
            await event.respond("⚠ Пожалуйста, убедитесь, что текст рассылки и корректный интервал установлены.")
            return

        session_string_row = cursor.execute("SELECT session_string FROM sessions WHERE user_id = ?", (user_id,)).fetchone()
        if not session_string_row:
            await event.respond("⚠ Ошибка: не найден session_string для аккаунта.")
            return

        session_string = session_string_row[0]
        session = StringSession(session_string)
        client = TelegramClient(session, API_ID, API_HASH)

        await client.connect()

        try:
            group = await client.get_entity(group_id)
            group_title = group.title
        except Exception as e:
            await event.respond(f"⚠ Ошибка при получении информации о группе: {e}")
            return
        finally:
            await client.disconnect()

        async def send_broadcast():
            session = StringSession(session_string)
            client = TelegramClient(session, API_ID, API_HASH)

            await client.connect()
            try:
                group = await client.get_entity(group_id)
                await client.send_message(group, broadcast_text)
            except Exception as e:
                print(f"Ошибка отправки сообщения в группу: {e}")
            finally:
                await client.disconnect()

        scheduler.add_job(
            send_broadcast,
            IntervalTrigger(minutes=interval_minutes),
            id=job_id,
            replace_existing=True
        )

        await event.respond(f"✅ Рассылка в группу **{group_title}** начата!")
        if not scheduler.running:
            scheduler.start()
    else:
        await event.respond("⚠ Рассылка еще не настроена для этой группы.")

@bot.on(events.CallbackQuery(data=lambda data: data.decode().startswith("stop_accountbroadcast_")))
async def stop_broadcast(event):
    data = event.data.decode()
    try:
        user_id, group_id = map(int, data.split("_")[2:])

    except ValueError as e:
        await event.respond(f"⚠ Ошибка при извлечении user_id и group_id: {e}")

        return
    session_string = cursor.execute("SELECT session_string FROM sessions WHERE user_id = ?", (user_id,)).fetchone()[0]
    session = StringSession(session_string)
    client = TelegramClient(session, API_ID, API_HASH)
    await client.connect()
    group = await client.get_entity(group_id)
    job_id = f"broadcast_{user_id}_{group_id}"
    job = scheduler.get_job(job_id)
    if job:
        job.remove()
        cursor.execute("UPDATE broadcasts SET is_active = ? WHERE user_id = ? AND group_id = ?", (False, user_id, group_id))
        conn.commit()

        await event.respond(f"⛔ Рассылка в группу **{group.title}** остановлена.")
    else:
        await event.respond(f"⚠ Рассылка в группу **{group.title}** не была запущена.")

user_sessions_phone = {}

@bot.on(events.CallbackQuery(data=b"delete_account"))
async def handle_delete_account(event):
    user_sessions_phone[event.sender_id] = {"step": "awaiting_phone"}
    await event.respond("📲 Введите номер телефона аккаунта, который нужно удалить:")

@bot.on(events.NewMessage)
async def handle_user_input(event):
    user_state = user_sessions_phone.get(event.sender_id)

    if user_state and user_state["step"] == "awaiting_phone":
        phone_number = event.text.strip()

        if phone_number.startswith("+") and phone_number[1:].isdigit():
            cursor.execute("SELECT user_id FROM sessions WHERE session_string = ?", (phone_number,))
            user = cursor.fetchone()

            if user:
                cursor.execute("DELETE FROM sessions WHERE session_string = ?", (phone_number,))
                conn.commit()
                await event.respond(f"✅ Аккаунт с номером {phone_number} успешно удален.")
            else:
                await event.respond("⚠ Этот аккаунт не найден в базе данных.")

            user_sessions_phone.pop(event.sender_id, None)
        else:
            await event.respond("⚠ Пожалуйста, введите корректный номер телефона, начиная с '+'.")

cursor.execute("CREATE TABLE IF NOT EXISTS groups (group_id INTEGER PRIMARY KEY AUTOINCREMENT, group_username TEXT UNIQUE)")
conn.commit()

user_sessions = {}

@bot.on(events.CallbackQuery(data=b"groups"))
async def manage_groups(event):
    user_sessions[event.sender_id] = {"step": "awaiting_group_username"}
    await event.respond("📲 Напишите @username группы, чтобы добавить её в базу данных:")

@bot.on(events.NewMessage)
async def handle_group_input(event):
    user_state = user_sessions.pop(event.sender_id, None) 

    if user_state and user_state["step"] == "awaiting_group_username":
        group_username = event.text.strip()

        if group_username.startswith("@") and " " not in group_username:  
            try:
                cursor.execute("INSERT INTO groups (group_username) VALUES (?)", (group_username,))
                conn.commit()
                await event.respond(f"✅ Группа {group_username} успешно добавлена в базу данных!")
            except sqlite3.IntegrityError:
                await event.respond("⚠ Эта группа уже существует в базе данных.")
        else:
            await event.respond("⚠ Ошибка! Неправильный формат. Попробуйте снова, нажав кнопку.")



@bot.on(events.CallbackQuery(data=b"my_groups"))
async def my_groups(event):
    cursor.execute("SELECT group_username FROM groups")
    groups = cursor.fetchall()

    if not groups:
        await event.respond("❌ У вас нет добавленных групп.")
        return

    message = "📑 **Список добавленных групп:**\n"

    for group in groups:
        message += f"📌 {group[0]}\n"
        buttons = [
            [Button.inline("❌ Удалить группу", b"delete_group")],
            [Button.inline("➕ Добавить все аккаунты в эти группы", b"add_all_accounts_to_groups")]
        ]
    await event.respond(message, buttons=buttons)

@bot.on(events.CallbackQuery(data=b"add_all_accounts_to_groups"))
async def add_all_accounts_to_groups(event):
    cursor.execute("SELECT session_string FROM sessions")
    accounts = cursor.fetchall()

    cursor.execute("SELECT group_username FROM groups")
    groups = cursor.fetchall()

    if not accounts:
        await event.respond("❌ Нет добавленных аккаунтов.")
        return

    if not groups:
        await event.respond("❌ Нет добавленных групп.")
        return

    group_list = "\n".join([f"📌 {group[0]}" for group in groups])
    await event.respond(f"✅ Аккаунты успешно добавлены в следующие группы:\n{group_list}")

    for account in accounts:
        session = StringSession(account[0])
        client = TelegramClient(session, API_ID, API_HASH)
        await client.connect()
        try:

            for group in groups:
                await client(JoinChannelRequest(group[0]))
        except Exception as e:
            await event.respond(f"⚠ Ошибка при добавлении аккаунта: {e}")

user_sessions_deliting = {}

@bot.on(events.CallbackQuery(data=b"delete_group"))
async def handle_delete_group(event):
    user_sessions_deliting[event.sender_id] = {"step": "awaiting_group_username"}
    await event.respond("📲 Введите @username группы, которую нужно удалить:")

@bot.on(events.NewMessage)
async def handle_user_input(event):
    user_state = user_sessions_deliting.get(event.sender_id)

    if user_state and user_state["step"] == "awaiting_group_username":
        group_username = event.text.strip()

        if group_username.startswith("@"):
            cursor.execute("SELECT * FROM groups WHERE group_username = ?", (group_username,))
            group = cursor.fetchone()

            if group:
                cursor.execute("DELETE FROM groups WHERE group_username = ?", (group_username,))
                conn.commit()
                await event.respond(f"✅ Группа {group_username} успешно удалена из базы данных!")
            else:
                await event.respond("⚠ Группа с именем {group_username} не найдена в базе данных.")

            user_sessions_deliting.pop(event.sender_id, None)
        else:
            await event.respond("⚠ Пожалуйста, введите корректный @username группы, начиная с '@'.")
            return

async def schedule_account_broadcast(user_id: int, text: str,
                                     min_m: int, max_m: int | None):
    row = cursor.execute(
        "SELECT session_string FROM sessions WHERE user_id = ?", (user_id,)
    ).fetchone()
    if not row:
        return
    sess_str = row[0]

    cli = TelegramClient(StringSession(sess_str), API_ID, API_HASH)
    await cli.connect()

    dialogs = await cli.get_dialogs()
    entities: list[Channel | Chat] = []
    for d in dialogs:
        ent = d.entity

        # 1) лички / боты — пропускаем
        if not isinstance(ent, (Channel, Chat)):
            continue

        # 2) обычный broadcast-канал (лента новостей): постить может только владелец
        if isinstance(ent, Channel) and ent.broadcast and not ent.megagroup:
            continue

        # 3) проверяем, есть ли у нас право "send_messages"
        try:
            perms = await cli.get_permissions(ent)
            if hasattr(perms, "send_messages") and not perms.send_messages:
                continue
        except Exception:
            continue        # не смогли запросить права — лучше пропустить

        entities.append(ent)


        try:
            # проверяем право «send messages»
            perms = await cli.get_permissions(ent)
            if hasattr(perms, "send_messages") and not perms.send_messages:
                continue
        except Exception:
            continue          # если не смогли проверить — пропускаем

        entities.append(ent)

    await cli.disconnect()
    if not entities:
        return

    # --- ставим job на каждую годную группу ---
    for ent in entities:
        gid_key_str = gid_key(ent.id)           # положительный id
        job_id = f"broadcastALL_{user_id}_{gid_key_str}"

        scheduler.remove_job(job_id) if scheduler.get_job(job_id) else None

        async def send_message(ss=sess_str, entity=ent, txt=text, job_id=job_id):
            from telethon.errors import ChatAdminRequiredError, ChatWriteForbiddenError
            c = TelegramClient(StringSession(ss), API_ID, API_HASH)
            await c.connect()
            try:
                await c.send_message(entity, txt)
            except (ChatAdminRequiredError, ChatWriteForbiddenError):
                # нас лишили права писать – снимаем job и убираем "галочку"
                scheduler.remove_job(job_id)        # ⬅ главное
                broadcast_all_text.pop((user_id, gid_key(entity.id)), None)
            except Exception as e:
                logger.warning("❌ не отправлено в %s: %s", entity.id, e)
            finally:
                await c.disconnect()


        base = (min_m + max_m)//2 if max_m else min_m
        jitter = (max_m - min_m)*60//2 if max_m else 0
        trigger = IntervalTrigger(minutes=base, jitter=jitter)

        scheduler.add_job(send_message, trigger,
                          id=job_id,
                          next_run_time=datetime.utcnow(),
                          replace_existing=True)

        broadcast_all_text[(user_id, gid_key_str)] = text

    if not scheduler.running:
        scheduler.start()

print("🚀 Бот запущен...")
bot.run_until_disconnected()