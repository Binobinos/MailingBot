import datetime
import logging
import sqlite3
from typing import Dict, List, Union

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from telethon import TelegramClient, events, Button
from telethon.events import NewMessage, CallbackQuery
from telethon.errors import FloodWaitError, ChatWriteForbiddenError, ChatAdminRequiredError
from telethon.errors import SessionPasswordNeededError
from telethon.sessions import StringSession
from telethon.tl.functions.auth import SendCodeRequest
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.types import Channel, Chat  # ← ДОБАВИЛИ

from config import API_ID, API_HASH, BOT_TOKEN, ADMIN_ID_LIST, LOG_LEVEL, LOG_FORMAT

logging.basicConfig(format=LOG_FORMAT, level=LOG_LEVEL)

__CallbackQuery = NewMessage.Event
__Message = CallbackQuery.Event
bot: TelegramClient = TelegramClient("bot", API_ID, API_HASH).start(bot_token=BOT_TOKEN)
phone_waiting: Dict[int, bool] = {}  # Список пользователей ожидающие подтверждения телефона
code_waiting = {}
password_waiting = {}
user_clients: Dict[int, TelegramClient] = {}  # c
broadcast_all_state = {}  # key = admin_id -> шаги мастера
# сохраняем текст для каждой группы, когда запускаем broadcastALL
broadcast_all_text = {}  # key = (user_id, group_id) -> text
scheduler = AsyncIOScheduler()
user_sessions_deleting = {}
user_sessions = {}
user_sessions_phone = {}
user_states = {}
broadcast_jobs = {}

conn = sqlite3.connect("sessions.db")
start_cursor = conn.cursor()
start_cursor.execute("""
CREATE TABLE IF NOT EXISTS pre_groups (
    group_id INTEGER PRIMARY KEY AUTOINCREMENT, 
    group_username TEXT UNIQUE)""")

start_cursor.execute("""
CREATE TABLE IF NOT EXISTS groups (
    group_id INTEGER , 
    group_username TEXT,
    user_id INTEGER,
    UNIQUE(user_id, group_username))""")

start_cursor.execute("""
CREATE TABLE IF NOT EXISTS sessions (
    user_id INTEGER PRIMARY KEY,
    session_string TEXT)""")

start_cursor.execute("""
CREATE TABLE IF NOT EXISTS broadcasts ( 
    id INTEGER PRIMARY KEY AUTOINCREMENT, 
    user_id INTEGER, group_id INTEGER, 
    session_string TEXT, 
    broadcast_text TEXT, 
    interval_minutes INTEGER,
    is_active BOOLEAN,
    FOREIGN KEY (user_id) REFERENCES users(id),
    FOREIGN KEY (group_id) REFERENCES groups(id))""")

start_cursor.execute("""
CREATE TABLE IF NOT EXISTS send_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    group_id INTEGER,
    group_name TEXT,
    sent_at TEXT,
    message_text TEXT);""")

conn.commit()
start_cursor.close()


# ------------------------------------------------------------------
def gid_key(value: int) -> int:
    """Возвращает abs(id).  Для супергрупп (-100...) и обычных чатов получается один и тот же «ключ»."""
    return abs(value)


def broadcast_status_emoji(user_id: int,
                           group_id: int) -> str:
    gid_key_str = gid_key(group_id)
    return "✅" if gid_key_str in get_active_broadcast_groups(user_id) else "❌"


def get_active_broadcast_groups(user_id: int) -> List[int]:
    active = set()
    cursor = conn.cursor()
    cursor.execute("""SELECT group_id FROM broadcasts WHERE is_active = ? AND user_id = ?""", (True, user_id))
    broadcasts = cursor.fetchall()
    for job in broadcasts:
        active.add(job[0])
    cursor.close()
    return list(active)


def create_broadcast_data(user_id: int,
                          group_id: int,
                          broadcast_text: str,
                          interval_minutes: int) -> None:
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO broadcasts (user_id, group_id, broadcast_text, interval_minutes, is_active)
        VALUES (?, ?, ?, ?, ?)
    """, (user_id, gid_key(group_id), broadcast_text, interval_minutes, False))
    conn.commit()
    cursor.close()


def update_broadcast_data(user_id: int,
                          group_id: int,
                          broadcast_text: str,
                          interval_minutes: int) -> None:
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE broadcasts
        SET broadcast_text = ?, interval_minutes = ?
        WHERE user_id = ? AND group_id = ?
    """, (broadcast_text, interval_minutes, user_id, gid_key(group_id)))
    conn.commit()
    cursor.close()


async def schedule_account_broadcast(user_id: int,
                                     text: str,
                                     min_m: int,
                                     max_m: Union[int] = None) -> None:
    """Ставит/обновляет jobs broadcastALL_<user>_<gid> только для чатов,
    куда аккаунт реально может писать."""
    # --- сессия ---
    cursor = conn.cursor()
    row = cursor.execute(
        "SELECT session_string FROM sessions WHERE user_id = ?", (user_id,)
    ).fetchone()
    cursor.execute("""UPDATE broadcasts SET broadcast_text = ? WHERE user_id = ?""", (text, user_id))
    if not row:
        return
    sess_str = row[0]

    client = TelegramClient(StringSession(sess_str), API_ID, API_HASH)
    await client.connect()

    # --- собираем «разрешённые» чаты/каналы ---
    groups = cursor.execute("""SELECT group_username, group_id FROM groups WHERE user_id = ?""", (user_id,))
    ok_entities: list[Channel | Chat] = []
    for group in groups:
        ent = await client.get_entity(group[0])
        try:
            if not isinstance(ent, (Channel, Chat)):
                logging.info(f"пропускаем задачу {ent} так как данный чат Личный диалог или бот")
                continue
            if isinstance(ent, Channel) and ent.broadcast and not ent.megagroup:
                logging.info(f"пропускаем задачу {ent} так как данный чат витрина-канал")
                continue
        except Exception as error:
            logging.warning(f"Не смог проверить: {error}")
            continue
        ok_entities.append(ent)

    if not ok_entities:
        print()
        logging.info(f"Нету задач выходим")
        return

    for ent in ok_entities:
        print(ent)
        job_id = f"broadcastALL_{user_id}_{gid_key(ent.id)}"
        cursor.execute("UPDATE broadcasts SET is_active = ? WHERE user_id = ? AND group_id = ?",
                       (True, user_id, gid_key(ent.id)))
        if scheduler.get_job(job_id):
            logging.info(f"Удаляем задачу")
            scheduler.remove_job(job_id)

        async def send_message(
                ss=sess_str,
                entity=ent,
                jobs_id=job_id,
                start_text=text
        ):
            c = TelegramClient(StringSession(ss), API_ID, API_HASH)
            await c.connect()
            my_cursor = conn.cursor()
            my_cursor.execute("""SELECT broadcast_text FROM broadcasts WHERE group_id = ? AND user_id = ?""",
                              (entity.id, user_id))
            txt = my_cursor.fetchone()
            if txt:
                txt = txt[0]
            else:
                txt = start_text
            try:
                await c.send_message(entity, txt)
                logging.info(f"Отправляем {entity} {txt}")
                my_cursor.execute(
                    """INSERT INTO send_history (
                                user_id, 
                                group_id, 
                                group_name, 
                                sent_at, 
                                message_text) 
                            VALUES (?, ?, ?, ?, ?)""",
                    (user_id, entity.id, entity.title if hasattr(entity, 'title') else '',
                     datetime.datetime.now().isoformat(),
                     txt)
                )
                my_cursor.execute("UPDATE broadcasts SET is_active = ? WHERE user_id = ? AND group_id = ?",
                                  (False, user_id, gid_key(entity.id)))
            except (ChatWriteForbiddenError, ChatAdminRequiredError) as e:
                logging.info(f"Снимаем задачу {jobs_id} — нет прав писать: {e}")
                scheduler.remove_job(jobs_id)
                my_cursor.execute("UPDATE broadcasts SET is_active = ? WHERE user_id = ? AND group_id = ?",
                                  (False, user_id, gid_key(entity.id)))
            finally:
                await c.disconnect()
                conn.commit()
                my_cursor.close()

        base = (min_m + max_m) // 2 if max_m else min_m
        jitter = (max_m - min_m) * 60 // 2 if max_m else 0
        trigger = IntervalTrigger(minutes=base, jitter=jitter)
        next_run = datetime.datetime.now() + datetime.timedelta(
            minutes=((max_m - min_m) / len(ok_entities) if max_m else min_m))
        logging.info(f"Добавляем задачу в очередь")
        scheduler.add_job(
            send_message,
            trigger,
            id=job_id,
            next_run_time=next_run,
            replace_existing=True,
        )
    if not scheduler.running:
        logging.info("Запускаем все задачи")
        scheduler.start()

    await client.disconnect()
    if not ok_entities:
        return


# ------------------------------------------------------------------
@bot.on(NewMessage(pattern="/start"))
async def start(event: __Message) -> None:
    """
    Обрабатывает команду /start
    """
    logging.info(f"Нажата команда /start")
    if event.sender_id in ADMIN_ID_LIST:
        buttons = [
            [Button.inline("➕ Добавить аккаунт 👤", b"add_account"), Button.inline("➕ Добавить группу 👥", b"groups")],
            [Button.inline("👤 Мои аккаунты", b"my_accounts"), Button.inline("📑 Мои группы", b"my_groups")],
            [Button.inline("🕗 История рассылки", b"show_history")]
        ]
        await event.respond("👋 Добро пожаловать, Админ!", buttons=buttons)
    else:
        await event.respond("⛔ Запрещено!")


# ------- Добавление аккаунта -------
@bot.on(CallbackQuery(data=b"add_account"))
async def add_account(event: __CallbackQuery) -> None:
    """
    Добавляет аккаунт
    """
    logging.info(f"Выбрана кнопка добавления аккаунта. подтверждение телефона и отправка кода")
    user_id: int = event.sender_id
    phone_waiting[user_id] = True
    await event.respond("📲 Напишите номер телефона аккаунта в формате: `+79000000000`")


@bot.on(NewMessage(func=lambda e: e.sender_id in phone_waiting and e.text.startswith("+") and e.text[1:].isdigit()))
async def send_code_for_phone(event: __Message) -> None:
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


@bot.on(NewMessage(func=lambda e: e.sender_id in code_waiting and e.text.isdigit() and e.sender_id not in broadcast_all_state))
async def get_code(event: __Message) -> None:
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


@bot.on(NewMessage(func=lambda e: e.sender_id in password_waiting and e.sender_id not in user_states and e.sender_id not in broadcast_all_state))
async def get_password(event: __Message) -> None:
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


# ------- Список аккаунтов -------
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


# ---------- МЕНЮ «Рассылка во все чаты» ----------
@bot.on(CallbackQuery(data=lambda d: d.decode().startswith("broadcastAll_")))
async def broadcast_all_menu(event: __CallbackQuery) -> None:
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
@bot.on(CallbackQuery(data=lambda d: d.decode().startswith("sameIntervalAll_")))
async def same_interval_start(event: __CallbackQuery) -> None:
    admin_id = event.sender_id
    uid = int(event.data.decode().split("_")[1])
    broadcast_all_state[admin_id] = {"user_id": uid, "mode": "same", "step": "text"}
    await event.respond("📝 Пришлите текст рассылки для **всех** групп этого аккаунта:")


# ---------- случайный интервал ----------
@bot.on(CallbackQuery(data=lambda d: d.decode().startswith("diffIntervalAll_")))
async def diff_interval_start(event: __CallbackQuery) -> None:
    admin_id = event.sender_id
    uid = int(event.data.decode().split("_")[1])
    broadcast_all_state[admin_id] = {"user_id": uid, "mode": "diff", "step": "text"}
    await event.respond("📝 Пришлите текст рассылки, потом спрошу границы интервала:")


# ---------- мастер-диалог (текст → интервалы) ----------
@bot.on(CallbackQuery(func=lambda e: e.sender_id in broadcast_all_state))
async def broadcast_all_dialog(event: __Message) -> None:
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
        min_time = int(event.text)
        if min_time <= 0:
            await event.respond("⚠ Должно быть положительное число.")
            return
        await schedule_account_broadcast(st["user_id"], st["text"], min_time, None)
        await event.respond(f"✅ Запустил: каждые {min_time} мин.")
        broadcast_all_state.pop(event.sender_id, None)
        return

    # случайный интервал — шаг 2 (min)
    if st["mode"] == "diff" and st["step"] == "min":
        st["min"] = int(event.text)
        if st["min"] <= 0:
            await event.respond("⚠ Минимальное число должно быть больше нуля.")
            return
        st["step"] = "max"
        await event.respond("🔢 Максимальный интервал (мин):")
        return

    # случайный интервал — шаг 3 (max) + запуск
    if st["mode"] == "diff" and st["step"] == "max":
        max_m = int(event.text)
        if max_m <= st["min"]:
            await event.respond("⚠ Максимальное число должно быть больше минимального числа.")
            return
        await schedule_account_broadcast(st["user_id"], st["text"], st["min"], max_m)
        await event.respond(f"✅ Запустил: случайно каждые {st['min']}-{max_m} мин.")
        broadcast_all_state.pop(event.sender_id, None)


@bot.on(CallbackQuery(data=lambda data: data.decode().startswith("listOfgroups_")))
async def handle_groups_list(event: __CallbackQuery) -> None:
    user_id = int(event.data.decode().split("_")[1])
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
@bot.on(CallbackQuery(data=lambda data: data.decode().startswith("group_info_")))
async def handle_group_info(event: __CallbackQuery) -> None:
    # в callback-данных: group_info_<user_id>_<group_id>
    user_id, group_id = map(int, event.data.decode().split("_")[2:])
    cursor = conn.cursor()
    # --- вытаскиваем сессию из БД ---
    row = cursor.execute("SELECT session_string FROM sessions WHERE user_id = ?", (user_id,)).fetchone()
    if not row:
        await event.respond("⚠ Не удалось найти аккаунт.")
        return

    session_string = row[0]
    client = TelegramClient(StringSession(session_string), API_ID, API_HASH)
    await client.connect()
    account_name = "Без имени"
    group_name = "Нет названия"
    try:
        cursor.execute(
            "SELECT group_username FROM groups WHERE user_id = ? AND group_id = ?",
            (user_id, group_id)
        )
        group_row = cursor.fetchone()
        if group_row:
            group_name = group_row[0]
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
                       f"broadcasttextinterval_{user_id}_{gid_key(group_id)}")],
        [Button.inline("✅ Начать/возобновить рассылку",
                       f"startresumebroadcast_{user_id}_{gid_key(group_id)}")],
        [Button.inline("⛔ Остановить рассылку",
                       f"stop_accountbroadcast_{user_id}_{gid_key(group_id)}")]
    ]

    # ---------- ответ ----------
    await event.respond(
        f"📢 **Меню рассылки для группы {group_name} "
        f"от аккаунта {account_name}:**\n\n"
        f"{text_display}\n"
        f"🟢 **Статус рассылки:** {status}",
        buttons=keyboard
    )


@bot.on(CallbackQuery(data=lambda data: data.decode().startswith("broadcasttextinterval_")))
async def handle_broadcast_text_interval(event: __CallbackQuery) -> None:
    data = event.data.decode()
    user_id, group_id = map(int, data.split("_")[1:])
    async with bot.conversation(event.sender_id) as conv:
        user_states[event.sender_id] = "text_and_interval_waiting"
        cursor = conn.cursor()
        await event.respond("📝 Пожалуйста, отправьте текст для рассылки.")
        new_broadcast_text_event = await conv.wait_event(events.NewMessage(from_users=event.sender_id))
        new_broadcast_text = new_broadcast_text_event.text

        await event.respond("⏳ Пожалуйста, отправьте интервал рассылки в минут.")
        try:
            new_interval_minutes_event = await conv.wait_event(events.NewMessage(from_users=event.sender_id))
            new_interval_minutes = int(new_interval_minutes_event.text)

            cursor.execute("SELECT * FROM broadcasts WHERE user_id = ? AND group_id = ?", (user_id, gid_key(group_id)))
            existing_row = cursor.fetchone()
            if existing_row:
                update_broadcast_data(user_id, gid_key(group_id), new_broadcast_text, new_interval_minutes)
                await event.respond(
                    f"✅ Текст рассылки успешно обновлен на: {new_broadcast_text}\n"
                    f"⏳ Интервал рассылки обновлен на {new_interval_minutes} минут.")
            else:
                create_broadcast_data(user_id, gid_key(group_id), new_broadcast_text, new_interval_minutes)
                await event.respond(
                    f"✅ Текст рассылки и интервал были успешно добавлены:\n{new_broadcast_text}\n⏳"
                    f" Интервал рассылки — {new_interval_minutes} минут.")

            del user_states[event.sender_id]

        except ValueError:
            await event.respond("⚠ Пожалуйста, введите корректное число минут для интервала.")

            del user_states[event.sender_id]
        finally:
            cursor.close()


@bot.on(CallbackQuery(data=lambda data: data.decode().startswith("startresumebroadcast_")))
async def start_resume_broadcast(event: __CallbackQuery) -> None:
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
    cursor = conn.cursor()
    job_id = f"broadcast_{user_id}_{gid_key(group_id)}"
    existing_job = scheduler.get_job(job_id)

    if existing_job:
        await event.respond("⚠ Рассылка уже активна для этой группы.")
        return

    cursor.execute("SELECT broadcast_text, interval_minutes FROM broadcasts WHERE user_id = ? AND group_id = ?",
                   (user_id, gid_key(group_id)))
    cursor.execute("""
        UPDATE broadcasts 
        SET is_active = ? 
        WHERE user_id = ? AND group_id = ?
    """, (True, user_id, gid_key(group_id)))
    conn.commit()
    row = cursor.execute("SELECT broadcast_text, interval_minutes FROM broadcasts WHERE user_id = ? AND group_id = ?",
                         (user_id, gid_key(group_id))).fetchone()

    if row:
        broadcast_text, interval_minutes = row
        if not broadcast_text or not interval_minutes or interval_minutes <= 0:
            await event.respond("⚠ Пожалуйста, убедитесь, что текст рассылки и корректный интервал установлены.")
            return

        session_string_row = cursor.execute("SELECT session_string FROM sessions WHERE user_id = ?",
                                            (user_id,)).fetchone()
        if not session_string_row:
            await event.respond("⚠ Ошибка: не найден session_string для аккаунта.")
            return

        session_string = session_string_row[0]
        session = StringSession(session_string)
        client = TelegramClient(session, API_ID, API_HASH)
        await client.connect()

        async def send_broadcast(cursors=cursor,
                                 user_ids=user_id,
                                 broadcast_texts=broadcast_text,
                                 clients=client):

            await clients.connect()
            username = cursors.execute(
                """SELECT group_username FROM groups WHERE user_id = ? AND group_id = ?""",
                (user_ids, group_id)).fetchone()[0]
            try:
                groups = await clients.get_entity(username)
            except Exception as error:
                logging.error(f"Ошибка {error}")
                return
            try:
                await clients.send_message(groups, broadcast_texts)
                cursors.execute(
                    """INSERT INTO send_history (
                                user_id, 
                                group_id, 
                                group_name, 
                                sent_at, 
                                message_text) 
                            VALUES (?, ?, ?, ?, ?)""",
                    (user_id, groups.id, groups.title if hasattr(groups, 'title') else '',
                     datetime.datetime.now().isoformat(),
                     broadcast_texts)
                )
                cursors.execute("UPDATE broadcasts SET is_active = ? WHERE user_id = ? AND group_id = ?",
                                (False, user_ids, gid_key(groups.id)))
            except Exception as error:
                logging.error(f"Ошибка отправки сообщения в группу: {error}")
                cursors.execute("UPDATE broadcasts SET is_active = ? WHERE user_id = ? AND group_id = ?",
                                (False, user_ids, gid_key(groups.id)))
            finally:
                await clients.disconnect()

        scheduler.add_job(
            send_broadcast,
            IntervalTrigger(minutes=interval_minutes),
            id=job_id,
            replace_existing=True
        )

        await event.respond(f"✅ Рассылка начата!")
        if not scheduler.running:
            scheduler.start()
    else:
        await event.respond("⚠ Рассылка еще не настроена для этой группы.")
    cursor.close()


@bot.on(CallbackQuery(data=lambda data: data.decode().startswith("stop_accountbroadcast_")))
async def stop_broadcast(event: __CallbackQuery) -> None:
    data = event.data.decode()
    try:
        user_id, group_id = map(int, data.split("_")[2:])
    except ValueError as e:
        await event.respond(f"⚠ Ошибка при извлечении user_id и group_id: {e}")
        return
    cursor = conn.cursor()
    session_string = cursor.execute("SELECT session_string FROM sessions WHERE user_id = ?", (user_id,)).fetchone()[0]
    session = StringSession(session_string)
    client = TelegramClient(session, API_ID, API_HASH)
    await client.connect()
    groups = cursor.execute("SELECT group_username FROM groups WHERE group_id = ?", (group_id,)).fetchone()[0]
    group = await client.get_entity(groups)
    job_id = f"broadcast_{user_id}_{groups}"
    job = scheduler.get_job(job_id)
    if job:
        job.remove()
        cursor.execute("UPDATE broadcasts SET is_active = ? WHERE user_id = ? AND group_id = ?",
                       (False, user_id, gid_key(groups)))
        conn.commit()
        await event.respond(f"⛔ Рассылка в группу **{group.title}** остановлена.")
    else:
        await event.respond(f"⚠ Рассылка в группу **{group.title}** не была запущена.")
    cursor.close()


@bot.on(CallbackQuery(data=lambda data: data.decode().startswith("StopBroadcastAll_")))
async def stop_broadcast_all(event: __CallbackQuery) -> None:
    data = event.data.decode()
    try:
        user_id = int(data.split("_")[1])
    except ValueError as e:
        await event.respond(f"⚠ Ошибка при извлечении user_id и group_id: {e}")
        return
    cursor = conn.cursor()
    session_string = cursor.execute("SELECT session_string FROM sessions WHERE user_id = ?", (user_id,)).fetchone()[0]
    session = StringSession(session_string)
    client = TelegramClient(session, API_ID, API_HASH)
    await client.connect()
    groups = cursor.execute("SELECT group_username, group_id FROM broadcasts WHERE user_id = ? AND is_active = ?",
                            (user_id, True))
    msg = ["⛔ **Остановленные рассылки**:\n\n"]
    for group_ in groups:
        job_id = f"broadcastALL_{user_id}_{gid_key(group_[0])}"
        job = scheduler.get_job(job_id)
        if job:
            job.remove()
            cursor.execute("UPDATE broadcasts SET is_active = ? WHERE user_id = ? AND group_id = ?",
                           (False, user_id, gid_key(group_[1])))
            conn.commit()
            msg.append(f"⛔ Рассылка в группу **{group_[0]}** остановлена.")
        else:
            msg.append(f"⚠ Рассылка в группу **{group_[0]}** не была запущена.")
    await event.respond("\n".join(msg))
    cursor.close()


@bot.on(CallbackQuery(data=b"delete_account"))
async def handle_delete_account(event: __CallbackQuery) -> None:
    user_sessions_phone[event.sender_id] = {"step": "awaiting_phone"}
    await event.respond("📲 Введите номер телефона аккаунта, который нужно удалить:")


@bot.on(CallbackQuery(func=lambda event: (user_state := user_sessions_phone.get(event.sender_id)) and user_state["step"] == "awaiting_phone"))
async def handle_user_input(event: __Message):
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


@bot.on(CallbackQuery(data=b"groups"))
async def manage_groups(event: __CallbackQuery) -> None:
    user_sessions[event.sender_id] = {"step": "awaiting_group_username"}
    await event.respond("📲 Напишите @username группы, чтобы добавить её в базу данных:")


@bot.on(events.NewMessage(func=lambda event: (user_state := user_sessions.pop(event.sender_id, None)) and user_state["step"] == "awaiting_group_username"))
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
        except sqlite3.IntegrityError:
            await event.respond("⚠ Эта группа уже существует в базе данных.")
        finally:
            cursor.close()
    else:
        await event.respond("⚠ Ошибка! Неправильный формат. Попробуйте снова, нажав кнопку.")


@bot.on(CallbackQuery(data=b"my_groups"))
async def my_groups(event: __CallbackQuery) -> None:
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


@bot.on(CallbackQuery(data=b"add_all_accounts_to_groups"))
async def add_all_accounts_to_groups(event: __CallbackQuery) -> None:
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


@bot.on(CallbackQuery(data=b"add_all_groups"))
async def add_all_accounts_to_groups(event: __CallbackQuery) -> None:
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


@bot.on(CallbackQuery(data=b"delete_group"))
async def handle_delete_group(event: __CallbackQuery) -> None:
    user_sessions_deleting[event.sender_id] = {"step": "awaiting_group_username"}
    await event.respond("📲 Введите @username группы, которую нужно удалить:")


@bot.on(NewMessage(func=lambda event: (user_state := user_sessions_deleting.get(event.sender_id)) and user_state["step"] == "awaiting_group_username"))
async def handle_user_input(event: __Message) -> None:
    group_username = event.text.strip()

    if group_username.startswith("@"):
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM groups WHERE group_username = ?", (group_username,))
        group = cursor.fetchone()

        if group:
            cursor.execute("DELETE FROM groups WHERE group_username = ?", (group_username,))
            conn.commit()
            await event.respond(f"✅ Группа {group_username} успешно удалена из базы данных!")
        else:
            await event.respond("⚠ Группа с именем {group_username} не найдена в базе данных.")

        user_sessions_deleting.pop(event.sender_id, None)
        cursor.close()
    else:
        await event.respond("⚠ Пожалуйста, введите корректный @username группы, начиная с '@'.")
        return


# ---------------------------------------------------------------
@bot.on(CallbackQuery(data=lambda data: data.decode().startswith("show_history")))
async def show_history(event: __CallbackQuery) -> None:
    cursor = conn.cursor()
    cursor.execute("""
            SELECT group_name, sent_at, message_text
            FROM send_history
            ORDER BY sent_at DESC
            LIMIT 10
        """)
    rows = cursor.fetchall()
    cursor.close()
    if not rows:
        await event.respond("❌ История рассылки пуста.")
        return
    msg = "🕗 **10 последних рассылок:**\n\n"
    num = 1
    for group_name, sent_at, message_text in rows:
        msg += f"📌№{num}, Группа - **{group_name}**\n🕓 Время - **{sent_at}**\n💬 Сообщение - **{message_text}**\n\n"
        num += 1
    await event.respond(msg)


# ---------------------------------------------------------------
if __name__ == "__main__":
    logging.info("🚀 Бот запущен...")
    bot.run_until_disconnected()
    end_cursor = conn.cursor()
    end_cursor.execute("""DELETE FROM broadcasts WHERE is_active = ?""", (True,))
    end_cursor.execute("""DELETE FROM broadcasts WHERE is_active = ?""", (False,))
    end_cursor.close()
    conn.close()
