import asyncio
import datetime
import logging
from typing import Union

from apscheduler.triggers.interval import IntervalTrigger
from telethon import Button, TelegramClient
from telethon.errors import ChatWriteForbiddenError, ChatAdminRequiredError, FloodWaitError, SlowModeWaitError
from telethon.sessions import StringSession
from telethon.tl.functions.messages import SendMessageRequest
from telethon.tl.types import Channel, Chat

from config import callback_query, callback_message, broadcast_all_state, API_ID, API_HASH, scheduler, Query, bot, conn, \
    New_Message
from func.func import gid_key, create_broadcast_data, get_active_broadcast_groups


@bot.on(Query(data=lambda d: d.decode().startswith("broadcastAll_")))
async def broadcast_all_menu(event: callback_query) -> None:
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
@bot.on(Query(data=lambda d: d.decode().startswith("sameIntervalAll_")))
async def same_interval_start(event: callback_query) -> None:
    admin_id = event.sender_id
    uid = int(event.data.decode().split("_")[1])
    broadcast_all_state[admin_id] = {"user_id": uid, "mode": "same", "step": "text"}
    await event.respond("📝 Пришлите текст рассылки для **всех** групп этого аккаунта:")


# ---------- случайный интервал ----------
@bot.on(Query(data=lambda d: d.decode().startswith("diffIntervalAll_")))
async def diff_interval_start(event: callback_query) -> None:
    admin_id = event.sender_id
    uid = int(event.data.decode().split("_")[1])
    broadcast_all_state[admin_id] = {"user_id": uid, "mode": "diff", "step": "text"}
    await event.respond("📝 Пришлите текст рассылки, потом спрошу границы интервала:")


# ---------- мастер-диалог (текст → интервалы) ----------
@bot.on(New_Message(func=lambda e: e.sender_id in broadcast_all_state))
async def broadcast_all_dialog(event: callback_message) -> None:
    st = broadcast_all_state[event.sender_id]
    print(event, type(event))
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
            min_time = int(event.text)
        except ValueError:
            await event.respond(f"Некорректный формат числа. попробуйте еще раз нажав /start")
            return
        if min_time <= 0:
            await event.respond("⚠ Должно быть положительное число.")
            return
        await schedule_account_broadcast(int(st["user_id"]), st["text"], min_time, None)
        await event.respond(f"✅ Запустил: каждые {min_time} мин.")
        broadcast_all_state.pop(event.sender_id, None)
        return

    # случайный интервал — шаг 2 (min)
    if st["mode"] == "diff" and st["step"] == "min":
        try:
            st["min"] = int(event.text)
        except ValueError:
            await event.respond(f"Некорректный формат числа. попробуйте еще раз нажав /start")
            return
        if st["min"] <= 0:
            await event.respond("⚠ Минимальное число должно быть больше нуля.")
            return
        st["step"] = "max"
        await event.respond("🔢 Максимальный интервал (мин):")
        return

    # случайный интервал — шаг 3 (max) + запуск
    if st["mode"] == "diff" and st["step"] == "max":
        try:
            max_m = int(event.text)
        except ValueError:
            await event.respond(f"Некорректный формат числа. попробуйте еще раз нажав /start")
            return
        if max_m <= st["min"]:
            await event.respond("⚠ Максимальное число должно быть больше минимального числа.")
            return
        await schedule_account_broadcast(st["user_id"], st["text"], st["min"], max_m)
        await event.respond(f"✅ Запустил: случайно каждые {st['min']}-{max_m} мин.")
        broadcast_all_state.pop(event.sender_id, None)


@bot.on(Query(data=lambda data: data.decode().startswith("StopBroadcastAll_")))
async def stop_broadcast_all(event: callback_query) -> None:
    data = event.data.decode()
    try:
        user_id = int(data.split("_")[1])
    except ValueError as e:
        await event.respond(f"⚠ Ошибка при извлечении user_id и group_id: {e}")
        return
    cursor = conn.cursor()
    msg = ["⛔ **Остановленные рассылки**:\n\n"]
    for group_ in get_active_broadcast_groups(user_id):
        job_id = f"broadcastALL_{user_id}_{gid_key(group_)}"
        if scheduler.get_job(job_id):
            logging.info(f"Работа {job_id} найдена")
            scheduler.remove_job(job_id)
            cursor.execute("UPDATE broadcasts SET is_active = ? WHERE group_id = ?",
                           (False, gid_key(group_)))
            conn.commit()
            msg.append(f"⛔ Рассылка в группу **{group_}** остановлена.")
        else:
            logging.info(f"Работа {job_id} не найдена")
            msg.append(f"⚠ Рассылка в группу **{group_}** не была запущена.")
    await event.respond("\n".join(msg))
    cursor.close()


async def schedule_account_broadcast(user_id: int,
                                     text: str,
                                     min_m: int,
                                     max_m: Union[int] = None) -> None:
    """Ставит/обновляет jobs broadcastALL_<user>_<gid> только для чатов,
    куда аккаунт реально может писать."""
    # --- сессия ---
    cursor = conn.cursor()
    row = cursor.execute("SELECT session_string FROM sessions WHERE user_id = ?", (user_id,)).fetchone()
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

    sec_run = (((max_m - min_m) / len(ok_entities)) if max_m else min_m)
    current_time = sec_run
    for ent in ok_entities:
        logging.debug(ent)
        job_id = f"broadcastALL_{user_id}_{gid_key(ent.id)}"
        create_broadcast_data(user_id, gid_key(ent.id), text,
                              (((max_m - min_m) / len(ok_entities)) if max_m else min_m))
        if scheduler.get_job(job_id):
            logging.info(f"Удаляем задачу")
            scheduler.remove_job(job_id)

        async def send_message(
                ss: str = sess_str,
                entity: Union[Channel, Chat] = ent,
                jobs_id: str = job_id,
                start_text: str = text,
                max_retries: int = 10
        ) -> None:
            """Отправляет сообщение с обработкой ошибок и повторными попытками."""
            retry_count = 0
            cursor = None

            while retry_count < max_retries:
                try:
                    async with TelegramClient(StringSession(ss), API_ID, API_HASH) as client:
                        cursor = conn.cursor()

                        cursor.execute("""SELECT broadcast_text FROM broadcasts 
                                        WHERE group_id = ? AND user_id = ?""",
                                       (entity.id, user_id))
                        txt = cursor.fetchone()
                        txt = txt[0] if txt else start_text

                        await client.send_message(entity, txt)
                        logging.info(f"Отправлено: {txt} в {entity.title} (@{entity.username})")

                        cursor.execute("""INSERT INTO send_history 
                                        (user_id, group_id, group_name, sent_at, message_text) 
                                        VALUES (?, ?, ?, ?, ?)""",
                                       (user_id,
                                        entity.id,
                                        getattr(entity, 'title', ''),
                                        datetime.datetime.now().isoformat(),
                                        txt))
                        conn.commit()
                        return

                except (ChatWriteForbiddenError, ChatAdminRequiredError) as e:
                    logging.error(f"Ошибка при отправке в {entity.title} нет прав писать: {e} {type(e)}")
                    retry_count += 1
                except (FloodWaitError, SlowModeWaitError) as e:
                    wait_time = e.seconds
                    logging.warning(f"{type(e)}: ждем {wait_time} сек. (Попытка {retry_count + 1}/{max_retries})")
                    await asyncio.sleep(wait_time + 10)
                    retry_count += 1

                except Exception as e:
                    logging.error(f"Ошибка при отправке в {entity.title}: {e} {type(e)}")
                    retry_count += 1
                    await asyncio.sleep(5)

                finally:
                    if cursor:
                        cursor.close()

            logging.warning(f"Не удалось отправить в {entity.title} после {max_retries} попыток")
            cursor = conn.cursor()
            try:
                cursor.execute("""UPDATE broadcasts 
                                 SET is_active = ? 
                                 WHERE user_id = ? AND group_id = ?""",
                               (False, user_id, entity.id))
                conn.commit()
                if scheduler.get_job(jobs_id):
                    scheduler.remove_job(jobs_id)
            finally:
                cursor.close()

        base = (min_m + max_m) // 2 if max_m else min_m
        jitter = (max_m - min_m) * 60 // 2 if max_m else 0
        trigger = IntervalTrigger(minutes=base, jitter=jitter)
        next_run = datetime.datetime.now() + datetime.timedelta(minutes=current_time)
        logging.info(f"Добавляем задачу отправить сообщения в {ent.title} в {next_run.isoformat()}")
        scheduler.print_jobs()
        scheduler.add_job(
            send_message,
            trigger,
            id=job_id,
            next_run_time=next_run,
            replace_existing=True,
        )
        current_time += sec_run
    if not scheduler.running:
        logging.info("Запускаем все задачи")
        scheduler.start()

    await client.disconnect()
    if not ok_entities:
        return
