import asyncio
import datetime
import logging
from typing import Union, Optional

from apscheduler.triggers.interval import IntervalTrigger
from telethon import Button, TelegramClient
from telethon.errors import ChatWriteForbiddenError, ChatAdminRequiredError, FloodWaitError, SlowModeWaitError
from telethon.sessions import StringSession
from telethon.tl.types import Channel, Chat

from config import callback_query, callback_message, API_ID, API_HASH, scheduler, Query, bot, conn, \
    New_Message, broadcast_all_state_account
from func.func import gid_key, create_broadcast_data, get_active_broadcast_groups


@bot.on(Query(data=lambda d: d.decode().startswith("broadcast_All_account")))
async def broadcast_all_menu(event: callback_query) -> None:
    keyboard = [
        [Button.inline("⏲️ Интервал во все группы", f"same_IntervalAll_account")],
        [Button.inline("🎲 Разный интервал (25-35)", f"diff_IntervalAll_account")]
    ]
    await event.respond("Выберите режим отправки:", buttons=keyboard)


# ---------- одинаковый интервал ----------
@bot.on(Query(data=lambda d: d.decode().startswith("same_IntervalAll_account")))
async def same_interval_start(event: callback_query) -> None:
    admin_id = event.sender_id
    broadcast_all_state_account[admin_id] = {"mode": "same", "step": "text"}
    await event.respond("📝 Пришлите текст рассылки для **всех** групп этого аккаунта:")


# ---------- случайный интервал ----------
@bot.on(Query(data=lambda d: d.decode().startswith("diff_IntervalAll_account")))
async def diff_interval_start(event: callback_query) -> None:
    admin_id = event.sender_id
    broadcast_all_state_account[admin_id] = {"mode": "diff", "step": "text"}
    await event.respond("📝 Пришлите текст рассылки, потом спрошу границы интервала:")


# ---------- мастер-диалог (текст → интервалы) ----------
@bot.on(New_Message(func=lambda e: e.sender_id in broadcast_all_state_account))
async def broadcast_all_dialog(event: callback_message) -> None:
    st = broadcast_all_state_account[event.sender_id]
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
        await schedule_all_accounts_broadcast(st["text"], min_time, None)
        await event.respond(f"✅ Запустил: каждые {min_time} мин.")
        broadcast_all_state_account.pop(event.sender_id, None)
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
        await schedule_all_accounts_broadcast(st["text"], st["min"], max_m)
        await event.respond(f"✅ Запустил: случайно каждые {st['min']}-{max_m} мин.")
        broadcast_all_state_account.pop(event.sender_id, None)


@bot.on(Query(data=lambda data: data.decode() == "Stop_Broadcast_All_account"))
async def stop_broadcast_all(event: callback_query) -> None:
    """Останавливает все активные рассылки для всех аккаунтов и групп"""
    msg_lines = ["⛔ **Остановленные рассылки**:\n\n"]

    with conn:
        cursor = conn.cursor()
        try:
            sessions = cursor.execute("SELECT user_id, session_string FROM sessions").fetchall()

            for user_id, session_string in sessions:
                async with TelegramClient(StringSession(session_string), API_ID, API_HASH) as client:
                    try:
                        await client.connect()
                        account = await client.get_me()
                        username = getattr(account, 'username', 'без username')
                        msg_user = [f"**Аккаунт {username}**:\n"]

                        active_groups = get_active_broadcast_groups(user_id)

                        if not active_groups:
                            msg_user.append("Нет активных рассылок.\n")
                            continue

                        for group_id in active_groups:
                            job_id = f"broadcastALL_{user_id}_{gid_key(group_id)}"

                            if scheduler.get_job(job_id):
                                scheduler.remove_job(job_id)
                                cursor.execute(
                                    "UPDATE broadcasts SET is_active = ? WHERE user_id = ? AND group_id = ?",
                                    (False, user_id, gid_key(group_id)))
                                msg_user.append(f"⛔ Рассылка в группу **{group_id}** остановлена.\n")
                            else:
                                msg_user.append(f"⚠ Рассылка в группу **{group_id}** не была запущена.\n")

                            msg_lines.extend(msg_user)

                    except Exception as e:
                        logging.error(f"Ошибка при обработке аккаунта {user_id}: {str(e)}")
                        msg_lines.append(f"⚠ Ошибка при обработке аккаунта {user_id}\n")

            await event.respond("".join(msg_lines))

        finally:
            cursor.close()


async def schedule_all_accounts_broadcast(text: str,
                                          min_m: int,
                                          max_m: Optional[int] = None) -> None:
    """Планирует/обновляет задачи рассылки broadcastALL_<user>_<gid> только для чатов,
    куда пользователь действительно может писать."""

    with conn:
        cursor = conn.cursor()
        try:
            users = cursor.execute("SELECT user_id, session_string FROM sessions").fetchall()

            for user_id, session_string in users:
                cursor.execute("""UPDATE broadcasts SET broadcast_text = ? WHERE user_id = ?""",
                               (text, user_id))

                async with TelegramClient(StringSession(session_string), API_ID, API_HASH) as client:
                    await client.connect()

                    groups = cursor.execute("""SELECT group_username, group_id FROM groups 
                                            WHERE user_id = ?""", (user_id,)).fetchall()

                    ok_entities: list[Channel | Chat] = []
                    for group_username, group_id in groups:
                        try:
                            ent = await client.get_entity(group_username)

                            # Проверяем тип чата
                            if not isinstance(ent, (Channel, Chat)):
                                logging.info(f"Пропускаем {ent} - не чат/канал")
                                continue

                            # Пропускаем каналы-витрины
                            if isinstance(ent, Channel) and ent.broadcast and not ent.megagroup:
                                logging.info(f"Пропускаем {ent} - витрина-канал")
                                continue

                            ok_entities.append(ent)
                        except Exception as error:
                            logging.warning(f"Не смог проверить {group_username}: {error}")
                            continue

                    if not ok_entities:
                        continue

                    total_entities = len(ok_entities)
                    sec_run = ((max_m - min_m) / total_entities) if max_m else min_m
                    current_time = sec_run

                    for ent in ok_entities:
                        job_id = f"broadcastALL_{user_id}_{gid_key(ent.id)}"
                        interval = ((max_m - min_m) / total_entities) if max_m else min_m

                        create_broadcast_data(user_id, gid_key(ent.id), text, interval)

                        if scheduler.get_job(job_id):
                            scheduler.remove_job(job_id)

                        async def send_message(
                                ss: str = session_string,
                                entity: Union[Channel, Chat] = ent,
                                jobs_id: str = job_id,
                                start_text: str = text,
                                max_retries: int = 10
                        ) -> None:
                            """Отправляет сообщение с обработкой ошибок и повторными попытками."""
                            retry_count = 0

                            while retry_count < max_retries:
                                try:
                                    async with TelegramClient(StringSession(ss), API_ID, API_HASH) as client:
                                        async with conn:
                                            cursor = conn.cursor()
                                            cursor.execute("""SELECT broadcast_text FROM broadcasts 
                                                            WHERE group_id = ? AND user_id = ?""",
                                                           (entity.id, user_id))
                                            txt = cursor.fetchone()
                                            txt = txt[0] if txt else start_text

                                            await client.send_message(entity, txt)
                                            logging.info(f"Успешно отправлено в {entity.title}")

                                            cursor.execute("""INSERT INTO send_history 
                                                            (user_id, group_id, group_name, sent_at, message_text) 
                                                            VALUES (?, ?, ?, ?, ?)""",
                                                           (user_id, entity.id, getattr(entity, 'title', ''),
                                                            datetime.datetime.now().isoformat(), txt))
                                except (ChatWriteForbiddenError, ChatAdminRequiredError) as e:
                                    logging.error(f"Нет прав писать в {entity.title}: {e}")
                                    break
                                except (FloodWaitError, SlowModeWaitError) as e:
                                    wait_time = e.seconds
                                    logging.warning(f"{type(e).__name__}: ожидание {wait_time} сек.")
                                    await asyncio.sleep(wait_time + 10)
                                    retry_count += 1
                                except Exception as e:
                                    logging.error(f"Ошибка при отправке в {entity.title}: {type(e).__name__}: {e}")
                                    retry_count += 1
                                    await asyncio.sleep(5)
                                else:
                                    return

                            logging.warning(f"Не удалось отправить в {entity.title} после {max_retries} попыток")
                            async with conn:
                                cursor = conn.cursor()
                                cursor.execute("""UPDATE broadcasts 
                                                SET is_active = ? 
                                                WHERE user_id = ? AND group_id = ?""",
                                               (False, user_id, entity.id))
                                if scheduler.get_job(jobs_id):
                                    scheduler.remove_job(jobs_id)

                        base = (min_m + max_m) // 2 if max_m else min_m
                        jitter = (max_m - min_m) * 60 // 2 if max_m else min_m * 30
                        trigger = IntervalTrigger(minutes=base, jitter=jitter)
                        next_run = datetime.datetime.now() + datetime.timedelta(minutes=current_time)

                        logging.info(f"Добавляем задачу для {ent.title} на {next_run.isoformat()}")
                        scheduler.add_job(
                            send_message,
                            trigger,
                            id=job_id,
                            next_run_time=next_run,
                            replace_existing=True,
                        )
                        current_time += sec_run
        finally:
            cursor.close()

    if not scheduler.running:
        logging.info("Запускаем планировщик задач")
        scheduler.start()
