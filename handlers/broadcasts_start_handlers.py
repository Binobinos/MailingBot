import datetime
import logging

from apscheduler.triggers.interval import IntervalTrigger
from telethon import TelegramClient
from telethon.sessions import StringSession

from config import callback_query, API_ID, API_HASH, scheduler, user_states, Query, bot, conn, New_Message
from func.func import create_broadcast_data, gid_key


@bot.on(Query(data=lambda data: data.decode().startswith("BroadcastTextInterval_")))
async def handle_broadcast_text_interval(event: callback_query) -> None:
    data = event.data.decode()
    user_id, group_id = map(int, data.split("_")[1:])
    async with bot.conversation(event.sender_id) as conv:
        user_states[event.sender_id] = "text_and_interval_waiting"
        cursor = conn.cursor()
        await event.respond("📝 Пожалуйста, отправьте текст для рассылки.")
        new_broadcast_text_event = await conv.wait_event(New_Message(from_users=event.sender_id))
        new_broadcast_text = new_broadcast_text_event.text

        await event.respond("⏳ Пожалуйста, отправьте интервал рассылки в минут.")
        try:
            new_interval_minutes_event = await conv.wait_event(New_Message(from_users=event.sender_id))
            new_interval_minutes = int(new_interval_minutes_event.text)
            cursor.execute("SELECT * FROM broadcasts WHERE user_id = ? AND group_id = ?",
                           (user_id, gid_key(group_id)))
            exists = cursor.fetchone()
            if exists:
                cursor.execute("""UPDATE broadcasts SET 
                                                interval_minutes = ?,
                                                broadcast_text = ? WHERE user_id = ? AND group_id = ?""",
                               (new_interval_minutes, new_broadcast_text, user_id, group_id))
                conn.commit()
            else:
                cursor.execute("""INSERT INTO broadcasts  
                                            (user_id, group_id, broadcast_text, interval_minutes, is_active)
                                        VALUES (?, ?, ?, ?, ?)""",
                               (user_id, group_id, new_broadcast_text, new_interval_minutes, False))
                conn.commit()
            await event.respond(
                f"✅ Текст рассылки успешно обновлен на: {new_broadcast_text}\n"
                f"⏳ Интервал рассылки обновлен на {new_interval_minutes} минут.")

            del user_states[event.sender_id]

        except ValueError:
            await event.respond("⚠ Пожалуйста, введите корректное число минут для интервала.")

            del user_states[event.sender_id]
        finally:
            cursor.close()


@bot.on(Query(data=lambda data: data.decode().startswith("StartResumeBroadcast_")))
async def start_resume_broadcast(event: callback_query) -> None:
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

    cursor.execute("""
                SELECT broadcast_text, interval_minutes 
                FROM broadcasts 
                WHERE user_id = ? AND group_id = ?
            """, (user_id, gid_key(group_id)))
    row = cursor.fetchone()

    if not row:
        await event.respond("⚠ Рассылка еще не настроена для этой группы.")
        return
    broadcast_text, interval_minutes = row
    if not broadcast_text or not interval_minutes or interval_minutes <= 0:
        await event.respond("⚠ Пожалуйста, убедитесь, что текст рассылки и корректный интервал установлены.")
        return
    session_string_row = cursor.execute("SELECT session_string FROM sessions WHERE user_id = ?",
                                        (user_id,)).fetchone()
    if not session_string_row:
        await event.respond("⚠ Ошибка: не найден session_string для аккаунта.")
        return
    cursor.execute("""
        UPDATE broadcasts 
        SET is_active = ? 
        WHERE user_id = ? AND group_id = ?
    """, (True, user_id, gid_key(group_id)))
    conn.commit()
    session_string = session_string_row[0]
    session = StringSession(session_string)
    client = TelegramClient(session, API_ID, API_HASH)
    await client.connect()

    async def send_broadcast(
                             user_ids=user_id,
                             broadcast_texts=broadcast_text,
                             clients=client):

        await clients.connect()
        cursors = conn.cursor()
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
    cursor.close()


@bot.on(Query(data=lambda data: data.decode().startswith("StopAccountBroadcast_")))
async def stop_broadcast(event: callback_query) -> None:
    data = event.data.decode()
    try:
        user_id, group_id = map(int, data.split("_")[1:])
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
    job_id = f"broadcast_{user_id}_{group_id}"
    job = scheduler.get_job(job_id)
    if job:
        job.remove()
        cursor.execute("UPDATE broadcasts SET is_active = ? WHERE user_id = ? AND group_id = ?",
                       (False, user_id, gid_key(group_id)))
        conn.commit()
        await event.respond(f"⛔ Рассылка в группу **{group.title}** остановлена.")
    else:
        await event.respond(f"⚠ Рассылка в группу **{group.title}** не была запущена.")
    cursor.close()
