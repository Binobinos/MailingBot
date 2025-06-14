from config import callback_query, Query, bot, conn


@bot.on(Query(data=lambda data: data.decode().startswith("show_history")))
async def show_history(event: callback_query) -> None:
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
