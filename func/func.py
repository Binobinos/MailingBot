from typing import List

from main import conn


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