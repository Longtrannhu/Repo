import os
import re
import json
import requests
from datetime import datetime, timezone
from dateutil import tz
from pyairtable import Table
from pyairtable.formulas import match

# ==== ENV ====
BOT_TOKEN = os.getenv("BOT_TOKEN")
GROUP_ID = os.getenv("GROUP_ID")  # ví dụ: -1001234567890
AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
TBL_MESSAGES = os.getenv("AIRTABLE_TABLE_MESSAGES", "Messages")
TBL_META = os.getenv("AIRTABLE_TABLE_META", "Meta")

VN_TZ = tz.gettz("Asia/Ho_Chi_Minh")

if not all([BOT_TOKEN, GROUP_ID, AIRTABLE_TOKEN, AIRTABLE_BASE_ID, TBL_MESSAGES, TBL_META]):
    raise RuntimeError("Missing required environment variables.")

# ==== Airtable tables ====
tbl_messages = Table(AIRTABLE_TOKEN, AIRTABLE_BASE_ID, TBL_MESSAGES)
tbl_meta = Table(AIRTABLE_TOKEN, AIRTABLE_BASE_ID, TBL_META)

# ---------- Helpers ----------
def get_last_update_id():
    recs = tbl_meta.all(formula=match({"key": "last_update_id"}), page_size=1)
    if recs:
        return int(recs[0]["fields"].get("value", "0") or "0"), recs[0]["id"]
    rec = tbl_meta.create({"key": "last_update_id", "value": "0"})
    return 0, rec["id"]

def set_last_update_id(value, rec_id=None):
    if rec_id:
        tbl_meta.update(rec_id, {"value": str(value)})
    else:
        recs = tbl_meta.all(formula=match({"key": "last_update_id"}), page_size=1)
        if recs:
            tbl_meta.update(recs[0]["id"], {"value": str(value)})
        else:
            tbl_meta.create({"key": "last_update_id", "value": str(value)})

def delete_webhook():
    try:
        requests.get(
            f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook",
            params={"drop_pending_updates": False}, timeout=15
        )
    except Exception:
        pass

def fetch_updates(offset=None):
    params = {}
    if offset is not None:
        params["offset"] = offset
    # gửi allowed_updates đúng chuẩn JSON
    params["allowed_updates"] = json.dumps(["message"])
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    if not data.get("ok"):
        raise RuntimeError(f"Telegram getUpdates error: {data}")
    return data["result"]

def send_message(text):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    r = requests.get(url, params={"chat_id": GROUP_ID, "text": text}, timeout=30)
    r.raise_for_status()

def reply_to(msg, text):
    """Reply ngay dưới tin nhắn gốc."""
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    params = {
        "chat_id": msg["chat"]["id"],
        "text": text,
        "reply_to_message_id": msg["message_id"],
        "allow_sending_without_reply": True,
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()

def iso_local(ts):
    dt = datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(VN_TZ)
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def get_message_text(msg):
    # ưu tiên text, sau đó caption; nếu không có -> chuỗi rỗng
    return msg.get("text") or msg.get("caption") or ""

def is_service_message(msg: dict) -> bool:
    service_keys = [
        "new_chat_members","left_chat_member","new_chat_title","new_chat_photo",
        "delete_chat_photo","group_chat_created","supergroup_chat_created",
        "migrate_to_chat_id","migrate_from_chat_id","pinned_message"
    ]
    return any(k in msg for k in service_keys)

# ---- VALIDATION: 8 số + " - " + chữ (cho phép kèm số & khoảng trắng ở phần chữ) ----
def is_valid_report(text: str) -> bool:
    """
    Hợp lệ khi:
      - Bắt đầu đúng 8 chữ số
      - theo sau là ' - ' (dấu cách, gạch ngang, dấu cách)
      - phần sau chứa chữ cái (có thể có số và khoảng trắng). Yêu cầu có ÍT NHẤT 1 chữ cái.
    Ví dụ hợp lệ:
      23082025 - Kho Mien Dong
      23082025 - Mien Dong 01
    """
    if not text:
        return False
    m = re.match(r'^\s*(\d{8})\s-\s(.+?)\s*$', text)
    if not m:
        return False
    tail = m.group(2)
    # Cho phép chữ, số, khoảng trắng; phải có ít nhất một chữ (isalpha) để tránh toàn số.
    return any(ch.isalpha() for ch in tail) and all(ch.isalpha() or ch.isdigit() or ch.isspace() for ch in tail)

# ---------- Main ----------
def collect_once():
    delete_webhook()
    last_id, rec_id = get_last_update_id()
    updates = fetch_updates(offset=last_id + 1 if last_id else None)
    if not updates:
        print("No new updates.")
        return 0

    max_update_id = last_id
    created = 0

    # Tách tin nhắn thường và album (media_group_id)
    normal_msgs = []
    media_groups = {}  # mgid -> list[message]

    for upd in updates:
        max_update_id = max(max_update_id, upd["update_id"])
        msg = upd.get("message")
        if not msg:
            continue
        if str(msg["chat"]["id"]) != str(GROUP_ID):
            continue
        frm = msg.get("from", {}) or {}
        if frm.get("is_bot") or is_service_message(msg):
            continue

        mgid = msg.get("media_group_id")
        if mgid:
            media_groups.setdefault(mgid, []).append(msg)
        else:
            normal_msgs.append(msg)

    # Xử lý tin nhắn thường (text / 1 ảnh)
    for msg in normal_msgs:
        frm = msg.get("from", {}) or {}
        user_id = str(frm.get("id", ""))
        username = frm.get("username") or f"{frm.get('first_name','')} {frm.get('last_name','')}".strip() or ""
        text = get_message_text(msg)
        ts_local = iso_local(msg["date"])

        if is_valid_report(text):
            tbl_messages.create({
                "DateTime": ts_local,
                "UserID": user_id,
                "Username": username,
                "Message": text
            })
            created += 1
            ack = "Đã ghi nhận báo cáo 5s ngày hôm nay"
        else:
            ack = "Kiểm tra lại format và gửi báo cáo lại"

        try:
            reply_to(msg, ack)
        except Exception as e:
            print("Ack failed:", e)

    # Xử lý album nhiều ảnh: chỉ reply/lưu 1 lần theo caption của album
    for mgid, msgs in media_groups.items():
        # Chọn message đại diện: ưu tiên cái có caption; nếu không có thì lấy cái đầu
        rep = None
        caption = ""
        for m in msgs:
            cap = m.get("caption") or ""
            if cap:
                rep = m
                caption = cap
                break
        if rep is None:
            rep = msgs[0]  # reply vào ảnh đầu tiên nếu không có caption
            caption = ""   # không caption => coi là sai format

        frm = rep.get("from", {}) or {}
        user_id = str(frm.get("id", ""))
        username = frm.get("username") or f"{frm.get('first_name','')} {frm.get('last_name','')}".strip() or ""
        ts_local = iso_local(rep["date"])

        if is_valid_report(caption):
            tbl_messages.create({
                "DateTime": ts_local,
                "UserID": user_id,
                "Username": username,
                "Message": caption
            })
            created += 1
            ack = "Đã ghi nhận báo cáo 5s ngày hôm nay"
        else:
            ack = "Kiểm tra lại format và gửi báo cáo lại"

        try:
            reply_to(rep, ack)
        except Exception as e:
            print("Ack failed:", e)

    if max_update_id > last_id:
        set_last_update_id(max_update_id, rec_id)

    print(f"Collected {created} messages.")
    return created

def send_daily_report():
    # Đếm UserID duy nhất của NGÀY HÔM NAY (giờ VN) trong các bản ghi HỢP LỆ
    today_prefix = datetime.now(VN_TZ).strftime("%Y-%m-%d")
    formula = f"SEARCH('{today_prefix}', {{DateTime}})"
    users = set()
    for rec in tbl_messages.iterate(formula=formula, page_size=100):
        fields = rec.get("fields", {})
        uid = fields.get("UserID")
        if uid:
            users.add(uid)
    count = len(users)
    send_message(f"Hôm nay có {count} tài khoản đã gửi tin nhắn trong nhóm.")
    print(f"Report sent. Unique users today: {count}")

if __name__ == "__main__":
    mode = os.getenv("MODE", "collect")  # "collect" hoặc "report"
    if mode == "collect":
        collect_once()
    elif mode == "report":
        send_daily_report()
    else:
        raise SystemExit("Unknown MODE. Use collect or report.")
