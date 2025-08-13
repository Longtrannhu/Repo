import os
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

def fetch_updates(offset=None):
    params = {}
    if offset is not None:
        params["offset"] = offset
    params["allowed_updates"] = ["message"]
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

def iso_local(ts):
    dt = datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(VN_TZ)
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def collect_once():
    last_id, rec_id = get_last_update_id()
    updates = fetch_updates(offset=last_id + 1 if last_id else None)
    if not updates:
        print("No new updates.")
        return 0

    max_update_id = last_id
    created = 0

    for upd in updates:
        max_update_id = max(max_update_id, upd["update_id"])
        msg = upd.get("message")
        if not msg:
            continue
        if str(msg["chat"]["id"]) != str(GROUP_ID):
            continue

        user = msg.get("from", {})
        user_id = str(user.get("id", ""))
        username = user.get("username") or f"{user.get('first_name','')} {user.get('last_name','')}".strip() or ""
        text = msg.get("text") or "<non-text message>"
        ts_local = iso_local(msg["date"])

        tbl_messages.create({
            "DateTime": ts_local,
            "UserID": user_id,
            "Username": username,
            "Message": text
        })
        created += 1

    if max_update_id > last_id:
        set_last_update_id(max_update_id, rec_id)

    print(f"Collected {created} messages.")
    return created

def send_daily_report():
    # đếm UserID duy nhất của NGÀY HÔM NAY (giờ VN)
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
