# bot.py
import os, re, time, datetime
from typing import List, Dict, Any
import pytz
import requests
from pyairtable import Table

# ================== ENV & CONST ==================
TELEGRAM_BOT_TOKEN  = os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("BOT_TOKEN")
TELEGRAM_CHAT_ID    = str(os.getenv("TELEGRAM_CHAT_ID") or os.getenv("GROUP_ID") or "").strip()

AIRTABLE_TOKEN      = os.getenv("AIRTABLE_TOKEN")
AIRTABLE_BASE_ID    = os.getenv("AIRTABLE_BASE_ID")
TBL_MESSAGES        = os.getenv("TBL_MESSAGES", "Messages")
TBL_META            = os.getenv("TBL_META", "Meta")
TBL_IMAGES          = os.getenv("TBL_IMAGES", "").strip()  # optional

# Tên cột trong Airtable (có thể override qua ENV để khớp schema hiện tại)
COL_MSG_TEXT        = os.getenv("COL_MSG_TEXT", "TextOrCaption")
COL_MSG_CODE        = os.getenv("COL_MSG_CODE", "Code")
COL_MSG_TS          = os.getenv("COL_MSG_TS", "Timestamp")              # nếu không có, sẽ dùng createdTime
COL_MSG_CHAT        = os.getenv("COL_MSG_CHAT", "ChatId")
COL_MSG_USER        = os.getenv("COL_MSG_USER", "From")
COL_MSG_TG_MSG_ID   = os.getenv("COL_MSG_TG_MSG_ID", "TelegramMessageId")

COL_META_CODE       = os.getenv("COL_META_CODE", "MaNoi")
COL_META_NAME       = os.getenv("COL_META_NAME", "TenNoi")
COL_META_KEY        = os.getenv("COL_META_KEY", "Key")
COL_META_VAL        = os.getenv("COL_META_VAL", "Value")

# Bảng IMAGES (nếu dùng)
COL_IMG_HASH        = os.getenv("COL_IMG_HASH", "FileUniqueId")
COL_IMG_CODE        = os.getenv("COL_IMG_CODE", "Code")
COL_IMG_DATE        = os.getenv("COL_IMG_DATE", "Date")

VN_TZ = pytz.timezone("Asia/Ho_Chi_Minh")
CODE_RE = re.compile(r"^(\d{8})\s*-\s*", re.UNICODE)

# Reply messages (đã chỉnh theo yêu cầu trước đó)
MSG_OK      = "🆗Đã ghi nhận báo cáo 5s ngày hôm nay"
MSG_BADFMT  = "🆕Kiểm tra lại format và gửi báo cáo lại"
MSG_DUPIMG  = "⛔️Ảnh gửi có dấu hiệu trùng với trước đây, nhờ kiểm tra lại"

# ================== Utils ==================
def _today_vn():
    return datetime.datetime.now(VN_TZ).date()

def _iso_local(dttm_str: str):
    try:
        return datetime.datetime.fromisoformat(dttm_str.replace("Z","+00:00")).astimezone(VN_TZ)
    except Exception:
        return None

def _air_table(name):
    return Table(AIRTABLE_TOKEN, AIRTABLE_BASE_ID, name)

def _tg(method: str, **kwargs):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}"
    r = requests.post(url, json=kwargs, timeout=30)
    r.raise_for_status()
    return r.json()

def _send_reply(chat_id: str, reply_to_message_id: int, text: str):
    return _tg("sendMessage",
               chat_id=chat_id,
               text=text,
               reply_to_message_id=reply_to_message_id,
               allow_sending_without_reply=True)

def _send_markdown(chat_id: str, text: str):
    return _tg("sendMessage",
               chat_id=chat_id,
               text=text,
               parse_mode="Markdown")

# ================== Meta KV in Airtable ==================
def _meta_get(key: str) -> str:
    tbl = _air_table(TBL_META)
    recs = tbl.all(formula=f"LOWER({{{COL_META_KEY}}})='{key.lower()}'".format(COL_META_KEY=COL_META_KEY))
    return recs[0]["fields"].get(COL_META_VAL, "") if recs else ""

def _meta_set(key: str, val: str):
    tbl = _air_table(TBL_META)
    recs = tbl.all(formula=f"LOWER({{{COL_META_KEY}}})='{key.lower()}'".format(COL_META_KEY=COL_META_KEY))
    if recs:
        tbl.update(recs[0]["id"], {COL_META_VAL: val})
    else:
        tbl.create({COL_META_KEY: key, COL_META_VAL: val})

# ================== Collect logic (15') ==================
def _extract_code(text: str) -> str:
    if not text:
        return ""
    m = CODE_RE.match(text.strip())
    return m.group(1) if m else ""

def _photo_unique_ids(photo_sizes: List[Dict[str,Any]]) -> List[str]:
    # Telegram trả mảng các size; mỗi size có file_unique_id
    ids = []
    for ph in photo_sizes or []:
        u = ph.get("file_unique_id")
        if u: ids.append(u)
    # lọc unique
    return list(dict.fromkeys(ids))

def _is_duplicate_photo(ids: List[str]) -> bool:
    if not TBL_IMAGES or not ids:
        return False
    tbl = _air_table(TBL_IMAGES)
    # nếu bất kỳ file_unique_id đã tồn tại -> coi là trùng
    for uid in ids:
        recs = tbl.all(formula=f"{{{COL_IMG_HASH}}} = '{uid}'")
        if recs:
            return True
    return False

def _save_photo_ids(code: str, ids: List[str]):
    if not TBL_IMAGES or not ids:
        return
    tbl = _air_table(TBL_IMAGES)
    today = _today_vn().isoformat()
    for uid in ids:
        tbl.create({COL_IMG_HASH: uid, COL_IMG_CODE: code, COL_IMG_DATE: today})

def _save_message(record: Dict[str,Any]):
    _air_table(TBL_MESSAGES).create(record)

def collect_once():
    """
    Lấy update từ Telegram bằng getUpdates, dùng offset lưu trong Meta.Key='last_update_id'.
    - Chỉ xử lý message thuộc group CHAT_ID bạn cấu hình.
    - Một caption có nhiều ảnh -> chỉ reply 1 lần (theo message_id).
    - Sai format -> reply MSG_BADFMT, KHÔNG ghi Airtable.
    - Đúng format -> kiểm trùng ảnh (nếu bật TBL_IMAGES). Nếu trùng -> reply MSG_DUPIMG, vẫn ghi nhận text hợp lệ? (theo yêu cầu trước đây: trùng ảnh thì cảnh báo, còn flow bình thường vẫn chạy)
    """
    offset = _meta_get("last_update_id")
    offset = int(offset) + 1 if offset else None

    resp = _tg("getUpdates", timeout=10, allowed_updates=["message"], offset=offset)
    updates = resp.get("result", [])

    last_id = None
    for u in updates:
        last_id = u.get("update_id", last_id)
        msg = u.get("message") or {}
        chat = msg.get("chat", {})
        chat_id = str(chat.get("id", ""))
        if not chat_id or chat_id != str(TELEGRAM_CHAT_ID):
            continue

        message_id = msg.get("message_id")
        text = msg.get("text", "")
        caption = msg.get("caption", "")
        photos = msg.get("photo", [])  # mảng photo sizes nếu có
        from_user = msg.get("from", {})
        sender = f'{from_user.get("first_name","")} {from_user.get("last_name","")}'.strip() or from_user.get("username","")

        content = caption if caption else text
        code = _extract_code(content)

        if not code:
            # Sai format -> reply và bỏ qua ghi Airtable
            _send_reply(chat_id, message_id, MSG_BADFMT)
            continue

        # Kiểm tra trùng ảnh nếu có ảnh
        photo_ids = _photo_unique_ids(photos)
        is_dup = _is_duplicate_photo(photo_ids)

        if is_dup:
            _send_reply(chat_id, message_id, MSG_DUPIMG)

        # Ghi nhận hợp lệ (chỉ 1 bản ghi cho cả message, kể cả nhiều ảnh)
        record = {
            COL_MSG_TEXT: content,
            COL_MSG_CODE: code,
            COL_MSG_CHAT: chat_id,
            COL_MSG_USER: sender,
            COL_MSG_TG_MSG_ID: message_id,
            COL_MSG_TS: datetime.datetime.now(VN_TZ).isoformat()
        }
        _save_message(record)
        _save_photo_ids(code, photo_ids)

        # Trả lời xác nhận (chỉ 1 lần cho message này)
        _send_reply(chat_id, message_id, MSG_OK)

    if last_id is not None:
        _meta_set("last_update_id", str(last_id))

# ================== Daily report 21h ==================
def _get_master_codes():
    tbl = _air_table(TBL_META)
    # Lấy tất cả MaNoi/TenNoi (KHÔNG lấy các dòng Meta dạng key-value)
    recs = tbl.all(fields=[COL_META_CODE, COL_META_NAME])
    codes, name_map = [], {}
    for r in recs:
        f = r.get("fields", {})
        code = str(f.get(COL_META_CODE, "")).strip()
        if code:  # chỉ tính những dòng có MaNoi
            codes.append(code)
            name_map[code] = str(f.get(COL_META_NAME, "")).strip()
    return codes, name_map

def _get_today_messages():
    tbl = _air_table(TBL_MESSAGES)
    recs = tbl.all()
    today = _today_vn()
    items = []
    for r in recs:
        f = r.get("fields", {})
        text = str(f.get(COL_MSG_TEXT, "")).strip()
        code = str(f.get(COL_MSG_CODE, "")).strip()
        ts   = f.get(COL_MSG_TS) or r.get("createdTime")
        ts_dt = _iso_local(ts) if isinstance(ts, str) else ts
        if not code or not ts_dt:
            # thử cứu code từ text
            if not code and text:
                m = CODE_RE.match(text)
                if m:
                    code = m.group(1)
                    ts_dt = _iso_local(str(ts)) if ts else None
                else:
                    continue
        if not ts_dt:
            continue
        if ts_dt.astimezone(VN_TZ).date() == today:
            items.append({"code": code, "text": text, "ts": ts_dt})
    return items

def _pick_latest_per_code(items):
    latest = {}
    for it in items:
        c = it["code"]
        if c not in latest or it["ts"] > latest[c]["ts"]:
            latest[c] = it
    # trả theo code để ổn định
    return [latest[c] for c in sorted(latest.keys())]

def _format_table(col1_list: List[str], col2_list: List[str]) -> str:
    head1 = "Text/Caption đã gửi"
    head2 = "Những nơi chưa gửi"
    col1 = col1_list[:] if col1_list else ["(trống)"]
    col2 = col2_list[:] if col2_list else ["(đủ)"]
    w1 = max(len(head1), max((len(x) for x in col1), default=0))
    w2 = max(len(head2), max((len(x) for x in col2), default=0))
    line = f"+{'-'*(w1+2)}+{'-'*(w2+2)}+"
    header = f"| {head1.ljust(w1)} | {head2.ljust(w2)} |"
    rows = []
    n = max(len(col1), len(col2))
    for i in range(n):
        c1 = col1[i] if i < len(col1) else ""
        c2 = col2[i] if i < len(col2) else ""
        rows.append(f"| {c1.ljust(w1)} | {c2.ljust(w2)} |")
    return "\n".join([line, header, line, *rows, line])

def run_daily_report():
    today_str = _today_vn().strftime("%d/%m/%Y")
    master_codes, name_map = _get_master_codes()
    items_today = _get_today_messages()

    latest = _pick_latest_per_code(items_today)
    sent_codes = {it["code"] for it in latest}

    # Cột 1: caption mới nhất / nơi
    col1_vals = []
    for it in latest:
        txt = (it["text"] or "").replace("\n", " ")
        if len(txt) > 120:
            txt = txt[:117] + "..."
        col1_vals.append(txt)

    # Cột 2: nơi chưa gửi
    missing = [c for c in master_codes if c not in sent_codes]
    col2_vals = [f"{c} - {name_map.get(c,'')}".strip().rstrip(" -") for c in missing]

    table = _format_table(col1_vals, col2_vals)
    msg = f"📊 *Báo cáo 21h* (ngày {today_str})\n```\n{table}\n```"
    _send_markdown(TELEGRAM_CHAT_ID, msg)

# ================== Main ==================
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--collect", action="store_true", help="Poll Telegram & record messages")
    parser.add_argument("--daily", action="store_true", help="Send 21h report")
    args = parser.parse_args()

    if args.daily:
        run_daily_report()
    else:
        # Mặc định collector để khớp workflow 15' (hoặc dùng --collect)
        collect_once()
