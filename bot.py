# bot.py (V6) — Daily 21:00 VN, Collector mỗi 15', phát hiện ảnh trùng theo file_unique_id
import os, re, sys, json, logging
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from typing import List, Dict, Optional, Tuple
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

from pyairtable import Api
from pyairtable.formulas import match

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("report-bot")

# ----- ENV -----
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")  # ID nhóm nhận báo cáo / auto-reply

AIRTABLE_TOKEN   = os.getenv("AIRTABLE_TOKEN")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
TBL_MESSAGES     = os.getenv("TBL_MESSAGES", "Messages")
TBL_META         = os.getenv("TBL_META", "Meta")
TBL_IMAGES       = os.getenv("TBL_IMAGES")  # (tuỳ chọn) bảng lưu dấu vết ảnh trùng; nếu không có sẽ dùng bảng Meta

api = Api(AIRTABLE_TOKEN) if AIRTABLE_TOKEN else None
tbl_messages = api.table(AIRTABLE_BASE_ID, TBL_MESSAGES) if api else None
tbl_meta     = api.table(AIRTABLE_BASE_ID, TBL_META) if api else None
tbl_images   = api.table(AIRTABLE_BASE_ID, TBL_IMAGES) if (api and TBL_IMAGES) else None  # có thì dùng

VN_TZ = timezone(timedelta(hours=7))
FORMAT_RE = re.compile(r"^\s*\d{8}\s*-\s*[^\s].+$", re.UNICODE)

# tránh reply nhiều lần cho 1 album (khi chạy realtime/collector trong 1 lần quét)
PROCESSED_MEDIA_GROUP_IDS = set()

# ===== Utils =====
def is_valid_format(text: str) -> bool:
    return bool(text and FORMAT_RE.match(text))

def now_vn_iso() -> str:
    return datetime.now(VN_TZ).isoformat(timespec="seconds")

def safe_get_fields(rec) -> dict:
    if isinstance(rec, dict):
        return rec.get("fields", {}) or {}
    if isinstance(rec, list):
        for r in rec:
            if isinstance(r, dict):
                return r.get("fields", {}) or {}
    return {}

def _parse_any_dt(s: str) -> Optional[datetime]:
    if not s:
        return None
    s = str(s).strip()
    try:
        if s.endswith("Z"):
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        return datetime.fromisoformat(s)
    except Exception:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S",
                "%d/%m/%Y %H:%M:%S", "%d-%m-%Y %H:%M:%S",
                "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=VN_TZ)
        except Exception:
            continue
    return None

def _pick_text(fields: dict) -> str:
    for k in ("text", "Text", "message", "Message", "caption", "Caption"):
        if k in fields and fields[k]:
            return str(fields[k])
    return ""

# ===== Airtable IO =====
def insert_message_record(chat_id, user_id, username, text, ok, msg_id, media_group_id):
    """Chỉ ghi khi ĐÚNG format; map theo schema bảng hiện tại: DateTime/UserID/Username/Message."""
    if not (tbl_messages and ok):
        return
    fields = {
        "DateTime": now_vn_iso(),
        "UserID": str(user_id),
        "Username": username or "",
        "Message": text or "",
        # Có thể bật thêm các cột bên dưới nếu bảng của bạn có:
        # "chat_id": str(chat_id),
        # "message_id": str(msg_id),
        # "media_group_id": str(media_group_id) if media_group_id else "",
        # "is_valid": True,
    }
    try:
        tbl_messages.create(fields)
    except Exception as e:
        log.error("Insert to Airtable failed: %s", e)

# --- Lưu/kiểm tra ảnh đã thấy trước đây ---
def images_seen(uids: List[str]) -> Tuple[bool, List[str]]:
    """
    Trả về (has_any, existed_ids) — true nếu có ít nhất 1 uid đã từng lưu.
    Ưu tiên dùng bảng TBL_IMAGES (nếu có). Nếu không, fallback lưu vào bảng Meta với key = 'img:<uid>'.
    """
    existed = []
    if tbl_images:
        for uid in set(uids):
            try:
                rec = tbl_images.first(formula=match({"file_unique_id": uid}))
                if rec:
                    existed.append(uid)
            except Exception as e:
                log.warning("images_seen check error: %s", e)
    elif tbl_meta:
        for uid in set(uids):
            try:
                rec = tbl_meta.first(formula=match({"key": f"img:{uid}"})) or tbl_meta.first(formula=match({"Key": f"img:{uid}"}))
                if rec:
                    existed.append(uid)
            except Exception as e:
                log.warning("meta images_seen error: %s", e)
    return (len(existed) > 0, existed)

def save_images_fingerprints(uids: List[str], chat_id, user_id, caption: str, media_group_id, message_id):
    """Lưu dấu vết ảnh để phát hiện trùng lần sau."""
    unique = list(set(uids))
    if tbl_images:
        for uid in unique:
            body = {
                "file_unique_id": uid,
                "DateTime": now_vn_iso(),
                "chat_id": str(chat_id),
                "user_id": str(user_id),
                "caption": caption or "",
                "media_group_id": str(media_group_id) if media_group_id else "",
                "message_id": str(message_id),
            }
            try:
                tbl_images.create(body)
            except Exception as e:
                log.warning("save_images_fingerprints error: %s", e)
    elif tbl_meta:
        for uid in unique:
            key = f"img:{uid}"
            try:
                rec = tbl_meta.first(formula=match({"key": key})) or tbl_meta.first(formula=match({"Key": key}))
                body = {"key": key, "value": now_vn_iso()}
                if rec:
                    tbl_meta.update(rec["id"], body, replace=False)
                else:
                    tbl_meta.create(body)
            except Exception as e:
                log.warning("meta save image error: %s", e)

# ===== Telegram HTTP =====
def _tg_api(method: str, payload: dict):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}"
    data = json.dumps(payload).encode("utf-8")
    req = Request(url, data=data, headers={"Content-Type": "application/json"})
    with urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))

def tg_send_message(chat_id, text, reply_to: Optional[int] = None):
    body = {"chat_id": chat_id, "text": text}
    if reply_to:
        body["reply_to_message_id"] = reply_to
    try:
        _tg_api("sendMessage", body)
    except HTTPError as e:
        log.error("Telegram HTTPError %s: %s", e.code, e.read().decode("utf-8", "ignore"))
    except URLError as e:
        log.error("Telegram URLError: %s", e)

def tg_get_updates(offset: Optional[int] = None):
    body = {"timeout": 0, "allowed_updates": ["message"]}
    if offset is not None:
        body["offset"] = offset
    try:
        return _tg_api("getUpdates", body)
    except Exception as e:
        log.error("getUpdates error: %s", e)
        return {"ok": False, "result": []}

# ===== Daily Report =====
def fetch_today_records() -> List[Dict]:
    if not tbl_messages:
        log.warning("Airtable not configured; cannot fetch today records")
        return []
    today_start = datetime.now(VN_TZ).replace(hour=0, minute=0, second=0, microsecond=0)
    today_end = today_start + timedelta(days=1)
    try:
        records = tbl_messages.all(page_size=100, max_records=1000)
    except Exception as e:
        log.error("Airtable fetch failed: %s", e)
        return []

    results = []
    for rec in records:
        f = safe_get_fields(rec)
        ts = f.get("created_at") or f.get("DateTime") or f.get("datetime") or f.get("Created At") or rec.get("createdTime")
        dt = _parse_any_dt(ts) if ts else None
        if not dt:
            continue
        if not dt.tzinfo:
            dt = dt.replace(tzinfo=VN_TZ)
        if today_start <= dt.astimezone(VN_TZ) < today_end:
            results.append(rec)
    return results

def build_summary(records: List[Dict]) -> str:
    total = 0
    by_chat = defaultdict(int)
    for rec in records:
        f = safe_get_fields(rec)
        ok = bool(f.get("is_valid")) if "is_valid" in f else is_valid_format(_pick_text(f))
        if not ok:
            continue
        cid = f.get("chat_id") or f.get("ChatID") or f.get("Chat Id") or "unknown"
        by_chat[str(cid)] += 1
        total += 1

    lines = [f"BÁO CÁO 5S - {datetime.now(VN_TZ).strftime('%d/%m/%Y')}"]
    lines.append(f"Tổng báo cáo hợp lệ: {total}")
    if by_chat:
        lines.append("Theo room:")
        for cid, cnt in sorted(by_chat.items()):
            lines.append(f" - Chat {cid}: {cnt}")
    return "\n".join(lines)

def send_daily_report():
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Missing secrets: TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID", file=sys.stderr)
        sys.exit(1)
    report_text = build_summary(fetch_today_records())
    print(report_text)
    tg_send_message(TELEGRAM_CHAT_ID, report_text)

# ===== Collector (quét getUpdates, phát hiện ảnh trùng) =====
def run_collector_once():
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Missing secrets: TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID", file=sys.stderr)
        sys.exit(1)

    # 1) Lấy offset cũ từ Meta
    last_val, rec_id = meta_get("last_update_id") if tbl_meta else (None, None)
    try:
        offset = int(last_val) + 1 if (last_val is not None and str(last_val).isdigit()) else None
    except Exception:
        offset = None

    # 2) getUpdates
    data = tg_get_updates(offset)
    updates = data.get("result", [])

    # 3) Gom theo nhóm media_group_id (None => single)
    groups: Dict[str, List[dict]] = {}
    for upd in updates:
        msg = upd.get("message") or {}
        if not msg:
            continue
        chat = msg.get("chat") or {}
        if str(chat.get("id")) != str(TELEGRAM_CHAT_ID):
            continue  # chỉ xử lý đúng nhóm
        key = msg.get("media_group_id") or f"single:{msg.get('message_id')}"
        groups.setdefault(key, []).append(msg)

    # 4) Xử lý từng nhóm
    last_update_id = None
    for upd in updates:
        last_update_id = upd.get("update_id", last_update_id)

    for key, msgs in groups.items():
        # caption: lấy caption/text đầu tiên có
        caption = ""
        for m in msgs:
            caption = m.get("caption") or m.get("text") or caption
            if caption:
                break

        # thu thập các file_unique_id ảnh
        uids: List[str] = []
        for m in msgs:
            if "photo" in m and isinstance(m["photo"], list) and m["photo"]:
                # lấy size lớn nhất (phần tử cuối)
                uids.append(m["photo"][-1].get("file_unique_id"))
            # (tuỳ chọn) tài liệu ảnh:
            if "document" in m:
                doc = m["document"]
                mime = (doc.get("mime_type") or "")
                if mime.startswith("image/"):
                    uids.append(doc.get("file_unique_id"))
        uids = [u for u in uids if u]

        # Nếu có ảnh và có caption => kiểm tra trùng
        is_album = not key.startswith("single:")
        first_msg_id = msgs[0].get("message_id")
        chat_id = msgs[0].get("chat", {}).get("id")
        user = msgs[0].get("from") or {}
        user_id = user.get("id", 0)
        username = user.get("username", "")

        if uids and caption:
            has_dup, existed = images_seen(uids)
            if has_dup:
                tg_send_message(chat_id, "Ảnh gửi có dấu hiệu trùng với trước đây, nhờ kiểm tra lại", reply_to=first_msg_id)
                # không ghi Messages, cũng không ghi fingerprints mới
                continue
            # không trùng -> xử lý flow bình thường
            ok = is_valid_format(caption)
            if ok:
                insert_message_record(chat_id, user_id, username, caption, True, first_msg_id, msgs[0].get("media_group_id"))
                tg_send_message(chat_id, "Đã ghi nhận báo cáo 5s ngày hôm nay", reply_to=first_msg_id)
                save_images_fingerprints(uids, chat_id, user_id, caption, msgs[0].get("media_group_id"), first_msg_id)
            else:
                tg_send_message(chat_id, "Kiểm tra lại format và gửi báo cáo lại", reply_to=first_msg_id)
        else:
            # không phải nhóm ảnh có caption → giữ behavior cũ cho text đơn lẻ
            text = caption or (msgs[0].get("text") or "")
            if not text:
                continue
            ok = is_valid_format(text)
            if ok:
                insert_message_record(chat_id, user_id, username, text, True, first_msg_id, msgs[0].get("media_group_id"))
                tg_send_message(chat_id, "Đã ghi nhận báo cáo 5s ngày hôm nay", reply_to=first_msg_id)
            else:
                tg_send_message(chat_id, "Kiểm tra lại format và gửi báo cáo lại", reply_to=first_msg_id)

    # 5) Cập nhật offset
    if last_update_id is not None and tbl_meta:
        meta_upsert("last_update_id", last_update_id, rec_id=rec_id)

# ===== Meta helpers =====
def meta_get(key: str):
    if not tbl_meta:
        return None, None
    try:
        rec = tbl_meta.first(formula=match({"key": key})) or tbl_meta.first(formula=match({"Key": key}))
        if rec:
            f = safe_get_fields(rec)
            val = f.get("value") or f.get("Value")
            return val, rec.get("id")
    except Exception as e:
        log.warning("Meta get error: %s", e)
    return None, None

def meta_upsert(key: str, value, rec_id: Optional[str] = None):
    if not tbl_meta:
        return
    body = {"key": key, "value": str(value)}
    try:
        if rec_id:
            tbl_meta.update(rec_id, body, replace=False)
            return
        rec = tbl_meta.first(formula=match({"key": key})) or tbl_meta.first(formula=match({"Key": key}))
        if rec:
            tbl_meta.update(rec["id"], body, replace=False)
        else:
            tbl_meta.create(body)
    except Exception as e:
        log.warning("Meta upsert error: %s", e)

# ===== Bot realtime (chỉ khi --bot) =====
def run_bot_polling():
    if not TELEGRAM_BOT_TOKEN:
        log.error("TELEGRAM_BOT_TOKEN is required to run bot")
        sys.exit(1)

    from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters  # lazy import

    async def start_cmd(update, context):
        await update.message.reply_text("Bot sẵn sàng. Gửi báo cáo theo dạng `12345678 - Nội dung` nhé!")

    async def reply_once_for_media_group(update, context, reply_text: str):
        msg = update.effective_message
        mgid = msg.media_group_id
        if mgid:
            if mgid in PROCESSED_MEDIA_GROUP_IDS:
                return
            PROCESSED_MEDIA_GROUP_IDS.add(mgid)
            await msg.reply_text(reply_text)
        else:
            await msg.reply_text(reply_text)

    async def handle_message(update, context):
        msg = update.effective_message
        chat = update.effective_chat
        user = update.effective_user

        # Với realtime, chưa tải ảnh để tính hash; vẫn dựa vào flow cũ
        text = msg.caption if getattr(msg, "caption", None) else (msg.text or "")
        if not text:
            return

        ok = is_valid_format(text)
        if ok:
            insert_message_record(
                chat_id=chat.id,
                user_id=user.id if user else 0,
                username=(user.username if user and user.username else ""),
                text=text,
                ok=True,
                msg_id=msg.id,
                media_group_id=msg.media_group_id,
            )
            await reply_once_for_media_group(update, context, "Đã ghi nhận báo cáo 5s ngày hôm nay")
        else:
            await reply_once_for_media_group(update, context, "Kiểm tra lại format và gửi báo cáo lại")

    app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(MessageHandler(filters.TEXT | filters.PHOTO | filters.VIDEO, handle_message))
    log.info("Bot is running (polling)...")
    app.run_polling(close_loop=False)

# ===== Entrypoint =====
if __name__ == "__main__":
    in_actions      = os.getenv("GITHUB_ACTIONS") == "true"
    force_daily     = ("--daily" in sys.argv)     or (os.getenv("RUN_DAILY") == "1")
    force_collector = ("--collector" in sys.argv) or (os.getenv("RUN_COLLECTOR") == "1")
    force_bot       = ("--bot" in sys.argv)

    # Ưu tiên: --bot > --collector > --daily/in_actions > realtime
    if force_bot:
        run_bot_polling()
    elif force_collector:
        run_collector_once()
    elif force_daily or in_actions:
        send_daily_report()
    else:
        run_bot_polling()
