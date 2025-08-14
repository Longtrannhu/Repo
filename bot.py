# bot.py (V4) — Daily report khớp bảng DateTime/UserID/Username/Message
import os
import re
import sys
import json
import logging
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from typing import List, Dict, Optional

from pyairtable import Api  # chỉ cần pyairtable cho chế độ daily

# -------------------- Config --------------------
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("report-bot")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
TBL_MESSAGES = os.getenv("TBL_MESSAGES", "Messages")
TBL_META = os.getenv("TBL_META", "Meta")

api = Api(AIRTABLE_TOKEN) if AIRTABLE_TOKEN else None
tbl_messages = api.table(AIRTABLE_BASE_ID, TBL_MESSAGES) if api else None
tbl_meta = api.table(AIRTABLE_BASE_ID, TBL_META) if api else None  # nếu cần

VN_TZ = timezone(timedelta(hours=7))
# Format hợp lệ: 8 số + " - " + chữ
FORMAT_RE = re.compile(r"^\s*\d{8}\s*-\s*[^\s].+$", re.UNICODE)

# Dùng khi chạy bot realtime để tránh reply nhiều lần cho 1 album
PROCESSED_MEDIA_GROUP_IDS = set()

# -------------------- Utils --------------------
def is_valid_format(text: str) -> bool:
    return bool(text and FORMAT_RE.match(text))

def now_vn_iso() -> str:
    return datetime.now(VN_TZ).isoformat(timespec="seconds")

def safe_get_fields(rec) -> dict:
    """
    Record Airtable chuẩn: {'id': 'rec...', 'fields': {...}}
    Nếu lỡ là list[record], lấy phần tử đầu.
    """
    if isinstance(rec, dict):
        return rec.get("fields", {}) or {}
    if isinstance(rec, list):
        for r in rec:
            if isinstance(r, dict):
                return r.get("fields", {}) or {}
    raise TypeError(f"Unexpected record type: {type(rec)}")

def _parse_any_dt(s: str) -> Optional[datetime]:
    """Parse nhiều định dạng thời gian. Naive → coi như giờ VN."""
    if not s:
        return None
    s = str(s).strip()
    # ISO (có/không có Z)
    try:
        if s.endswith("Z"):
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        return datetime.fromisoformat(s)
    except Exception:
        pass
    # 2025-08-13 17:12:22, 13/08/2025 17:12:22, ...
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

def insert_message_record(
    chat_id, user_id, username, text, ok, msg_id, media_group_id
):
    """Chỉ ghi những cột có trong bảng hiện tại của bạn: DateTime, UserID, Username, Message.
       (Không chèn is_valid để giữ đúng schema; nếu bạn muốn thêm, bật dòng tương ứng bên dưới.)"""
    if not tbl_messages:
        log.warning("Airtable not configured; skip insert")
        return
    if not ok:
        return  # không ghi nếu sai format

    fields = {
        "DateTime": now_vn_iso(),
        "UserID": str(user_id),
        "Username": username or "",
        "Message": text or "",
        # "is_valid": True,          # nếu muốn thêm cột này thì bỏ comment
        # Có thể thêm các cột khác nếu bảng bạn đã có sẵn:
        # "chat_id": str(chat_id),
        # "message_id": str(msg_id),
        # "media_group_id": str(media_group_id) if media_group_id else "",
    }
    try:
        tbl_messages.create(fields)
    except Exception as e:
        log.error("Insert to Airtable failed: %s", e)

# --- Gửi Telegram qua HTTP (không cần python-telegram-bot) ---
def send_tg_message_http(token: str, chat_id: str, text: str):
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = json.dumps({"chat_id": chat_id, "text": text}).encode("utf-8")
    req = Request(url, data=data, headers={"Content-Type": "application/json"})
    try:
        with urlopen(req, timeout=30) as resp:
            resp.read()
    except HTTPError as e:
        log.error("Telegram HTTPError %s: %s", e.code, e.read().decode("utf-8", "ignore"))
        raise
    except URLError as e:
        log.error("Telegram URLError: %s", e)
        raise

# -------------------- Daily Report (đÃ CHỈNH THEO BẢNG CỦA BẠN) --------------------
def fetch_today_records() -> List[Dict]:
    """
    Lấy record trong ngày (giờ VN) từ bảng Messages.
    Tự nhận diện cột thời gian: created_at → DateTime → datetime → Created At → createdTime.
    """
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

    results: List[Dict] = []
    for rec in records:
        fields = safe_get_fields(rec)

        # chọn nguồn thời gian: created_at → DateTime → ... → createdTime
        ts = (
            fields.get("created_at")
            or fields.get("DateTime")
            or fields.get("datetime")
            or fields.get("Created At")
            or rec.get("createdTime")
        )

        dt = _parse_any_dt(ts) if ts else None
        if not dt:
            continue

        # nếu dt không có tz, coi là VN
        if not dt.tzinfo:
            dt = dt.replace(tzinfo=VN_TZ)

        dt_vn = dt.astimezone(VN_TZ)
        if today_start <= dt_vn < today_end:
            results.append(rec)
    return results

def build_summary(records: List[Dict]) -> str:
    """
    Tạo báo cáo tổng hợp:
    - Nếu có cột is_valid thì dùng luôn.
    - Nếu không, tự tính hợp lệ từ trường nội dung (text/Message/message/caption).
    """
    total = 0
    by_chat = defaultdict(int)

    for rec in records:
        f = safe_get_fields(rec)

        if "is_valid" in f:
            ok = bool(f.get("is_valid"))
        else:
            ok = is_valid_format(_pick_text(f))

        if not ok:
            continue

        total += 1
        cid = (
            f.get("chat_id")
            or f.get("ChatID")
            or f.get("Chat Id")
            or "unknown"
        )
        by_chat[str(cid)] += 1

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
    recs = fetch_today_records()
    report_text = build_summary(recs)
    print(report_text)  # hiển thị trong log Actions
    send_tg_message_http(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, report_text)

# -------------------- Bot realtime (lazy import PTB) --------------------
def run_bot_polling():
    if not TELEGRAM_BOT_TOKEN:
        log.error("TELEGRAM_BOT_TOKEN is required to run bot")
        sys.exit(1)

    # Import bên trong để chế độ daily không yêu cầu lib này
    from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters

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

# -------------------- Entrypoint --------------------
if __name__ == "__main__":
    # Mặc định chạy daily khi ở GitHub Actions
    in_actions = os.getenv("GITHUB_ACTIONS") == "true"
    force_daily = ("--daily" in sys.argv) or (os.getenv("RUN_DAILY") == "1")
    force_bot = ("--bot" in sys.argv)

    # Ưu tiên:
    # 1) --bot => chạy bot realtime
    # 2) --daily hoặc RUN_DAILY=1 => daily
    # 3) Nếu ở GitHub Actions => daily
    # 4) Mặc định local => bot realtime
    if force_bot:
        run_bot_polling()
    elif force_daily or in_actions:
        send_daily_report()
    else:
        run_bot_polling()
