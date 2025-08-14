# bot.py
import os
import re
import sys
import json
import time
import logging
from datetime import datetime, timedelta, timezone
from collections import defaultdict

from pyairtable import Api
from pyairtable.formulas import match

from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# -------------------- Config & Globals --------------------
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("report-bot")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")  # nơi nhận báo cáo hằng ngày

AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
TBL_MESSAGES = os.getenv("TBL_MESSAGES", "Messages")
TBL_META = os.getenv("TBL_META", "Meta")

if not TELEGRAM_BOT_TOKEN:
    log.warning("Missing TELEGRAM_BOT_TOKEN")

if not AIRTABLE_TOKEN or not AIRTABLE_BASE_ID:
    log.warning("Missing Airtable credentials")

api = Api(AIRTABLE_TOKEN) if AIRTABLE_TOKEN else None
tbl_messages = api.table(AIRTABLE_BASE_ID, TBL_MESSAGES) if api else None
tbl_meta = api.table(AIRTABLE_BASE_ID, TBL_META) if api else None

# Track các media_group_id đã trả lời để album chỉ reply 1 lần
# (in-memory; nếu muốn bền vững hơn thì lưu thêm vào Airtable Meta)
PROCESSED_MEDIA_GROUP_IDS = set()

# Format hợp lệ: 8 số + " - " + chữ (cho phép chữ/số/khoảng trắng, có dấu)
FORMAT_RE = re.compile(r"^\s*\d{8}\s*-\s*[^\s].+$", re.UNICODE)

# Timezone VN
VN_TZ = timezone(timedelta(hours=7))


# -------------------- Utils --------------------
def is_valid_format(text: str) -> bool:
    if not text:
        return False
    return bool(FORMAT_RE.match(text))


def now_vn_iso() -> str:
    return datetime.now(VN_TZ).isoformat(timespec="seconds")


def safe_get_fields(rec) -> dict:
    """
    Airtable record chuẩn là dict {'id': 'rec...', 'fields': {...}}
    Nhưng phòng trường hợp code ở nơi khác trả về list, ta xử lý an toàn.
    """
    if isinstance(rec, dict):
        return rec.get("fields", {}) or {}
    if isinstance(rec, list):
        # Lấy record đầu nếu là list record
        for r in rec:
            if isinstance(r, dict):
                return r.get("fields", {}) or {}
    raise TypeError(f"Unexpected record type: {type(rec)}")


def insert_message_record(chat_id: int, user_id: int, username: str, text: str, ok: bool, msg_id: int, media_group_id: str | None):
    if not tbl_messages:
        log.warning("Airtable not configured; skip insert")
        return

    fields = {
        "chat_id": str(chat_id),
        "user_id": str(user_id),
        "username": username or "",
        "text": text or "",
        "is_valid": bool(ok),
        "message_id": str(msg_id),
        "media_group_id": str(media_group_id) if media_group_id else "",
        "created_at": now_vn_iso(),
    }
    try:
        tbl_messages.create(fields)
    except Exception as e:
        log.error("Insert to Airtable failed: %s", e)


async def reply_once_for_media_group(update: Update, context: ContextTypes.DEFAULT_TYPE, reply_text: str):
    """
    Đảm bảo album nhiều ảnh cùng 1 caption chỉ trả lời 1 lần.
    - Nếu message có media_group_id, dùng nó để dedupe.
    - Nếu không có, trả lời bình thường.
    """
    msg = update.effective_message
    mgid = msg.media_group_id

    if mgid:
        if mgid in PROCESSED_MEDIA_GROUP_IDS:
            # Đã trả lời album này
            return
        PROCESSED_MEDIA_GROUP_IDS.add(mgid)
        await msg.reply_text(reply_text)
    else:
        await msg.reply_text(reply_text)


# -------------------- Handlers --------------------
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Bot sẵn sàng. Gửi báo cáo theo dạng `12345678 - Nội dung` nhé!")


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Nhận text hoặc ảnh/caption:
    - Nếu đúng format: trả lời xác nhận + ghi Airtable
    - Nếu sai: nhắc lại format + KHÔNG ghi Airtable
    - Album nhiều ảnh 1 caption: chỉ trả lời 1 lần
    """
    msg = update.effective_message
    chat = update.effective_chat
    user = update.effective_user

    text = ""
    media_group_id = msg.media_group_id

    if msg.caption:  # ảnh/video với caption
        text = msg.caption
    elif msg.text:
        text = msg.text
    else:
        # Không phải text/caption thì bỏ qua (stickers, etc.)
        return

    ok = is_valid_format(text)

    # Insert vào Airtable chỉ khi đúng format
    if ok:
        insert_message_record(
            chat_id=chat.id,
            user_id=user.id if user else 0,
            username=(user.username if user and user.username else ""),
            text=text,
            ok=True,
            msg_id=msg.id,
            media_group_id=media_group_id,
        )
        await reply_once_for_media_group(update, context, "Đã ghi nhận báo cáo 5s ngày hôm nay")
    else:
        # KHÔNG insert nếu sai format
        await reply_once_for_media_group(update, context, "Kiểm tra lại format và gửi báo cáo lại")


# -------------------- Daily Report --------------------
def fetch_today_records() -> list[dict]:
    """
    Lấy record trong ngày (theo giờ VN) từ bảng Messages.
    Giả sử có cột 'created_at' ISO8601.
    """
    if not tbl_messages:
        log.warning("Airtable not configured; cannot fetch today records")
        return []

    today_start = datetime.now(VN_TZ).replace(hour=0, minute=0, second=0, microsecond=0)
    today_end = today_start + timedelta(days=1)

    # Airtable không query theo TZ, nên ta lọc client-side sau khi lấy rộng
    # Có thể thêm điều kiện match theo ngày (nếu bạn có cột ngày dạng string YYYY-MM-DD)
    try:
        # Lấy tối đa 1000 record gần đây, sau đó lọc theo created_at
        records = tbl_messages.all(page_size=100, max_records=1000)
    except Exception as e:
        log.error("Airtable fetch failed: %s", e)
        return []

    results = []
    for rec in records:
        try:
            fields = safe_get_fields(rec)
            created = fields.get("created_at")
            if not created:
                continue
            # parse created_at
            try:
                dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
            except Exception:
                # nếu không chuẩn ISO, bỏ qua
                continue
            # chuyển về VN
            dt_vn = dt.astimezone(VN_TZ)
            if today_start <= dt_vn < today_end:
                results.append(rec)
        except Exception as e:
            log.warning("Skip bad record shape: %s", e)
    return results


def build_summary(records: list[dict]) -> str:
    """
    Tạo báo cáo tổng hợp:
    - Tổng số báo cáo hợp lệ hôm nay
    - Đếm theo chat_id
    """
    total = 0
    by_chat = defaultdict(int)

    for rec in records:
        fields = safe_get_fields(rec)
        if not fields.get("is_valid"):
            continue
        total += 1
        cid = fields.get("chat_id", "unknown")
        by_chat[cid] += 1

    lines = [f"BÁO CÁO 5S - {datetime.now(VN_TZ).strftime('%d/%m/%Y')}"]
    lines.append(f"Tổng báo cáo hợp lệ: {total}")
    if by_chat:
        lines.append("Theo room:")
        for cid, cnt in sorted(by_chat.items(), key=lambda x: x[0]):
            lines.append(f" - Chat {cid}: {cnt}")
    return "\n".join(lines)


async def send_daily_report_async(token: str, chat_id: str, text: str):
    app = ApplicationBuilder().token(token).build()
    async with app:
        await app.bot.send_message(chat_id=chat_id, text=text)


def send_daily_report():
    """
    Hàm chạy trong GitHub Actions:
    - Lấy record hôm nay
    - Tổng hợp
    - Gửi Telegram
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Missing secrets: TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID", file=sys.stderr)
        sys.exit(1)

    recs = fetch_today_records()
    report_text = build_summary(recs)
    # In ra log để xem trên Actions
    print(report_text)

    # Gửi Telegram (synchronous wrapper)
    import asyncio
    asyncio.run(send_daily_report_async(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, report_text))


# -------------------- Entrypoint --------------------
def run_bot_polling():
    if not TELEGRAM_BOT_TOKEN:
        log.error("TELEGRAM_BOT_TOKEN is required to run bot")
        sys.exit(1)

    app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start_cmd))
    # Nhận mọi message text/photo/caption
    app.add_handler(MessageHandler(filters.TEXT | filters.PHOTO | filters.VIDEO, handle_message))

    log.info("Bot is running (polling)...")
    app.run_polling(close_loop=False)


if __name__ == "__main__":
    """
    Cách chạy:
    - Chạy bot:      python bot.py
    - Chạy báo cáo:  python bot.py --daily
      (hoặc đặt ENV RUN_DAILY=1 để auto chạy báo cáo)
    """
    if "--daily" in sys.argv or os.getenv("RUN_DAILY") == "1":
        send_daily_report()
    else:
        run_bot_polling()
