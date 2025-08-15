# bot.py — Telegram collector + 21h report (Airtable, pyairtable 3.x)
# - Gộp album theo media_group_id
# - Chống trùng ảnh (Images table) + fallback chống trùng caption trong ngày
# - Chỉ cảnh báo 1 lần/caption trong mỗi lần quét (tránh spam)
# - Báo cáo 21h dùng HTML (escape + tự cắt khi dài)

import os, re, datetime, hashlib
from typing import List, Dict, Any, Set
import pytz
import requests
from requests.exceptions import HTTPError
from pyairtable import Api  # v3.x

# ===== ENV =====
TELEGRAM_BOT_TOKEN  = os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("BOT_TOKEN")
TELEGRAM_CHAT_ID    = str(os.getenv("TELEGRAM_CHAT_ID") or os.getenv("GROUP_ID") or "").strip()

AIRTABLE_TOKEN      = os.getenv("AIRTABLE_TOKEN")
AIRTABLE_BASE_ID    = os.getenv("AIRTABLE_BASE_ID")
TBL_MESSAGES        = os.getenv("TBL_MESSAGES", "Messages")
TBL_META            = os.getenv("TBL_META", "Meta")
TBL_IMAGES          = os.getenv("TBL_IMAGES", "").strip()  # optional: bật chống trùng ảnh

# Tên cột Messages (có thể override qua ENV)
COL_MSG_TEXT        = os.getenv("COL_MSG_TEXT", "TextOrCaption")
COL_MSG_CODE        = os.getenv("COL_MSG_CODE", "Code")
COL_MSG_TS          = os.getenv("COL_MSG_TS", "Timestamp")  # chỉ dùng khi ĐỌC; khi CREATE bỏ qua

# Danh sách nơi bắt buộc (Meta)
COL_META_CODE       = os.getenv("COL_META_CODE", "MaNoi")
COL_META_NAME       = os.getenv("COL_META_NAME", "TenNoi")
# KV (lưu offset) – có thể không tồn tại, sẽ fallback qua MaNoi/TenNoi
COL_META_KEY        = os.getenv("COL_META_KEY", "Key")
COL_META_VAL        = os.getenv("COL_META_VAL", "Value")

# Bảng IMAGES (nếu dùng)
COL_IMG_HASH        = os.getenv("COL_IMG_HASH", "FileUniqueId")
COL_IMG_CODE        = os.getenv("COL_IMG_CODE", "Code")
COL_IMG_DATE        = os.getenv("COL_IMG_DATE", "Date")

VN_TZ = pytz.timezone("Asia/Ho_Chi_Minh")
CODE_RE = re.compile(r"^(\d{8})\s*-\s*", re.UNICODE)
CODE8_RE = re.compile(r"^\d{8}$")

MSG_OK      = "🆗Đã ghi nhận báo cáo 5s ngày hôm nay"
MSG_BADFMT  = "🆕Kiểm tra lại format và gửi báo cáo lại"
MSG_DUPIMG  = "⛔️Ảnh/caption trùng với trước đây, nhờ kiểm tra lại"

# ===== Airtable client (v3) =====
_api = Api(AIRTABLE_TOKEN)
def _air_table(name: str):
    return _api.table(AIRTABLE_BASE_ID, name)

# ===== Helpers =====
def _today_vn():
    return datetime.datetime.now(VN_TZ).date()

def _iso_local(dttm):
    if isinstance(dttm, str):
        try:
            return datetime.datetime.fromisoformat(dttm.replace("Z","+00:00")).astimezone(VN_TZ)
        except Exception:
            return None
    return dttm

def _tg(method: str, **kwargs):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}"
    r = requests.post(url, json=kwargs, timeout=30)
    r.raise_for_status()
    return r.json()

def _send_reply(chat_id: str, reply_to_message_id: int, text: str):
    return _tg("sendMessage", chat_id=chat_id, text=text,
               reply_to_message_id=reply_to_message_id,
               allow_sending_without_reply=True)

def _send_markdown(chat_id: str, text: str):
    # Giữ lại nếu cần dùng nơi khác; báo cáo 21h dùng HTML bên dưới
    return _tg("sendMessage", chat_id=chat_id, text=text, parse_mode="Markdown")

# ---- NEW: HTML helpers (an toàn, tránh 400 Bad Request) ----
def _html_escape(s: str) -> str:
    if s is None:
        return ""
    return (str(s)
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;"))

def _send_html(chat_id: str, html: str):
    return _tg("sendMessage", chat_id=chat_id, text=html, parse_mode="HTML")

def _send_long_html(chat_id: str, html: str, limit: int = 3900):
    """
    Telegram giới hạn ~4096 ký tự. Hàm này tự cắt theo dòng để gửi nhiều phần.
    Dùng 3900 để chừa biên an toàn cho emoji/tag HTML.
    """
    text = html
    while len(text) > limit:
        cut = text.rfind("\n", 0, limit)
        if cut == -1:
            cut = limit
        _send_html(chat_id, text[:cut])
        text = text[cut:].lstrip("\n")
    if text:
        _send_html(chat_id, text)

# ===== Meta KV with fallback =====
def _kv_find(tbl, key_field: str, val_field: str, key: str):
    formula = f"LOWER(TO_TEXT({{{key_field}}}))='{key.lower()}'"
    return tbl.all(formula=formula), val_field

def _meta_get(key: str) -> str:
    tbl = _air_table(TBL_META)
    try:
        recs, valf = _kv_find(tbl, COL_META_KEY, COL_META_VAL, key)
        if recs:
            return str(recs[0]["fields"].get(valf, ""))
    except HTTPError:
        pass
    try:
        recs, valf = _kv_find(tbl, COL_META_CODE, COL_META_NAME, key)
        if recs:
            return str(recs[0]["fields"].get(valf, ""))
    except HTTPError:
        pass
    return ""

def _meta_set(key: str, val: str):
    tbl = _air_table(TBL_META)
    try:
        recs, valf = _kv_find(tbl, COL_META_KEY, COL_META_VAL, key)
        if recs:
            tbl.update(recs[0]["id"], {valf: val}); return
    except HTTPError:
        recs = []
    try:
        recs, valf = _kv_find(tbl, COL_META_CODE, COL_META_NAME, key)
        if recs:
            tbl.update(recs[0]["id"], {valf: val}); return
    except HTTPError:
        recs = []
    for kf, vf in [(COL_META_KEY, COL_META_VAL), (COL_META_CODE, COL_META_NAME)]:
        try:
            tbl.create({kf: key, vf: val}); return
        except HTTPError:
            continue
    return

# ===== Dedup helpers =====
def _photo_unique_ids(photo_sizes: List[Dict[str,Any]]) -> List[str]:
    ids = []
    for ph in (photo_sizes or []):
        u = ph.get("file_unique_id")
        if u and u not in ids:
            ids.append(u)
    return ids

def _load_seen_uids() -> Set[str]:
    if not TBL_IMAGES:
        return set()
    tbl = _air_table(TBL_IMAGES)
    try:
        recs = tbl.all(fields=[COL_IMG_HASH])
    except HTTPError:
        return set()
    s = set()
    for r in recs:
        u = (r.get("fields") or {}).get(COL_IMG_HASH)
        if u: s.add(u)
    return s

def _is_duplicate_photo(ids: List[str], seen: Set[str]) -> bool:
    return any(uid in seen for uid in ids)

def _save_photo_ids(code: str, ids: List[str], seen: Set[str]):
    if not TBL_IMAGES or not ids:
        return
    tbl = _air_table(TBL_IMAGES)
    today = _today_vn().isoformat()
    for uid in ids:
        if uid in seen:  # tránh tạo trùng nhiều lần
            continue
        tbl.create({COL_IMG_HASH: uid, COL_IMG_CODE: code, COL_IMG_DATE: today})
        seen.add(uid)

def _hash_caption(text: str) -> str:
    return hashlib.sha1((text or "").strip().encode("utf-8")).hexdigest()

def _load_today_caption_hashes() -> Set[str]:
    """Fallback dedup khi chưa bật Images table: chặn caption trùng trong ngày."""
    tbl = _air_table(TBL_MESSAGES)
    try:
        recs = tbl.all(fields=[COL_MSG_TEXT])
    except HTTPError:
        return set()
    today = _today_vn()
    s = set()
    for r in recs:
        fields = r.get("fields", {})
        created = r.get("createdTime")
        ts_dt = _iso_local(created) if isinstance(created, str) else created
        if not ts_dt:
            continue
        if ts_dt.astimezone(VN_TZ).date() == today:
            h = _hash_caption(fields.get(COL_MSG_TEXT, ""))
            s.add(h)
    return s

# ===== Collector (15') — GỘP THEO media_group_id & DEDUP & CHỈ CẢNH BÁO 1 LẦN =====
def _extract_code(text: str) -> str:
    if not text:
        return ""
    m = CODE_RE.match(text.strip())
    return m.group(1) if m else ""

def collect_once():
    offset = _meta_get("last_update_id")
    offset = int(offset) + 1 if offset else None

    # tải bộ nhớ trùng để dùng trong phiên poll này
    seen_uids = _load_seen_uids()
    seen_caps = _load_today_caption_hashes()
    warned_caps: Set[str] = set()   # caption đã cảnh báo trong LẦN QUÉT NÀY

    resp = _tg("getUpdates", timeout=10, allowed_updates=["message"], offset=offset)
    updates = resp.get("result", [])

    group_buf: Dict[str, Dict[str, Any]] = {}
    last_update_id = None

    for u in updates:
        last_update_id = u.get("update_id", last_update_id)
        msg = u.get("message") or {}
        chat = msg.get("chat", {})
        chat_id = str(chat.get("id", ""))
        if not chat_id or chat_id != str(TELEGRAM_CHAT_ID):
            continue

        message_id = msg.get("message_id")
        text = msg.get("text", "")
        caption = msg.get("caption", "")
        photos = msg.get("photo", [])
        media_group_id = msg.get("media_group_id")

        content = caption if caption else text

        # Gom album: chỉ xử lý 1 lần/album
        if media_group_id:
            g = group_buf.get(media_group_id)
            if not g:
                g = {"chat_id": chat_id, "rep_msg_id": message_id, "caption": None, "photo_ids": set()}
                group_buf[media_group_id] = g
            g["photo_ids"].update(_photo_unique_ids(photos))
            if content:
                g["caption"] = content
                g["rep_msg_id"] = message_id
            continue

        # ---- Message lẻ ----
        code = _extract_code(content)
        if not code:
            if content:
                ch = _hash_caption(content)
                if ch not in warned_caps:
                    _send_reply(chat_id, message_id, MSG_BADFMT)
                    warned_caps.add(ch)
            continue

        photo_ids = _photo_unique_ids(photos)
        cap_hash = _hash_caption(content)

        # Nếu trùng ảnh hoặc caption trong ngày -> CHỈ cảnh báo 1 lần cho cap này
        if _is_duplicate_photo(photo_ids, seen_uids) or cap_hash in seen_caps:
            if cap_hash not in warned_caps:
                _send_reply(chat_id, message_id, MSG_DUPIMG)
                warned_caps.add(cap_hash)
            continue

        # Lưu & xác nhận
        _air_table(TBL_MESSAGES).create({COL_MSG_TEXT: content, COL_MSG_CODE: code})
        _save_photo_ids(code, photo_ids, seen_uids)
        seen_caps.add(cap_hash)
        _send_reply(chat_id, message_id, MSG_OK)

    # ---- Xử lý album đã gom ----
    for mgid, g in group_buf.items():
        chat_id = g["chat_id"]
        rep_id = g["rep_msg_id"]
        content = g["caption"] or ""
        photo_ids = list(g["photo_ids"])

        code = _extract_code(content)
        if not code:
            if content:
                ch = _hash_caption(content)
                if ch not in warned_caps:
                    _send_reply(chat_id, rep_id, MSG_BADFMT)
                    warned_caps.add(ch)
            continue

        cap_hash = _hash_caption(content)
        if _is_duplicate_photo(photo_ids, seen_uids) or cap_hash in seen_caps:
            if cap_hash not in warned_caps:
                _send_reply(chat_id, rep_id, MSG_DUPIMG)
                warned_caps.add(cap_hash)
            continue

        _air_table(TBL_MESSAGES).create({COL_MSG_TEXT: content, COL_MSG_CODE: code})
        _save_photo_ids(code, photo_ids, seen_uids)
        seen_caps.add(cap_hash)
        _send_reply(chat_id, rep_id, MSG_OK)

    if last_update_id is not None:
        _meta_set("last_update_id", str(last_update_id))

# ===== Daily report (21h) =====
def _get_master_codes():
    tbl = _air_table(TBL_META)
    recs = tbl.all(fields=[COL_META_CODE, COL_META_NAME])
    codes, name_map = [], {}
    for r in recs:
        f = r.get("fields", {})
        code = str(f.get(COL_META_CODE, "")).strip()
        if code and CODE8_RE.fullmatch(code):
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
    return [latest[c] for c in sorted(latest.keys())]

def run_daily_report():
    today_str = _today_vn().strftime("%d/%m/%Y")

    # 1) Lấy master list & tin đã gửi trong ngày
    master_codes, name_map = _get_master_codes()
    items_today = _get_today_messages()
    latest = _pick_latest_per_code(items_today)   # mỗi nơi lấy caption mới nhất
    sent_codes = {it["code"] for it in latest}

    total = len(master_codes)
    sent  = len(sent_codes)
    miss  = max(total - sent, 0)
    pct   = int(round((sent/total)*100)) if total else 0

    # 2) Danh sách "đã gửi" (HTML an toàn)
    sent_lines = []
    for it in sorted(latest, key=lambda x: x["code"]):
        code = _html_escape(it["code"])
        name = _html_escape(name_map.get(it["code"], ""))
        txt  = (it["text"] or "").replace("\n", " ")
        if len(txt) > 90:
            txt = txt[:87] + "..."
        txt = _html_escape(txt)
        sent_lines.append(f"• ✅ <code>{code}</code> — {name} — “{txt}”")
    if not sent_lines:
        sent_lines = ["<i>Chưa có nơi nào gửi trong hôm nay</i>"]

    # 3) Danh sách "chưa gửi"
    missing = [c for c in master_codes if c not in sent_codes]
    miss_lines = []
    for c in missing:
        code = _html_escape(c)
        name = _html_escape(name_map.get(c, ""))
        miss_lines.append(f"• ❌ <code>{code}</code> — {name}")
    if not miss_lines:
        miss_lines = ["<i>Tất cả nơi đã gửi đầy đủ</i>"]

    # 4) Ghép message HTML
    header = (
        f"📊 <b>Báo cáo 21h</b> — {today_str}\n"
        f"<b>Tổng quan:</b> Tổng <code>{total}</code> • ✅ Đã gửi <code>{sent}</code> • "
        f"❌ Thiếu <code>{miss}</code> • 📈 {pct}% đã gửi\n\n"
    )
    body1 = f"<b>1) Text/Caption đã gửi ({sent}):</b>\n" + "\n".join(sent_lines) + "\n\n"
    body2 = f"<b>2) Những nơi chưa gửi ({miss}):</b>\n" + "\n".join(miss_lines)
    html_msg = header + body1 + body2

    # 5) Gửi (tự động cắt nếu quá dài)
    _send_long_html(TELEGRAM_CHAT_ID, html_msg)

# ===== Main =====
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--collect", action="store_true", help="Poll Telegram & record messages")
    parser.add_argument("--daily", action="store_true", help="Send 21h report")
    args = parser.parse_args()

    if args.daily:
        run_daily_report()
    else:
        collect_once()
