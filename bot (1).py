import os
import csv
import html
import re
import sqlite3
import threading
import time
import tempfile
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, HTTPServer
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Set

import pytz
import telebot
from telebot import types
from apscheduler.schedulers.background import BackgroundScheduler

# =========================
# CONFIG
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN", "8238655181:AAFIt2IrqeELIwDj3na8xqNMZ6kIQJJvRBE").strip()
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set.")

_admin_ids_env = os.getenv("ADMIN_IDS", "6939239782,5094614110")
ADMIN_IDS: Set[int] = set(int(x.strip()) for x in _admin_ids_env.split(",") if x.strip().isdigit())

CASES_GROUP_ID = int(os.getenv("CASES_GROUP_ID", "-5285886935"))
FLEET_CONTACT = "@Chester_FLEET"
DB_PATH = os.getenv("DB_PATH", "gwe_fleet_bot.db")
ET_TZ = pytz.timezone("US/Eastern")

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")

# =========================
# STATE MACHINE (thread-safe)
# =========================
_state_lock = threading.Lock()
user_states: Dict[int, Dict[str, Any]] = {}


def set_state(user_id: int, state: str, data: Optional[Dict[str, Any]] = None) -> None:
    with _state_lock:
        user_states[user_id] = {"state": state, "data": data or {}}


def get_state(user_id: int) -> Dict[str, Any]:
    with _state_lock:
        return dict(user_states.get(user_id, {"state": None, "data": {}}))


def clear_state(user_id: int) -> None:
    with _state_lock:
        user_states.pop(user_id, None)


# =========================
# DATABASE
# =========================
@contextmanager
def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    try:
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"[DB_ERROR] {repr(e)}")
        raise
    finally:
        conn.close()


def init_db() -> None:
    with get_db() as conn:
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS groups (
                id INTEGER PRIMARY KEY,
                title TEXT,
                unit_code TEXT,
                driver_name TEXT
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS pti_reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_id INTEGER,
                driver_name TEXT,
                timestamp_et TEXT,
                date_et TEXT,
                FOREIGN KEY(group_id) REFERENCES groups(id)
            )
        """)
        cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS uq_pti_group_date
            ON pti_reports(group_id, date_et)
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS pm_plans (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_id INTEGER,
                created_at TEXT,
                is_done INTEGER DEFAULT 0,
                done_at TEXT,
                amount REAL,
                FOREIGN KEY(group_id) REFERENCES groups(id)
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS drivers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS cases (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_id INTEGER,
                group_title TEXT,
                driver_name TEXT,
                message TEXT,
                status TEXT DEFAULT 'NEW',
                created_at TEXT,
                updated_at TEXT,
                updated_by TEXT,
                cases_message_id INTEGER,
                FOREIGN KEY(group_id) REFERENCES groups(id)
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS parking (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                unit_number TEXT,
                unit_type TEXT,
                location TEXT,
                start_date TEXT,
                end_date TEXT,
                status TEXT DEFAULT 'ACTIVE',
                created_at TEXT,
                created_by INTEGER,
                closed_at TEXT,
                notes TEXT,
                alert_24h_sent INTEGER DEFAULT 0,
                alert_12h_sent INTEGER DEFAULT 0,
                alert_2h_sent INTEGER DEFAULT 0,
                alert_expired_sent INTEGER DEFAULT 0,
                home_request_group_id INTEGER DEFAULT NULL
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS dot_documents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                unit_number TEXT UNIQUE,
                unit_type TEXT,
                expiry_date TEXT,
                photo_file_id TEXT,
                created_at TEXT,
                updated_at TEXT
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS units (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                unit_number TEXT UNIQUE,
                unit_type TEXT,
                year_model TEXT,
                vin TEXT,
                plate TEXT,
                state TEXT,
                trailer_type TEXT,
                trailer_length TEXT,
                created_at TEXT,
                updated_at TEXT
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS admins (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                added_at TEXT
            )
        """)

        for aid in ADMIN_IDS:
            cur.execute("INSERT OR IGNORE INTO admins (user_id, added_at) VALUES (?, ?)",
                        (aid, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))


# =========================
# HELPERS
# =========================
def is_admin(user_id: int) -> bool:
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM admins WHERE user_id = ?", (user_id,))
            return cur.fetchone() is not None
    except:
        return user_id in ADMIN_IDS


def get_et_now() -> datetime:
    return datetime.now(ET_TZ)


def format_et(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M (ET)")


def h(s: Optional[str]) -> str:
    return html.escape(s or "")


def log_send_error(context: str, e: Exception) -> None:
    print(f"[SEND_ERROR] {context}: {repr(e)}")


def get_all_admin_ids() -> List[int]:
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT user_id FROM admins")
            return [r["user_id"] for r in cur.fetchall()]
    except:
        return list(ADMIN_IDS)


def ensure_group_record(chat) -> Optional[int]:
    if chat.type not in ("group", "supergroup"):
        return None
    title = chat.title or ""
    m = re.search(r"\bGL\d{4}\b", title)
    unit_code = m.group(0) if m else None
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT id FROM groups WHERE id = ?", (chat.id,))
            row = cur.fetchone()
            if row:
                cur.execute("""
                    UPDATE groups SET title = ?,
                    unit_code = CASE WHEN unit_code IS NULL OR unit_code = '' THEN ? ELSE unit_code END
                    WHERE id = ?
                """, (title, unit_code, chat.id))
                return row["id"]
            cur.execute("INSERT INTO groups (id, title, unit_code, driver_name) VALUES (?, ?, ?, ?)",
                        (chat.id, title, unit_code, None))
            return chat.id
    except Exception as e:
        print(f"[ERROR] ensure_group_record: {repr(e)}")
        return None


def get_or_create_driver_id(driver_name: str) -> int:
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT id FROM drivers WHERE name = ?", (driver_name,))
            row = cur.fetchone()
            if row:
                return int(row["id"])
            cur.execute("INSERT INTO drivers (name) VALUES (?)", (driver_name,))
            return int(cur.lastrowid)
    except:
        return -1


def get_driver_name_by_id(driver_id: int) -> Optional[str]:
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT name FROM drivers WHERE id = ?", (driver_id,))
            row = cur.fetchone()
            return row["name"] if row else None
    except:
        return None


# =========================
# KEYBOARDS
# =========================
def kb_main_menu():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row("PTI reports", "PM")
    kb.row("Cases", "Parking")
    kb.row("DOT", "Units")
    kb.row("Message", "Manage groups")
    kb.row("Monthly Report", "Manage admins")
    return kb


def kb_pti_menu():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row("Missing PTI today", "Missing PTI week")
    kb.row("Send PTI reminder", "Driver report")
    kb.row("Back to main menu")
    return kb


def kb_pm_menu():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row("PM plans", "Reports")
    kb.row("Back to main menu")
    return kb


def kb_pm_plans_menu():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row("Planned units", "Add unit to plan")
    kb.row("Send reminder to planned units")
    kb.row("Back to main menu")
    return kb


def kb_manage_groups_menu():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row("Groups", "Delete group")
    kb.row("Back to main menu")
    return kb


def kb_cases_menu():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row("Active cases", "All cases history")
    kb.row("Back to main menu")
    return kb


def kb_parking_menu():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row("Active parkings", "Add parking")
    kb.row("Home time requests", "Parking history")
    kb.row("Back to main menu")
    return kb


def kb_dot_menu():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row("Add DOT document", "Expiring DOT docs")
    kb.row("Back to main menu")
    return kb


def kb_units_menu():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row("Add unit", "Search unit")
    kb.row("Back to main menu")
    return kb


def kb_admins_menu():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row("List admins", "Add admin")
    kb.row("Remove admin", "Back to main menu")
    return kb


# =========================
# GROUP COMMANDS
# =========================
@bot.message_handler(commands=["start"])
def cmd_start(message):
    if message.chat.type in ("group", "supergroup"):
        return
    user_id = message.from_user.id
    if is_admin(user_id):
        clear_state(user_id)
        bot.send_message(message.chat.id,
                         "Assalamu aleykum xizmatizdamiz <b>Xo'jayin</b>. 🚛\nWelcome to GWE Fleet Bot!",
                         reply_markup=kb_main_menu())
    else:
        bot.send_message(message.chat.id,
                         f"This bot works only for admins.\nContact: {h(FLEET_CONTACT)}")


@bot.message_handler(commands=["pti"])
def cmd_pti(message):
    chat = message.chat
    if chat.type not in ("group", "supergroup"):
        return
    group_id = ensure_group_record(chat)
    if group_id is None:
        return
    text = (message.text or "").strip()
    parts = text.split(maxsplit=1)
    try:
        with get_db() as conn:
            cur = conn.cursor()
            if len(parts) > 1:
                driver_name = parts[1].strip()
                cur.execute("UPDATE groups SET driver_name = ? WHERE id = ?", (driver_name, group_id))
            else:
                cur.execute("SELECT driver_name FROM groups WHERE id = ?", (group_id,))
                row = cur.fetchone()
                if not row or not row["driver_name"]:
                    bot.reply_to(message,
                                 "Please send PTI first time as:\n<code>/pti Full Name</code>\n"
                                 "After that you can use just <code>/pti</code>.")
                    return
                driver_name = row["driver_name"]

            get_or_create_driver_id(driver_name)
            now_et = get_et_now()
            today_str = now_et.strftime("%Y-%m-%d")
            cur.execute("""
                INSERT OR IGNORE INTO pti_reports (group_id, driver_name, timestamp_et, date_et)
                VALUES (?, ?, ?, ?)
            """, (group_id, driver_name, now_et.strftime("%Y-%m-%d %H:%M:%S"), today_str))
            inserted = cur.rowcount > 0

        if inserted:
            bot.reply_to(message, "✅ PTI report was sent to fleet department!")
        else:
            bot.reply_to(message, "✅ PTI for today is already recorded.\nYour report is forwarded to fleet.")

        notif = (f"🚨 <b>New PTI Report</b>\n"
                 f"👤 Driver: <b>{h(driver_name)}</b>\n"
                 f"🕒 Time: <b>{h(format_et(now_et))}</b>\n"
                 f"🚛 Group: <b>{h(chat.title)}</b>")
        if not inserted:
            notif += "\nℹ️ Additional PTI (already counted today)."
        for admin_id in get_all_admin_ids():
            try:
                bot.send_message(admin_id, notif)
            except Exception as e:
                log_send_error("PTI admin notify", e)
    except Exception as e:
        print(f"[ERROR] cmd_pti: {repr(e)}")
        bot.reply_to(message, "Error processing PTI report.")


@bot.message_handler(commands=["pm"])
def cmd_pm_group(message):
    chat = message.chat
    if chat.type not in ("group", "supergroup"):
        return
    group_id = ensure_group_record(chat)
    if group_id is None:
        return
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT id FROM pm_plans WHERE group_id = ? AND is_done = 0", (group_id,))
            if cur.fetchone():
                bot.reply_to(message, "✅ This unit is already in PM plan.")
                return
            now_str = get_et_now().strftime("%Y-%m-%d %H:%M:%S")
            cur.execute("INSERT INTO pm_plans (group_id, created_at, is_done) VALUES (?, ?, 0)",
                        (group_id, now_str))
        bot.reply_to(message, "✅ Unit added to PM plan.")
    except Exception as e:
        print(f"[ERROR] cmd_pm_group: {repr(e)}")
        bot.reply_to(message, "Error adding to PM plan.")


@bot.message_handler(commands=["fleet"])
def cmd_fleet(message):
    chat = message.chat
    if chat.type not in ("group", "supergroup"):
        return
    group_id = ensure_group_record(chat)
    if group_id is None:
        return

    text = (message.text or "").strip()
    parts = text.split(maxsplit=1)
    case_message = parts[1].strip() if len(parts) > 1 else "(no description)"

    driver_name = message.from_user.first_name or ""
    if message.from_user.last_name:
        driver_name += f" {message.from_user.last_name}"

    now_et = get_et_now()
    now_str = now_et.strftime("%Y-%m-%d %H:%M:%S")

    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO cases (group_id, group_title, driver_name, message, status, created_at, updated_at)
                VALUES (?, ?, ?, ?, 'NEW', ?, ?)
            """, (group_id, chat.title, driver_name, case_message, now_str, now_str))
            case_id = cur.lastrowid

        case_text = (f"🆕 <b>NEW CASE #{case_id:04d}</b>\n"
                     f"🚛 Group: <b>{h(chat.title)}</b>\n"
                     f"👤 Driver: <b>{h(driver_name)}</b>\n"
                     f"🕒 Time: <b>{now_et.strftime('%H:%M  %d/%m/%Y')}</b>\n"
                     f"📝 <b>{h(case_message)}</b>")

        kb = types.InlineKeyboardMarkup()
        kb.row(
            types.InlineKeyboardButton("🔄 IN PROCESS", callback_data=f"case_process:{case_id}"),
            types.InlineKeyboardButton("✅ DONE", callback_data=f"case_done:{case_id}")
        )

        sent = bot.send_message(CASES_GROUP_ID, case_text, reply_markup=kb)
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("UPDATE cases SET cases_message_id = ? WHERE id = ?", (sent.message_id, case_id))

        bot.reply_to(message, f"✅ Case #{case_id:04d} sent to fleet department.")
    except Exception as e:
        print(f"[ERROR] cmd_fleet: {repr(e)}")
        bot.reply_to(message, "Error creating case.")


@bot.message_handler(commands=["home"])
def cmd_home(message):
    chat = message.chat
    if chat.type not in ("group", "supergroup"):
        return
    ensure_group_record(chat)

    driver_name = message.from_user.first_name or ""
    if message.from_user.last_name:
        driver_name += f" {message.from_user.last_name}"

    now_et = get_et_now()
    notif = (f"🏠 <b>HOME TIME REQUEST</b>\n"
             f"🚛 Group: <b>{h(chat.title)}</b>\n"
             f"👤 Driver: <b>{h(driver_name)}</b>\n"
             f"🕒 Time: <b>{now_et.strftime('%H:%M  %d/%m/%Y')}</b>")

    kb = types.InlineKeyboardMarkup()
    kb.row(
        types.InlineKeyboardButton("🅿️ ADD PARKING", callback_data=f"home_add_parking:{chat.id}"),
        types.InlineKeyboardButton("❌ IGNORE", callback_data=f"home_ignore:{chat.id}")
    )
    for admin_id in get_all_admin_ids():
        try:
            bot.send_message(admin_id, notif, reply_markup=kb)
        except Exception as e:
            log_send_error("Home time notify", e)

    bot.reply_to(message, "🏠 Home time request sent to fleet. Please wait.")


@bot.message_handler(commands=["unit", "info"])
def cmd_unit_info(message):
    text = (message.text or "").strip()
    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        bot.reply_to(message, "Usage: <code>/unit GL1234</code> or <code>/info T1234</code>")
        return

    unit_number = parts[1].strip().upper()
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM units WHERE unit_number = ?", (unit_number,))
            unit = cur.fetchone()
            cur.execute("""SELECT expiry_date, photo_file_id FROM dot_documents
                          WHERE unit_number = ? ORDER BY expiry_date DESC LIMIT 1""", (unit_number,))
            dot_row = cur.fetchone()

        if not unit:
            bot.reply_to(message, f"Unit <code>{h(unit_number)}</code> not found.")
            return

        if unit["unit_type"] == "TRUCK":
            dot_info = ""
            if dot_row:
                try:
                    exp_dt = datetime.strptime(dot_row["expiry_date"], "%Y-%m-%d")
                    days_left = (exp_dt.date() - get_et_now().date()).days
                    dot_info = f"\n🔍 DOT Expires: <code>{exp_dt.strftime('%m/%d/%Y')}</code> ({days_left} days left)"
                except:
                    pass
            msg = (f"🚛 <b>{h(unit['unit_number'])}</b>\n"
                   f"📅 Year & Model: <b>{h(unit['year_model'])}</b>\n"
                   f"🔑 VIN: <code>{h(unit['vin'])}</code>\n"
                   f"🔢 Plate: <code>{h(unit['plate'])}</code>\n"
                   f"📍 State: <code>{h(unit['state'])}</code>"
                   f"{dot_info}")
        else:
            msg = (f"🚜 <b>{h(unit['unit_number'])}</b>\n"
                   f"📅 Year & Model: <b>{h(unit['year_model'])}</b>\n"
                   f"📏 Type & Length: <b>{h(unit['trailer_type'])} {h(unit['trailer_length'])}</b>\n"
                   f"🔑 VIN: <code>{h(unit['vin'])}</code>\n"
                   f"🔢 Plate: <code>{h(unit['plate'])}</code>\n"
                   f"📍 State: <code>{h(unit['state'])}</code>")

        bot.reply_to(message, msg)
    except Exception as e:
        print(f"[ERROR] cmd_unit_info: {repr(e)}")
        bot.reply_to(message, "Error fetching unit info.")


# Handle @mention as case
@bot.message_handler(func=lambda m: (
    m.chat.type in ("group", "supergroup") and
    m.text and
    m.entities and
    any(e.type == "mention" for e in (m.entities or []))
))
def handle_fleet_mention(message):
    try:
        me = bot.get_me()
        if f"@{me.username}" not in (message.text or ""):
            return
    except:
        return

    chat = message.chat
    group_id = ensure_group_record(chat)
    if group_id is None:
        return

    try:
        username = bot.get_me().username
        case_message = (message.text or "").replace(f"@{username}", "").strip()
    except:
        case_message = message.text or ""

    if not case_message:
        case_message = "(no description)"

    driver_name = message.from_user.first_name or ""
    if message.from_user.last_name:
        driver_name += f" {message.from_user.last_name}"

    now_et = get_et_now()
    now_str = now_et.strftime("%Y-%m-%d %H:%M:%S")

    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO cases (group_id, group_title, driver_name, message, status, created_at, updated_at)
                VALUES (?, ?, ?, ?, 'NEW', ?, ?)
            """, (group_id, chat.title, driver_name, case_message, now_str, now_str))
            case_id = cur.lastrowid

        case_text = (f"🆕 <b>NEW CASE #{case_id:04d}</b>\n"
                     f"🚛 Group: <b>{h(chat.title)}</b>\n"
                     f"👤 Driver: <b>{h(driver_name)}</b>\n"
                     f"🕒 Time: <b>{now_et.strftime('%H:%M  %d/%m/%Y')}</b>\n"
                     f"📝 <b>{h(case_message)}</b>")

        kb = types.InlineKeyboardMarkup()
        kb.row(
            types.InlineKeyboardButton("🔄 IN PROCESS", callback_data=f"case_process:{case_id}"),
            types.InlineKeyboardButton("✅ DONE", callback_data=f"case_done:{case_id}")
        )
        sent = bot.send_message(CASES_GROUP_ID, case_text, reply_markup=kb)
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("UPDATE cases SET cases_message_id = ? WHERE id = ?", (sent.message_id, case_id))

        bot.reply_to(message, f"✅ Case #{case_id:04d} sent to fleet.")
    except Exception as e:
        print(f"[ERROR] handle_fleet_mention: {repr(e)}")


# =========================
# ADMIN PRIVATE HANDLER
# =========================
@bot.message_handler(func=lambda m: m.chat.type == "private" and is_admin(m.from_user.id))
def handle_admin(message):
    user_id = message.from_user.id

    # handle photo (for DOT)
    if not message.text:
        st = get_state(user_id)
        if st["state"] == "DOT_WAIT_PHOTO" and message.photo:
            handle_dot_photo(message, st["data"])
        return

    text = message.text.strip()

    # GLOBAL NAV
    nav_map = {
        "/menu": kb_main_menu, "Back to main menu": kb_main_menu, "Main menu": kb_main_menu,
    }
    if text in nav_map:
        clear_state(user_id)
        bot.send_message(message.chat.id, "Main menu:", reply_markup=kb_main_menu())
        return

    menu_map = {
        "PTI reports": ("PTI reports menu:", kb_pti_menu),
        "PM": ("PM menu:", kb_pm_menu),
        "PM plans": ("PM plans menu:", kb_pm_plans_menu),
        "Cases": ("Cases menu:", kb_cases_menu),
        "Parking": ("Parking menu:", kb_parking_menu),
        "DOT": ("DOT menu:", kb_dot_menu),
        "Units": ("Units menu:", kb_units_menu),
        "Manage groups": ("Manage groups:", kb_manage_groups_menu),
        "Manage admins": ("Manage admins:", kb_admins_menu),
    }
    if text in menu_map:
        clear_state(user_id)
        label, kb_func = menu_map[text]
        bot.send_message(message.chat.id, label, reply_markup=kb_func())
        return

    if text == "Message":
        clear_state(user_id)
        kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
        kb.row("All groups", "Choose groups")
        kb.row("Cancel")
        set_state(user_id, "BROADCAST_CHOOSE_MODE")
        bot.send_message(message.chat.id, "Choose where to send:", reply_markup=kb)
        return

    if text == "Monthly Report":
        clear_state(user_id)
        handle_monthly_report_menu(message)
        return

    st = get_state(user_id)
    state = st["state"]
    data = st["data"]

    # ===== BROADCAST =====
    if state == "BROADCAST_CHOOSE_MODE":
        if text == "Cancel":
            clear_state(user_id)
            bot.send_message(message.chat.id, "Cancelled.", reply_markup=kb_main_menu())
        elif text == "All groups":
            set_state(user_id, "BROADCAST_WAIT_TEXT", {"group_ids": None})
            kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
            kb.row("Cancel")
            bot.send_message(message.chat.id, "Send message to broadcast to <b>all groups</b>:", reply_markup=kb)
        elif text == "Choose groups":
            show_groups_mono(message.chat.id)
            set_state(user_id, "BROADCAST_WAIT_GROUPS")
            kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
            kb.row("Cancel")
            bot.send_message(message.chat.id,
                             "Send group IDs separated by space:\nExample: -1001234567890 -1009876543210",
                             reply_markup=kb)
        return

    if state == "BROADCAST_WAIT_GROUPS":
        if text.lower() == "cancel":
            clear_state(user_id)
            bot.send_message(message.chat.id, "Cancelled.", reply_markup=kb_main_menu())
            return
        tokens = text.replace(",", " ").split()
        group_ids = []
        for t in tokens:
            if t.lstrip("-").isdigit():
                gid = int(t)
                if gid not in group_ids:
                    group_ids.append(gid)
        if not group_ids:
            bot.send_message(message.chat.id, "No valid IDs. Try again or Cancel.")
            return
        set_state(user_id, "BROADCAST_WAIT_TEXT", {"group_ids": group_ids})
        kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
        kb.row("Cancel")
        bot.send_message(message.chat.id, "Now send the message:", reply_markup=kb)
        return

    if state == "BROADCAST_WAIT_TEXT":
        if text.lower() == "cancel":
            clear_state(user_id)
            bot.send_message(message.chat.id, "Cancelled.", reply_markup=kb_main_menu())
            return
        safe_text = h(text)
        set_state(user_id, "BROADCAST_CONFIRM", {"text": safe_text, "group_ids": data.get("group_ids")})
        kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
        kb.row("Approve", "Cancel")
        bot.send_message(message.chat.id, f"Your message:\n\n{safe_text}\n\nPress <b>Approve</b> to send.",
                         reply_markup=kb)
        return

    if state == "BROADCAST_CONFIRM":
        if text == "Cancel":
            clear_state(user_id)
            bot.send_message(message.chat.id, "Cancelled.", reply_markup=kb_main_menu())
        elif text == "Approve":
            try:
                send_broadcast(message.chat.id, data.get("text", ""), data.get("group_ids"))
            except Exception as e:
                bot.send_message(message.chat.id, "Error during broadcast.")
            finally:
                clear_state(user_id)
                bot.send_message(message.chat.id, "Done.", reply_markup=kb_main_menu())
        return

    # ===== STATES =====
    if state == "DRIVER_REPORT_WAIT_NAME":
        show_driver_search_results(message.chat.id, user_id, text)
        return

    if state == "PM_ADD_UNIT_WAIT_SEARCH":
        show_pm_add_unit_search_results(message.chat.id, user_id, text)
        return

    if state == "PM_DONE_WAIT_AMOUNT":
        handle_pm_done_amount(message, data.get("plan_id"))
        return

    if state == "PM_REPORT_CHOOSE_MONTH":
        handle_pm_report_month_choice(message, text)
        return

    if state == "DELETE_GROUP_WAIT_SEARCH":
        show_delete_group_search_results(message.chat.id, user_id, text)
        return

    if state == "MONTHLY_REPORT_CHOOSE_MONTH":
        handle_monthly_report_generate(message, text)
        return

    # PARKING STATES
    if state == "PARKING_ADD_UNIT":
        set_state(user_id, "PARKING_ADD_LOCATION", {**data, "unit_number": text.upper()})
        bot.send_message(message.chat.id, "Enter parking location/yard name:")
        return

    if state == "PARKING_ADD_LOCATION":
        set_state(user_id, "PARKING_ADD_START", {**data, "location": text})
        bot.send_message(message.chat.id, "Enter start date (MM/DD/YYYY):")
        return

    if state == "PARKING_ADD_START":
        try:
            datetime.strptime(text, "%m/%d/%Y")
        except ValueError:
            bot.send_message(message.chat.id, "Invalid format. Use MM/DD/YYYY:")
            return
        set_state(user_id, "PARKING_ADD_END", {**data, "start_date": text})
        bot.send_message(message.chat.id, "Enter end date (MM/DD/YYYY):")
        return

    if state == "PARKING_ADD_END":
        try:
            datetime.strptime(text, "%m/%d/%Y")
        except ValueError:
            bot.send_message(message.chat.id, "Invalid format. Use MM/DD/YYYY:")
            return
        save_parking(message, data, text)
        return

    if state == "PARKING_EXTEND_DAYS":
        try:
            days = int(text.strip())
        except ValueError:
            bot.send_message(message.chat.id, "Send number of days:")
            return
        extend_parking(message, data.get("parking_id"), days)
        return

    # DOT STATES
    if state == "DOT_CHOOSE_TYPE":
        if "Truck" in text:
            set_state(user_id, "DOT_SEARCH_UNIT", {"unit_type": "TRUCK"})
            bot.send_message(message.chat.id, "Enter truck number (e.g. GL1234):")
        elif "Trailer" in text:
            set_state(user_id, "DOT_SEARCH_UNIT", {"unit_type": "TRAILER"})
            bot.send_message(message.chat.id, "Enter trailer number (e.g. T1234):")
        return

    if state == "DOT_SEARCH_UNIT":
        unit_number = text.upper()
        unit_type = data.get("unit_type", "TRUCK")
        set_state(user_id, "DOT_WAIT_EXPIRY", {"unit_number": unit_number, "unit_type": unit_type})
        bot.send_message(message.chat.id,
                         f"Unit: <b>{h(unit_number)}</b>\nEnter DOT expiry date (MM/DD/YYYY):")
        return

    if state == "DOT_WAIT_EXPIRY":
        try:
            exp_dt = datetime.strptime(text, "%m/%d/%Y")
        except ValueError:
            bot.send_message(message.chat.id, "Invalid format. Use MM/DD/YYYY:")
            return
        set_state(user_id, "DOT_WAIT_PHOTO",
                  {**data, "expiry_date": exp_dt.strftime("%Y-%m-%d")})
        bot.send_message(message.chat.id, "Now send the photo of the DOT document:")
        return

    # UNIT STATES
    if state == "UNIT_ADD_NUMBER":
        unit_number = text.upper()
        unit_type = "TRAILER" if unit_number.startswith("T") else "TRUCK"
        set_state(user_id, "UNIT_ADD_YEAR_MODEL", {"unit_number": unit_number, "unit_type": unit_type})
        bot.send_message(message.chat.id,
                         f"Adding {'🚜 Trailer' if unit_type == 'TRAILER' else '🚛 Truck'} <b>{h(unit_number)}</b>\n\n"
                         f"Enter Year & Model (e.g. 2026 FREIGHTLINER CASCADIA):")
        return

    if state == "UNIT_ADD_YEAR_MODEL":
        set_state(user_id, "UNIT_ADD_VIN", {**data, "year_model": text.upper()})
        bot.send_message(message.chat.id, "Enter VIN number (17 characters):")
        return

    if state == "UNIT_ADD_VIN":
        if len(text.strip()) != 17:
            bot.send_message(message.chat.id, "VIN must be exactly 17 characters. Try again:")
            return
        set_state(user_id, "UNIT_ADD_PLATE", {**data, "vin": text.strip().upper()})
        bot.send_message(message.chat.id, "Enter plate number:")
        return

    if state == "UNIT_ADD_PLATE":
        set_state(user_id, "UNIT_ADD_STATE", {**data, "plate": text.strip().upper()})
        bot.send_message(message.chat.id, "Enter state of registration (e.g. INDIANA):")
        return

    if state == "UNIT_ADD_STATE":
        if data.get("unit_type") == "TRUCK":
            save_unit(message, {**data, "state": text.strip().upper()})
        else:
            set_state(user_id, "UNIT_ADD_TRAILER_TYPE", {**data, "state": text.strip().upper()})
            kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
            kb.row("DRY VAN", "REEFER", "FLATBED")
            kb.row("STEP DECK", "LOWBOY", "TANKER")
            bot.send_message(message.chat.id, "Select trailer type:", reply_markup=kb)
        return

    if state == "UNIT_ADD_TRAILER_TYPE":
        set_state(user_id, "UNIT_ADD_TRAILER_LENGTH", {**data, "trailer_type": text.upper()})
        kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
        kb.row("53ft", "48ft", "45ft", "40ft")
        bot.send_message(message.chat.id, "Select trailer length:", reply_markup=kb)
        return

    if state == "UNIT_ADD_TRAILER_LENGTH":
        save_unit(message, {**data, "trailer_length": text})
        return

    if state == "UNIT_SEARCH":
        show_unit_search_results(message.chat.id, user_id, text)
        return

    # ADMIN STATES
    if state == "ADMIN_ADD_WAIT_ID":
        handle_add_admin(message, text)
        return

    if state == "ADMIN_REMOVE_WAIT_ID":
        handle_remove_admin(message, text)
        return

    # ===== NO STATE — MENU ACTIONS =====
    actions = {
        "Missing PTI today": lambda: send_missing_pti_today(message.chat.id),
        "Missing PTI week": lambda: send_missing_pti_week(message.chat.id),
        "Send PTI reminder": lambda: send_pti_reminder_today(message.chat.id),
        "Planned units": lambda: show_planned_units(message.chat.id),
        "Send reminder to planned units": lambda: send_reminder_to_planned_units(message.chat.id),
        "Groups": lambda: show_all_groups(message.chat.id),
        "Active cases": lambda: show_active_cases(message.chat.id),
        "All cases history": lambda: show_cases_history(message.chat.id),
        "Active parkings": lambda: show_active_parkings(message.chat.id),
        "Home time requests": lambda: show_home_time_requests(message.chat.id),
        "Parking history": lambda: show_parking_history(message.chat.id),
        "Expiring DOT docs": lambda: show_expiring_dot(message.chat.id),
        "List admins": lambda: show_admins_list(message.chat.id),
    }

    if text in actions:
        actions[text]()
        return

    state_actions = {
        "Driver report": ("DRIVER_REPORT_WAIT_NAME", "Send driver name:"),
        "Add unit to plan": ("PM_ADD_UNIT_WAIT_SEARCH", "Send unit or part of group title:"),
        "Delete group": ("DELETE_GROUP_WAIT_SEARCH", "Send part of group title to delete:"),
        "Add parking": ("PARKING_ADD_UNIT", "Enter unit number (e.g. GL1234 or T5678):"),
        "Add unit": ("UNIT_ADD_NUMBER", "Enter unit number (e.g. GL1234 or T5678):"),
        "Search unit": ("UNIT_SEARCH", "Enter unit number or part of it:"),
        "Add admin": ("ADMIN_ADD_WAIT_ID", "Send Telegram user ID of new admin:"),
        "Remove admin": ("ADMIN_REMOVE_WAIT_ID", "Send Telegram user ID to remove:"),
    }

    if text in state_actions:
        new_state, prompt = state_actions[text]
        set_state(user_id, new_state)
        bot.send_message(message.chat.id, prompt)
        return

    if text == "Reports":
        set_state(user_id, "PM_REPORT_CHOOSE_MONTH")
        kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
        kb.row("January", "February", "March")
        kb.row("April", "May", "June")
        kb.row("July", "August", "September")
        kb.row("October", "November", "December")
        kb.row("Back to main menu")
        bot.send_message(message.chat.id, "Choose month for PM report:", reply_markup=kb)
        return

    if text == "Add DOT document":
        kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
        kb.row("🚛 Truck GL####", "🚜 Trailer T####")
        kb.row("Back to main menu")
        set_state(user_id, "DOT_CHOOSE_TYPE")
        bot.send_message(message.chat.id, "Search by truck or trailer?", reply_markup=kb)
        return

    bot.send_message(message.chat.id, "Choose from the menu.", reply_markup=kb_main_menu())


@bot.message_handler(func=lambda m: m.chat.type == "private" and not is_admin(m.from_user.id))
def handle_non_admin(message):
    bot.send_message(message.chat.id, f"This bot works only for admins.\nContact: {h(FLEET_CONTACT)}")


# =========================
# PTI FUNCTIONS
# =========================
def send_missing_pti_today(chat_id):
    try:
        today = get_et_now().strftime("%Y-%m-%d")
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT id, title FROM groups")
            groups = cur.fetchall()
            missing = []
            for g in groups:
                cur.execute("SELECT 1 FROM pti_reports WHERE group_id=? AND date_et=? LIMIT 1", (g["id"], today))
                if not cur.fetchone():
                    missing.append(g["title"])
        if not missing:
            bot.send_message(chat_id, "✅ All groups sent PTI today!")
            return
        msg = f"🔴 <b>Missing PTI today ({len(missing)} groups):</b>\n" + "".join(f"• {h(t)}\n" for t in missing)
        bot.send_message(chat_id, msg)
    except Exception as e:
        print(f"[ERROR] send_missing_pti_today: {repr(e)}")


def send_missing_pti_week(chat_id):
    try:
        today = get_et_now().date()
        week_ago = today - timedelta(days=6)
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT id, title FROM groups")
            groups = cur.fetchall()
            missing = []
            for g in groups:
                cur.execute("""SELECT 1 FROM pti_reports WHERE group_id=? AND date_et BETWEEN ? AND ? LIMIT 1""",
                            (g["id"], week_ago.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d")))
                if not cur.fetchone():
                    missing.append(g["title"])
        if not missing:
            bot.send_message(chat_id, "✅ All groups sent PTI this week!")
            return
        msg = f"🔴 <b>Missing PTI this week ({len(missing)} groups):</b>\n" + "".join(f"• {h(t)}\n" for t in missing)
        bot.send_message(chat_id, msg)
    except Exception as e:
        print(f"[ERROR] send_missing_pti_week: {repr(e)}")


def send_pti_reminder_today(chat_id):
    try:
        today = get_et_now().strftime("%Y-%m-%d")
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT id, title FROM groups")
            groups = cur.fetchall()
            missing = []
            for g in groups:
                cur.execute("SELECT 1 FROM pti_reports WHERE group_id=? AND date_et=? LIMIT 1", (g["id"], today))
                if not cur.fetchone():
                    missing.append({"id": g["id"], "title": g["title"]})
        if not missing:
            bot.send_message(chat_id, "✅ All groups sent PTI today!")
            return
        sent = 0
        for g in missing:
            try:
                bot.send_message(g["id"], "🔔 Reminder: Please send today's PTI with /pti command. Thank you!")
                sent += 1
            except Exception as e:
                log_send_error(f"PTI reminder -> {g['id']}", e)
        lines = [f"✅ PTI reminder sent to <b>{sent}</b> groups:\n"]
        lines += [f"• {h(g['title'])}" for g in missing]
        bot.send_message(chat_id, "\n".join(lines))
    except Exception as e:
        print(f"[ERROR] send_pti_reminder_today: {repr(e)}")


def show_driver_search_results(chat_id, user_id, query):
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT DISTINCT driver_name FROM pti_reports WHERE driver_name LIKE ? ORDER BY driver_name",
                        (f"%{query}%",))
            rows = cur.fetchall()
        if not rows:
            bot.send_message(chat_id, "No drivers found. Try again.")
            clear_state(user_id)
            return
        kb = types.InlineKeyboardMarkup()
        for r in rows:
            did = get_or_create_driver_id(r["driver_name"])
            if did > 0:
                kb.add(types.InlineKeyboardButton(h(r["driver_name"]), callback_data=f"driverreportid:{did}"))
        bot.send_message(chat_id, "Choose driver:", reply_markup=kb)
        clear_state(user_id)
    except Exception as e:
        print(f"[ERROR] show_driver_search_results: {repr(e)}")
        clear_state(user_id)


def build_driver_report_text(driver_name):
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT timestamp_et FROM pti_reports WHERE driver_name=? ORDER BY timestamp_et",
                        (driver_name,))
            rows = cur.fetchall()
        if not rows:
            return f"No PTI reports for <b>{h(driver_name)}</b>."
        total = len(rows)
        by_month = {}
        for r in rows:
            if not r["timestamp_et"]:
                continue
            try:
                dt = datetime.strptime(r["timestamp_et"], "%Y-%m-%d %H:%M:%S")
                key = (dt.year, dt.month)
                by_month.setdefault(key, []).append(dt)
            except:
                continue
        lines = [f"<b>{h(driver_name)}</b> — total <b>{total}</b> reports\n"]
        for (year, month) in sorted(by_month.keys()):
            dts = by_month[(year, month)]
            month_name = datetime(year, month, 1).strftime("%B")
            lines.append(f"<b>{h(month_name)} {year} — {len(dts)} reports</b>")
            for i, dt in enumerate(dts, 1):
                lines.append(f"{i}) {dt.strftime('%Y-%m-%d %H:%M')}")
            lines.append("")
        return "\n".join(lines)
    except Exception as e:
        return f"Error building report."


# =========================
# PM FUNCTIONS
# =========================
def show_planned_units(chat_id):
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("""SELECT p.id, g.title FROM pm_plans p JOIN groups g ON p.group_id=g.id
                          WHERE p.is_done=0 ORDER BY p.created_at""")
            rows = cur.fetchall()
        if not rows:
            bot.send_message(chat_id, "✅ No units in PM plan.")
            return
        for r in rows:
            kb = types.InlineKeyboardMarkup()
            kb.add(
                types.InlineKeyboardButton("✅ Done", callback_data=f"pm_done:{r['id']}"),
                types.InlineKeyboardButton("❌ Delete", callback_data=f"pm_delete:{r['id']}")
            )
            bot.send_message(chat_id, f"🔧 <b>{h(r['title'])}</b>", reply_markup=kb)
    except Exception as e:
        print(f"[ERROR] show_planned_units: {repr(e)}")


def show_pm_add_unit_search_results(chat_id, user_id, query):
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT id, title FROM groups WHERE title LIKE ? ORDER BY title", (f"%{query}%",))
            rows = cur.fetchall()
        if not rows:
            bot.send_message(chat_id, "No groups found.")
            clear_state(user_id)
            return
        kb = types.InlineKeyboardMarkup()
        for r in rows:
            kb.add(types.InlineKeyboardButton(h(r["title"]), callback_data=f"pm_add_plan:{r['id']}"))
        bot.send_message(chat_id, "Choose unit:", reply_markup=kb)
        clear_state(user_id)
    except Exception as e:
        print(f"[ERROR] show_pm_add_unit_search_results: {repr(e)}")
        clear_state(user_id)


def send_reminder_to_planned_units(chat_id):
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("""SELECT p.id, g.id AS group_id, g.title FROM pm_plans p
                          JOIN groups g ON p.group_id=g.id WHERE p.is_done=0""")
            rows = cur.fetchall()
        if not rows:
            bot.send_message(chat_id, "✅ No planned units.")
            return
        cnt = 0
        for r in rows:
            try:
                bot.send_message(r["group_id"], "🔔 Reminder: Your unit is in PM plan. Schedule maintenance ASAP.")
                cnt += 1
            except Exception as e:
                log_send_error(f"PM reminder -> {r['group_id']}", e)
        bot.send_message(chat_id, f"✅ Reminder sent to <b>{cnt}</b> units.")
    except Exception as e:
        print(f"[ERROR] send_reminder_to_planned_units: {repr(e)}")


def handle_pm_done_amount(message, plan_id):
    if not plan_id:
        bot.send_message(message.chat.id, "Internal error.")
        return
    user_id = message.from_user.id
    text = (message.text or "").strip()
    if text.lower() == "cancel":
        clear_state(user_id)
        bot.send_message(message.chat.id, "Cancelled.", reply_markup=kb_pm_plans_menu())
        return
    try:
        amount = float(text.replace("$", "").strip())
    except ValueError:
        bot.send_message(message.chat.id, "Send amount as number, e.g. 546.13")
        return
    try:
        with get_db() as conn:
            cur = conn.cursor()
            now_str = get_et_now().strftime("%Y-%m-%d %H:%M:%S")
            cur.execute("UPDATE pm_plans SET is_done=1, done_at=?, amount=? WHERE id=?",
                        (now_str, amount, plan_id))
        clear_state(user_id)
        bot.send_message(message.chat.id, f"✅ PM done!\nAmount: <b>{amount:.2f}$</b>",
                         reply_markup=kb_pm_plans_menu())
    except Exception as e:
        print(f"[ERROR] handle_pm_done_amount: {repr(e)}")


def handle_pm_report_month_choice(message, month_name):
    user_id = message.from_user.id
    if month_name == "Back to main menu":
        clear_state(user_id)
        bot.send_message(message.chat.id, "Main menu:", reply_markup=kb_main_menu())
        return
    month_map = {"January": 1, "February": 2, "March": 3, "April": 4, "May": 5, "June": 6,
                 "July": 7, "August": 8, "September": 9, "October": 10, "November": 11, "December": 12}
    m = month_map.get(month_name)
    if not m:
        bot.send_message(message.chat.id, "Choose month from buttons.")
        return
    year = get_et_now().year
    month_str = f"{m:02d}"
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("""SELECT p.done_at, p.amount, g.title FROM pm_plans p
                          JOIN groups g ON p.group_id=g.id
                          WHERE p.is_done=1 AND substr(p.done_at,1,4)=? AND substr(p.done_at,6,2)=?
                          ORDER BY p.done_at""", (str(year), month_str))
            rows = cur.fetchall()
        if not rows:
            bot.send_message(message.chat.id, f"No PMs for {h(month_name)} {year}.")
            clear_state(user_id)
            bot.send_message(message.chat.id, "PM menu:", reply_markup=kb_pm_menu())
            return
        total = sum(r["amount"] or 0 for r in rows)
        lines = [f"<b>{h(month_name)} {year}</b> — {len(rows)} PM done\n"]
        for r in rows:
            if not r["done_at"]:
                continue
            try:
                dt = datetime.strptime(r["done_at"], "%Y-%m-%d %H:%M:%S")
                lines.append(f"• {h(r['title'])} — {dt.strftime('%d %b')} — {float(r['amount'] or 0):.2f}$")
            except:
                continue
        lines.append(f"\n💰 Total: <b>{total:.2f}$</b>")
        bot.send_message(message.chat.id, "\n".join(lines))

        filename = os.path.join(tempfile.gettempdir(), f"pm_report_{year}_{month_str}.csv")
        try:
            with open(filename, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["Group", "Date", "Amount"])
                for r in rows:
                    if not r["done_at"]:
                        continue
                    try:
                        dt = datetime.strptime(r["done_at"], "%Y-%m-%d %H:%M:%S")
                        writer.writerow([r["title"], dt.strftime("%Y-%m-%d"), f"{float(r['amount'] or 0):.2f}"])
                    except:
                        continue
            with open(filename, "rb") as f:
                bot.send_document(message.chat.id, f, caption=f"PM report {h(month_name)} {year}")
        except Exception as e:
            print(f"[ERROR] CSV: {repr(e)}")
        finally:
            if os.path.exists(filename):
                os.remove(filename)

        clear_state(user_id)
        bot.send_message(message.chat.id, "PM menu:", reply_markup=kb_pm_menu())
    except Exception as e:
        print(f"[ERROR] handle_pm_report_month_choice: {repr(e)}")
        clear_state(user_id)


# =========================
# CASES FUNCTIONS
# =========================
def show_active_cases(chat_id):
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM cases WHERE status != 'DONE' ORDER BY created_at DESC LIMIT 20")
            rows = cur.fetchall()
        if not rows:
            bot.send_message(chat_id, "✅ No active cases!")
            return
        for r in rows:
            emoji = "🆕" if r["status"] == "NEW" else "🔄"
            msg = (f"{emoji} <b>Case #{r['id']:04d}</b> — {h(r['status'])}\n"
                   f"🚛 {h(r['group_title'])}\n"
                   f"👤 {h(r['driver_name'])}\n"
                   f"📝 {h(r['message'])}\n"
                   f"🕒 {r['created_at']}")
            bot.send_message(chat_id, msg)
    except Exception as e:
        print(f"[ERROR] show_active_cases: {repr(e)}")


def show_cases_history(chat_id):
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM cases ORDER BY created_at DESC LIMIT 30")
            rows = cur.fetchall()
        if not rows:
            bot.send_message(chat_id, "No cases yet.")
            return
        lines = ["<b>Cases history (last 30):</b>\n"]
        for r in rows:
            emoji = "✅" if r["status"] == "DONE" else ("🔄" if r["status"] == "IN PROCESS" else "🆕")
            msg_preview = (r["message"] or "")[:30]
            lines.append(f"{emoji} #{r['id']:04d} — {h(r['group_title'])} — {h(msg_preview)}")
        bot.send_message(chat_id, "\n".join(lines))
    except Exception as e:
        print(f"[ERROR] show_cases_history: {repr(e)}")


# =========================
# PARKING FUNCTIONS
# =========================
def save_parking(message, data, end_date_str):
    user_id = message.from_user.id
    unit_number = data.get("unit_number", "")
    unit_type = "TRAILER" if unit_number.startswith("T") else "TRUCK"
    location = data.get("location", "")
    start_date = data.get("start_date", "")
    home_group_id = data.get("home_request_group_id")

    try:
        start_dt = datetime.strptime(start_date, "%m/%d/%Y")
        end_dt = datetime.strptime(end_date_str, "%m/%d/%Y")
        now_str = get_et_now().strftime("%Y-%m-%d %H:%M:%S")

        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO parking (unit_number, unit_type, location, start_date, end_date,
                    status, created_at, created_by, home_request_group_id)
                VALUES (?, ?, ?, ?, ?, 'ACTIVE', ?, ?, ?)
            """, (unit_number, unit_type, location,
                  start_dt.strftime("%Y-%m-%d"), end_dt.strftime("%Y-%m-%d"),
                  now_str, user_id, home_group_id))

        clear_state(user_id)
        bot.send_message(message.chat.id,
                         f"✅ Parking added!\n\n"
                         f"🚛 Unit: <b>{h(unit_number)}</b>\n"
                         f"📍 Location: <b>{h(location)}</b>\n"
                         f"📅 {start_date} → {end_date_str}",
                         reply_markup=kb_parking_menu())

        if home_group_id:
            try:
                bot.send_message(home_group_id,
                                 f"🏠 Home time parking confirmed!\n"
                                 f"📍 Location: <b>{h(location)}</b>\n"
                                 f"📅 {start_date} → {end_date_str}")
            except:
                pass
    except Exception as e:
        print(f"[ERROR] save_parking: {repr(e)}")
        bot.send_message(message.chat.id, "Error saving parking.")
        clear_state(user_id)


def extend_parking(message, parking_id, days):
    user_id = message.from_user.id
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT end_date FROM parking WHERE id=?", (parking_id,))
            row = cur.fetchone()
            if not row:
                bot.send_message(message.chat.id, "Parking not found.")
                clear_state(user_id)
                return
            old_end = datetime.strptime(row["end_date"], "%Y-%m-%d")
            new_end = old_end + timedelta(days=days)
            cur.execute("""UPDATE parking SET end_date=?,
                alert_24h_sent=0, alert_12h_sent=0, alert_2h_sent=0, alert_expired_sent=0
                WHERE id=?""", (new_end.strftime("%Y-%m-%d"), parking_id))

        clear_state(user_id)
        bot.send_message(message.chat.id,
                         f"✅ Extended by {days} days.\nNew end: <b>{new_end.strftime('%m/%d/%Y')}</b>",
                         reply_markup=kb_parking_menu())

        for admin_id in get_all_admin_ids():
            if admin_id != user_id:
                try:
                    username = message.from_user.username or message.from_user.first_name
                    bot.send_message(admin_id,
                                     f"🔄 Parking #{parking_id} extended by {days} days → {new_end.strftime('%m/%d/%Y')} by @{h(username)}")
                except:
                    pass
    except Exception as e:
        print(f"[ERROR] extend_parking: {repr(e)}")
        bot.send_message(message.chat.id, "Error extending parking.")
        clear_state(user_id)


def show_active_parkings(chat_id):
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM parking WHERE status='ACTIVE' ORDER BY end_date")
            rows = cur.fetchall()
        if not rows:
            bot.send_message(chat_id, "✅ No active parkings.")
            return
        for r in rows:
            try:
                end_dt = datetime.strptime(r["end_date"], "%Y-%m-%d")
                days_left = (end_dt.date() - get_et_now().date()).days
                if days_left < 0:
                    status_text = f"⛔ EXPIRED {abs(days_left)} days ago"
                elif days_left == 0:
                    status_text = "⚠️ EXPIRES TODAY"
                else:
                    status_text = f"✅ {days_left} days left"
            except:
                status_text = ""

            msg = (f"🅿️ <b>Parking #{r['id']}</b>\n"
                   f"🚛 Unit: <b>{h(r['unit_number'])}</b> ({h(r['unit_type'])})\n"
                   f"📍 Location: <b>{h(r['location'])}</b>\n"
                   f"📅 {r['start_date']} → {r['end_date']}\n"
                   f"{status_text}")
            kb = types.InlineKeyboardMarkup()
            kb.row(
                types.InlineKeyboardButton("🔄 Extend", callback_data=f"parking_extend:{r['id']}"),
                types.InlineKeyboardButton("✅ Picked Up", callback_data=f"parking_pickedup:{r['id']}")
            )
            bot.send_message(chat_id, msg, reply_markup=kb)
    except Exception as e:
        print(f"[ERROR] show_active_parkings: {repr(e)}")


def show_home_time_requests(chat_id):
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("""SELECT * FROM parking WHERE home_request_group_id IS NOT NULL
                          AND status='ACTIVE' ORDER BY created_at DESC LIMIT 10""")
            rows = cur.fetchall()
        if not rows:
            bot.send_message(chat_id, "No home time parkings.")
            return
        for r in rows:
            msg = (f"🏠 <b>Home Time #{r['id']}</b>\n"
                   f"🚛 Unit: <b>{h(r['unit_number'])}</b>\n"
                   f"📍 Location: <b>{h(r['location'])}</b>\n"
                   f"📅 {r['start_date']} → {r['end_date']}")
            bot.send_message(chat_id, msg)
    except Exception as e:
        print(f"[ERROR] show_home_time_requests: {repr(e)}")


def show_parking_history(chat_id):
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM parking ORDER BY created_at DESC LIMIT 20")
            rows = cur.fetchall()
        if not rows:
            bot.send_message(chat_id, "No parking history.")
            return
        lines = ["<b>Parking history (last 20):</b>\n"]
        for r in rows:
            emoji = "✅" if r["status"] == "CLOSED" else "🅿️"
            lines.append(f"{emoji} {h(r['unit_number'])} — {h(r['location'])} — {r['end_date']}")
        bot.send_message(chat_id, "\n".join(lines))
    except Exception as e:
        print(f"[ERROR] show_parking_history: {repr(e)}")


# =========================
# DOT FUNCTIONS
# =========================
def handle_dot_photo(message, data):
    user_id = message.from_user.id
    if not message.photo:
        bot.send_message(message.chat.id, "Please send a photo.")
        return

    file_id = message.photo[-1].file_id
    unit_number = data.get("unit_number", "")
    unit_type = data.get("unit_type", "TRUCK")
    expiry_date = data.get("expiry_date", "")

    try:
        now_str = get_et_now().strftime("%Y-%m-%d %H:%M:%S")
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT id FROM dot_documents WHERE unit_number=?", (unit_number,))
            existing = cur.fetchone()
            if existing:
                cur.execute("""UPDATE dot_documents SET expiry_date=?, photo_file_id=?, unit_type=?, updated_at=?
                              WHERE unit_number=?""", (expiry_date, file_id, unit_type, now_str, unit_number))
            else:
                cur.execute("""INSERT INTO dot_documents (unit_number, unit_type, expiry_date, photo_file_id, created_at, updated_at)
                              VALUES (?, ?, ?, ?, ?, ?)""", (unit_number, unit_type, expiry_date, file_id, now_str, now_str))

        clear_state(user_id)
        try:
            exp_dt = datetime.strptime(expiry_date, "%Y-%m-%d")
            exp_display = exp_dt.strftime("%m/%d/%Y")
        except:
            exp_display = expiry_date

        bot.send_message(message.chat.id,
                         f"✅ DOT document saved!\n"
                         f"🚛 Unit: <b>{h(unit_number)}</b>\n"
                         f"📅 Expires: <b>{exp_display}</b>",
                         reply_markup=kb_dot_menu())
    except Exception as e:
        print(f"[ERROR] handle_dot_photo: {repr(e)}")
        bot.send_message(message.chat.id, "Error saving DOT document.")
        clear_state(user_id)


def show_expiring_dot(chat_id):
    try:
        today = get_et_now().date()
        deadline = today + timedelta(days=30)
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM dot_documents WHERE expiry_date <= ? ORDER BY expiry_date",
                        (deadline.strftime("%Y-%m-%d"),))
            rows = cur.fetchall()
        if not rows:
            bot.send_message(chat_id, "✅ No DOT docs expiring in 30 days.")
            return
        msg = f"<b>⚠️ DOT expiring within 30 days ({len(rows)}):</b>\n\n"
        for r in rows:
            try:
                exp_dt = datetime.strptime(r["expiry_date"], "%Y-%m-%d")
                days_left = (exp_dt.date() - today).days
                if days_left < 0:
                    status = f"⛔ EXPIRED {abs(days_left)}d ago"
                elif days_left == 0:
                    status = "⚠️ TODAY"
                else:
                    status = f"{days_left}d left"
                msg += f"• <b>{h(r['unit_number'])}</b> — {exp_dt.strftime('%m/%d/%Y')} ({status})\n"
            except:
                continue
        bot.send_message(chat_id, msg)
    except Exception as e:
        print(f"[ERROR] show_expiring_dot: {repr(e)}")


# =========================
# UNITS FUNCTIONS
# =========================
def save_unit(message, data):
    user_id = message.from_user.id
    unit_number = data.get("unit_number", "")
    unit_type = data.get("unit_type", "TRUCK")
    now_str = get_et_now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT id FROM units WHERE unit_number=?", (unit_number,))
            existing = cur.fetchone()
            if existing:
                cur.execute("""UPDATE units SET unit_type=?, year_model=?, vin=?, plate=?, state=?,
                              trailer_type=?, trailer_length=?, updated_at=? WHERE unit_number=?""",
                            (unit_type, data.get("year_model"), data.get("vin"), data.get("plate"),
                             data.get("state"), data.get("trailer_type"), data.get("trailer_length"),
                             now_str, unit_number))
                action = "updated"
            else:
                cur.execute("""INSERT INTO units (unit_number, unit_type, year_model, vin, plate, state,
                              trailer_type, trailer_length, created_at, updated_at)
                              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                            (unit_number, unit_type, data.get("year_model"), data.get("vin"),
                             data.get("plate"), data.get("state"), data.get("trailer_type"),
                             data.get("trailer_length"), now_str, now_str))
                action = "saved"

        clear_state(user_id)
        bot.send_message(message.chat.id,
                         f"✅ Unit <b>{h(unit_number)}</b> {action}!",
                         reply_markup=kb_units_menu())
    except Exception as e:
        print(f"[ERROR] save_unit: {repr(e)}")
        bot.send_message(message.chat.id, "Error saving unit.")
        clear_state(user_id)


def show_unit_search_results(chat_id, user_id, query):
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT unit_number, unit_type FROM units WHERE unit_number LIKE ? ORDER BY unit_number",
                        (f"%{query.upper()}%",))
            rows = cur.fetchall()
        if not rows:
            bot.send_message(chat_id, "No units found.")
            clear_state(user_id)
            return
        kb = types.InlineKeyboardMarkup()
        for r in rows:
            emoji = "🚛" if r["unit_type"] == "TRUCK" else "🚜"
            kb.add(types.InlineKeyboardButton(f"{emoji} {r['unit_number']}",
                                               callback_data=f"unit_view:{r['unit_number']}"))
        bot.send_message(chat_id, "Found units:", reply_markup=kb)
        clear_state(user_id)
    except Exception as e:
        print(f"[ERROR] show_unit_search_results: {repr(e)}")
        clear_state(user_id)


# =========================
# ADMINS FUNCTIONS
# =========================
def show_admins_list(chat_id):
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT user_id, username, added_at FROM admins")
            rows = cur.fetchall()
        if not rows:
            bot.send_message(chat_id, "No admins.")
            return
        lines = [f"<b>Admins ({len(rows)}):</b>\n"]
        for r in rows:
            username = f"@{r['username']}" if r["username"] else "no username"
            lines.append(f"• <code>{r['user_id']}</code> — {h(username)}")
        bot.send_message(chat_id, "\n".join(lines))
    except Exception as e:
        print(f"[ERROR] show_admins_list: {repr(e)}")


def handle_add_admin(message, text):
    user_id = message.from_user.id
    if not text.strip().isdigit():
        bot.send_message(message.chat.id, "Send a valid numeric Telegram user ID:")
        return
    new_id = int(text.strip())
    try:
        now_str = get_et_now().strftime("%Y-%m-%d %H:%M:%S")
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("INSERT OR IGNORE INTO admins (user_id, added_at) VALUES (?, ?)", (new_id, now_str))
        clear_state(user_id)
        bot.send_message(message.chat.id, f"✅ <code>{new_id}</code> added as admin.",
                         reply_markup=kb_admins_menu())
    except Exception as e:
        print(f"[ERROR] handle_add_admin: {repr(e)}")
        bot.send_message(message.chat.id, "Error.")
        clear_state(user_id)


def handle_remove_admin(message, text):
    user_id = message.from_user.id
    if not text.strip().isdigit():
        bot.send_message(message.chat.id, "Send a valid numeric Telegram user ID:")
        return
    remove_id = int(text.strip())
    if remove_id == user_id:
        bot.send_message(message.chat.id, "❌ You cannot remove yourself.")
        return
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM admins WHERE user_id=?", (remove_id,))
        clear_state(user_id)
        bot.send_message(message.chat.id, f"✅ <code>{remove_id}</code> removed from admins.",
                         reply_markup=kb_admins_menu())
    except Exception as e:
        print(f"[ERROR] handle_remove_admin: {repr(e)}")
        bot.send_message(message.chat.id, "Error.")
        clear_state(user_id)


# =========================
# MONTHLY REPORT
# =========================
def handle_monthly_report_menu(message):
    user_id = message.from_user.id
    set_state(user_id, "MONTHLY_REPORT_CHOOSE_MONTH")
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row("January", "February", "March")
    kb.row("April", "May", "June")
    kb.row("July", "August", "September")
    kb.row("October", "November", "December")
    kb.row("Back to main menu")
    bot.send_message(message.chat.id, "Choose month:", reply_markup=kb)


def handle_monthly_report_generate(message, month_name):
    user_id = message.from_user.id
    if month_name == "Back to main menu":
        clear_state(user_id)
        bot.send_message(message.chat.id, "Main menu:", reply_markup=kb_main_menu())
        return

    month_map = {"January": 1, "February": 2, "March": 3, "April": 4, "May": 5, "June": 6,
                 "July": 7, "August": 8, "September": 9, "October": 10, "November": 11, "December": 12}
    m = month_map.get(month_name)
    if not m:
        bot.send_message(message.chat.id, "Choose from buttons.")
        return

    year = get_et_now().year
    month_str = f"{m:02d}"
    month_start = f"{year}-{month_str}-01"
    month_end = f"{year}-{m+1:02d}-01" if m < 12 else f"{year+1}-01-01"

    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) as cnt FROM cases WHERE created_at >= ? AND created_at < ?",
                        (month_start, month_end))
            cases_opened = cur.fetchone()["cnt"]
            cur.execute("SELECT COUNT(*) as cnt FROM cases WHERE status='DONE' AND updated_at >= ? AND updated_at < ?",
                        (month_start, month_end))
            cases_closed = cur.fetchone()["cnt"]
            cases_pending = max(0, cases_opened - cases_closed)
            cur.execute("SELECT COUNT(*) as cnt FROM pm_plans WHERE is_done=1 AND done_at >= ? AND done_at < ?",
                        (month_start, month_end))
            pm_count = cur.fetchone()["cnt"]

        report_text = (
            f"📊 <b>MONTHLY REPORT — {h(month_name.upper())} {year}</b>\n\n"
            f"📋 <b>Cases:</b>\n"
            f"  • Opened: <b>{cases_opened}</b>\n"
            f"  • Closed: <b>{cases_closed}</b>\n"
            f"  • Pending: <b>{cases_pending}</b>\n\n"
            f"🔧 <b>PM Service (oil change):</b>\n"
            f"  • Trucks sent: <b>{pm_count}</b>"
        )
        bot.send_message(message.chat.id, report_text)

        filename = os.path.join(tempfile.gettempdir(), f"monthly_report_{year}_{month_str}.csv")
        try:
            with open(filename, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["Month", "Year", "Cases Opened", "Cases Closed", "Cases Pending", "PM Services"])
                writer.writerow([month_name, year, cases_opened, cases_closed, cases_pending, pm_count])
            with open(filename, "rb") as f:
                bot.send_document(message.chat.id, f,
                                  caption=f"📊 {h(month_name)} {year} — ready for AI analysis")
        except Exception as e:
            print(f"[ERROR] monthly CSV: {repr(e)}")
        finally:
            if os.path.exists(filename):
                os.remove(filename)

        clear_state(user_id)
        bot.send_message(message.chat.id, "Main menu:", reply_markup=kb_main_menu())
    except Exception as e:
        print(f"[ERROR] handle_monthly_report_generate: {repr(e)}")
        bot.send_message(message.chat.id, "Error generating report.")
        clear_state(user_id)


# =========================
# BROADCAST & GROUPS
# =========================
def send_broadcast(chat_id, text, group_ids=None):
    try:
        with get_db() as conn:
            cur = conn.cursor()
            if group_ids is None:
                cur.execute("SELECT id, title FROM groups")
                rows = cur.fetchall()
            else:
                if not group_ids:
                    bot.send_message(chat_id, "No groups selected.")
                    return
                placeholders = ",".join("?" * len(group_ids))
                cur.execute(f"SELECT id, title FROM groups WHERE id IN ({placeholders})", group_ids)
                rows = cur.fetchall()
        sent = 0
        for g in rows:
            try:
                bot.send_message(g["id"], text)
                sent += 1
            except Exception as e:
                log_send_error(f"Broadcast -> {g['id']}", e)
        bot.send_message(chat_id, f"✅ Sent to <b>{sent}</b> groups.")
    except Exception as e:
        print(f"[ERROR] send_broadcast: {repr(e)}")


def show_all_groups(chat_id):
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT id, title FROM groups ORDER BY title")
            rows = cur.fetchall()
        if not rows:
            bot.send_message(chat_id, "No groups registered.")
            return
        lines = [f"<b>Groups ({len(rows)}):</b>\n"]
        for r in rows:
            lines.append(f"• <code>{h(r['title'])}</code>  id: <code>{r['id']}</code>")
            if len("\n".join(lines)) > 3500:
                bot.send_message(chat_id, "\n".join(lines))
                lines = []
        if lines:
            bot.send_message(chat_id, "\n".join(lines))
    except Exception as e:
        print(f"[ERROR] show_all_groups: {repr(e)}")


def show_groups_mono(chat_id):
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT id, title FROM groups ORDER BY title")
            rows = cur.fetchall()
        if not rows:
            bot.send_message(chat_id, "No groups yet.")
            return
        lines = ["<b>Groups (copy IDs):</b>\n"]
        for r in rows:
            lines.append(f"• <code>{h(r['title'])}</code>  <code>{r['id']}</code>")
            if len("\n".join(lines)) > 3500:
                bot.send_message(chat_id, "\n".join(lines))
                lines = []
        if lines:
            bot.send_message(chat_id, "\n".join(lines))
    except Exception as e:
        print(f"[ERROR] show_groups_mono: {repr(e)}")


def show_delete_group_search_results(chat_id, user_id, query):
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT id, title FROM groups WHERE title LIKE ? ORDER BY title", (f"%{query}%",))
            rows = cur.fetchall()
        if not rows:
            bot.send_message(chat_id, "No groups found.")
            clear_state(user_id)
            return
        kb = types.InlineKeyboardMarkup()
        for r in rows:
            kb.add(types.InlineKeyboardButton(h(r["title"]), callback_data=f"delgroup:{r['id']}"))
        bot.send_message(chat_id, "Choose group to delete:", reply_markup=kb)
        clear_state(user_id)
    except Exception as e:
        print(f"[ERROR] show_delete_group_search_results: {repr(e)}")
        clear_state(user_id)


# =========================
# CALLBACKS
# =========================
@bot.callback_query_handler(func=lambda c: c.data.startswith("driverreportid:"))
def cb_driver_report(call):
    try:
        bot.answer_callback_query(call.id)
        driver_id = int(call.data.split(":", 1)[1])
        name = get_driver_name_by_id(driver_id)
        if not name:
            bot.send_message(call.message.chat.id, "Driver not found.")
            return
        bot.send_message(call.message.chat.id, build_driver_report_text(name))
    except Exception as e:
        print(f"[ERROR] cb_driver_report: {repr(e)}")


@bot.callback_query_handler(func=lambda c: c.data.startswith("pm_add_plan:"))
def cb_pm_add_plan(call):
    try:
        group_id = int(call.data.split(":", 1)[1])
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT title FROM groups WHERE id=?", (group_id,))
            g = cur.fetchone()
            if not g:
                bot.answer_callback_query(call.id, "Group not found.")
                return
            cur.execute("SELECT 1 FROM pm_plans WHERE group_id=? AND is_done=0 LIMIT 1", (group_id,))
            if cur.fetchone():
                bot.answer_callback_query(call.id, "Already in PM plan.")
                return
            now_str = get_et_now().strftime("%Y-%m-%d %H:%M:%S")
            cur.execute("INSERT INTO pm_plans (group_id, created_at, is_done) VALUES (?, ?, 0)", (group_id, now_str))
        bot.answer_callback_query(call.id, "Added!")
        bot.send_message(call.message.chat.id, f"✅ <b>{h(g['title'])}</b> added to PM plan.")
    except Exception as e:
        print(f"[ERROR] cb_pm_add_plan: {repr(e)}")
        bot.answer_callback_query(call.id, "Error.")


@bot.callback_query_handler(func=lambda c: c.data.startswith("pm_done:"))
def cb_pm_done(call):
    try:
        plan_id = int(call.data.split(":", 1)[1])
        user_id = call.from_user.id
        if not is_admin(user_id):
            bot.answer_callback_query(call.id, "Admins only.")
            return
        set_state(user_id, "PM_DONE_WAIT_AMOUNT", {"plan_id": plan_id})
        kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
        kb.row("Cancel")
        bot.answer_callback_query(call.id)
        bot.send_message(call.message.chat.id, "Send PM amount (e.g. 546.13):", reply_markup=kb)
    except Exception as e:
        print(f"[ERROR] cb_pm_done: {repr(e)}")
        bot.answer_callback_query(call.id, "Error.")


@bot.callback_query_handler(func=lambda c: c.data.startswith("pm_delete:"))
def cb_pm_delete(call):
    try:
        plan_id = int(call.data.split(":", 1)[1])
        if not is_admin(call.from_user.id):
            bot.answer_callback_query(call.id, "Admins only.")
            return
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM pm_plans WHERE id=?", (plan_id,))
        bot.answer_callback_query(call.id, "Deleted.")
        bot.send_message(call.message.chat.id, "❌ PM plan deleted.")
    except Exception as e:
        print(f"[ERROR] cb_pm_delete: {repr(e)}")
        bot.answer_callback_query(call.id, "Error.")


@bot.callback_query_handler(func=lambda c: c.data.startswith("delgroup:"))
def cb_del_group(call):
    try:
        if not is_admin(call.from_user.id):
            bot.answer_callback_query(call.id, "Admins only.")
            return
        group_id = int(call.data.split(":", 1)[1])
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM pti_reports WHERE group_id=?", (group_id,))
            cur.execute("DELETE FROM pm_plans WHERE group_id=?", (group_id,))
            cur.execute("DELETE FROM groups WHERE id=?", (group_id,))
        bot.answer_callback_query(call.id, "Deleted.")
        bot.send_message(call.message.chat.id, "✅ Group deleted.")
    except Exception as e:
        print(f"[ERROR] cb_del_group: {repr(e)}")
        bot.answer_callback_query(call.id, "Error.")


@bot.callback_query_handler(func=lambda c: c.data.startswith("case_process:"))
def cb_case_process(call):
    try:
        case_id = int(call.data.split(":", 1)[1])
        user = call.from_user
        username = f"@{user.username}" if user.username else user.first_name
        now_str = get_et_now().strftime("%Y-%m-%d %H:%M:%S")
        now_et = get_et_now()

        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM cases WHERE id=?", (case_id,))
            case = cur.fetchone()
            if not case:
                bot.answer_callback_query(call.id, "Case not found.")
                return
            cur.execute("UPDATE cases SET status='IN PROCESS', updated_at=?, updated_by=? WHERE id=?",
                        (now_str, username, case_id))

        new_text = (f"🔄 <b>CASE #{case_id:04d} — IN PROCESS</b>\n"
                    f"🚛 Group: <b>{h(case['group_title'])}</b>\n"
                    f"👤 Driver: <b>{h(case['driver_name'])}</b>\n"
                    f"📝 <b>{h(case['message'])}</b>\n\n"
                    f"🔄 Taken by: <b>{h(username)}</b> at {now_et.strftime('%H:%M %d/%m/%Y')}")

        kb = types.InlineKeyboardMarkup()
        kb.add(types.InlineKeyboardButton("✅ DONE", callback_data=f"case_done:{case_id}"))
        try:
            bot.edit_message_text(new_text, CASES_GROUP_ID, case["cases_message_id"], reply_markup=kb)
        except:
            pass
        bot.answer_callback_query(call.id, "Marked IN PROCESS ✅")
    except Exception as e:
        print(f"[ERROR] cb_case_process: {repr(e)}")
        bot.answer_callback_query(call.id, "Error.")


@bot.callback_query_handler(func=lambda c: c.data.startswith("case_done:"))
def cb_case_done(call):
    try:
        case_id = int(call.data.split(":", 1)[1])
        user = call.from_user
        username = f"@{user.username}" if user.username else user.first_name
        now_str = get_et_now().strftime("%Y-%m-%d %H:%M:%S")
        now_et = get_et_now()

        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM cases WHERE id=?", (case_id,))
            case = cur.fetchone()
            if not case:
                bot.answer_callback_query(call.id, "Case not found.")
                return
            cur.execute("UPDATE cases SET status='DONE', updated_at=?, updated_by=? WHERE id=?",
                        (now_str, username, case_id))

        new_text = (f"✅ <b>CASE #{case_id:04d} — DONE</b>\n"
                    f"🚛 Group: <b>{h(case['group_title'])}</b>\n"
                    f"👤 Driver: <b>{h(case['driver_name'])}</b>\n"
                    f"📝 <b>{h(case['message'])}</b>\n\n"
                    f"✅ Closed by: <b>{h(username)}</b> at {now_et.strftime('%H:%M %d/%m/%Y')}")
        try:
            bot.edit_message_text(new_text, CASES_GROUP_ID, case["cases_message_id"])
        except:
            pass

        try:
            bot.send_message(case["group_id"], f"✅ Your case #{case_id:04d} has been resolved by fleet!")
        except:
            pass

        bot.answer_callback_query(call.id, "Case DONE ✅")
    except Exception as e:
        print(f"[ERROR] cb_case_done: {repr(e)}")
        bot.answer_callback_query(call.id, "Error.")


@bot.callback_query_handler(func=lambda c: c.data.startswith("home_add_parking:"))
def cb_home_add_parking(call):
    try:
        if not is_admin(call.from_user.id):
            bot.answer_callback_query(call.id, "Admins only.")
            return
        group_id = int(call.data.split(":", 1)[1])
        user_id = call.from_user.id

        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT unit_code FROM groups WHERE id=?", (group_id,))
            g = cur.fetchone()

        unit_hint = g["unit_code"] if g and g["unit_code"] else ""
        set_state(user_id, "PARKING_ADD_UNIT", {"home_request_group_id": group_id})
        bot.answer_callback_query(call.id)
        bot.send_message(call.message.chat.id,
                         f"Adding home time parking.\n"
                         f"Enter unit number{f' (e.g. {unit_hint})' if unit_hint else ''}:")
    except Exception as e:
        print(f"[ERROR] cb_home_add_parking: {repr(e)}")
        bot.answer_callback_query(call.id, "Error.")


@bot.callback_query_handler(func=lambda c: c.data.startswith("home_ignore:"))
def cb_home_ignore(call):
    try:
        bot.answer_callback_query(call.id, "Request ignored.")
        try:
            bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id)
        except:
            pass
    except Exception as e:
        print(f"[ERROR] cb_home_ignore: {repr(e)}")


@bot.callback_query_handler(func=lambda c: c.data.startswith("parking_extend:"))
def cb_parking_extend(call):
    try:
        if not is_admin(call.from_user.id):
            bot.answer_callback_query(call.id, "Admins only.")
            return
        parking_id = int(call.data.split(":", 1)[1])
        user_id = call.from_user.id
        set_state(user_id, "PARKING_EXTEND_DAYS", {"parking_id": parking_id})
        bot.answer_callback_query(call.id)
        bot.send_message(call.message.chat.id, "How many days to extend?")
    except Exception as e:
        print(f"[ERROR] cb_parking_extend: {repr(e)}")
        bot.answer_callback_query(call.id, "Error.")


@bot.callback_query_handler(func=lambda c: c.data.startswith("parking_pickedup:"))
def cb_parking_pickedup(call):
    try:
        if not is_admin(call.from_user.id):
            bot.answer_callback_query(call.id, "Admins only.")
            return
        parking_id = int(call.data.split(":", 1)[1])
        now_str = get_et_now().strftime("%Y-%m-%d %H:%M:%S")
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("UPDATE parking SET status='CLOSED', closed_at=? WHERE id=?", (now_str, parking_id))
        bot.answer_callback_query(call.id, "Picked up ✅")
        bot.send_message(call.message.chat.id, f"✅ Parking #{parking_id} closed — unit picked up!")

        user_id = call.from_user.id
        username = f"@{call.from_user.username}" if call.from_user.username else call.from_user.first_name
        for admin_id in get_all_admin_ids():
            if admin_id != user_id:
                try:
                    bot.send_message(admin_id, f"✅ Parking #{parking_id} closed by {h(username)}")
                except:
                    pass
    except Exception as e:
        print(f"[ERROR] cb_parking_pickedup: {repr(e)}")
        bot.answer_callback_query(call.id, "Error.")


@bot.callback_query_handler(func=lambda c: c.data.startswith("unit_view:"))
def cb_unit_view(call):
    try:
        unit_number = call.data.split(":", 1)[1]
        bot.answer_callback_query(call.id)
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM units WHERE unit_number=?", (unit_number,))
            unit = cur.fetchone()
            cur.execute("SELECT expiry_date FROM dot_documents WHERE unit_number=? ORDER BY expiry_date DESC LIMIT 1",
                        (unit_number,))
            dot_row = cur.fetchone()

        if not unit:
            bot.send_message(call.message.chat.id, "Unit not found.")
            return

        if unit["unit_type"] == "TRUCK":
            dot_info = ""
            if dot_row:
                try:
                    exp_dt = datetime.strptime(dot_row["expiry_date"], "%Y-%m-%d")
                    days_left = (exp_dt.date() - get_et_now().date()).days
                    dot_info = f"\n🔍 DOT Expires: <code>{exp_dt.strftime('%m/%d/%Y')}</code> ({days_left}d)"
                except:
                    pass
            msg = (f"🚛 <b>{h(unit['unit_number'])}</b>\n"
                   f"📅 {h(unit['year_model'])}\n"
                   f"🔑 VIN: <code>{h(unit['vin'])}</code>\n"
                   f"🔢 Plate: <code>{h(unit['plate'])}</code>\n"
                   f"📍 <code>{h(unit['state'])}</code>{dot_info}")
        else:
            msg = (f"🚜 <b>{h(unit['unit_number'])}</b>\n"
                   f"📅 {h(unit['year_model'])}\n"
                   f"📏 {h(unit['trailer_type'])} {h(unit['trailer_length'])}\n"
                   f"🔑 VIN: <code>{h(unit['vin'])}</code>\n"
                   f"🔢 Plate: <code>{h(unit['plate'])}</code>\n"
                   f"📍 <code>{h(unit['state'])}</code>")

        bot.send_message(call.message.chat.id, msg)
    except Exception as e:
        print(f"[ERROR] cb_unit_view: {repr(e)}")


# =========================
# SCHEDULED JOBS
# =========================
def job_parking_alerts():
    try:
        now = get_et_now()
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM parking WHERE status='ACTIVE'")
            rows = cur.fetchall()

        for r in rows:
            try:
                end_dt = datetime.strptime(r["end_date"], "%Y-%m-%d").replace(hour=23, minute=59)
                end_dt = ET_TZ.localize(end_dt)
                diff_hours = (end_dt - now).total_seconds() / 3600
                parking_id = r["id"]
                unit = h(r["unit_number"])
                location = h(r["location"])
                end_display = datetime.strptime(r["end_date"], "%Y-%m-%d").strftime("%m/%d/%Y")

                def alert(label, field, pid=parking_id, u=unit, loc=location, ed=end_display):
                    msg = (f"⚠️ <b>PARKING — {label}</b>\n"
                           f"🚛 Unit: <b>{u}</b>\n"
                           f"📍 Location: <b>{loc}</b>\n"
                           f"📅 Expires: <b>{ed}</b>")
                    kb = types.InlineKeyboardMarkup()
                    kb.row(
                        types.InlineKeyboardButton("🔄 Extend", callback_data=f"parking_extend:{pid}"),
                        types.InlineKeyboardButton("✅ Picked Up", callback_data=f"parking_pickedup:{pid}")
                    )
                    for admin_id in get_all_admin_ids():
                        try:
                            bot.send_message(admin_id, msg, reply_markup=kb)
                        except:
                            pass
                    with get_db() as conn2:
                        conn2.execute(f"UPDATE parking SET {field}=1 WHERE id=?", (pid,))

                if 0 < diff_hours <= 24 and not r["alert_24h_sent"]:
                    alert("24 HOURS LEFT", "alert_24h_sent")
                if 0 < diff_hours <= 12 and not r["alert_12h_sent"]:
                    alert("12 HOURS LEFT", "alert_12h_sent")
                if 0 < diff_hours <= 2 and not r["alert_2h_sent"]:
                    alert("2 HOURS LEFT", "alert_2h_sent")
                if diff_hours <= 0 and not r["alert_expired_sent"]:
                    msg = (f"⛔ <b>PARKING EXPIRED!</b>\n"
                           f"🚛 Unit: <b>{unit}</b>\n"
                           f"📍 Location: <b>{location}</b>\n"
                           f"📅 Was due: <b>{end_display}</b>")
                    kb = types.InlineKeyboardMarkup()
                    kb.row(
                        types.InlineKeyboardButton("🔄 Extend", callback_data=f"parking_extend:{parking_id}"),
                        types.InlineKeyboardButton("✅ Picked Up", callback_data=f"parking_pickedup:{parking_id}")
                    )
                    for admin_id in get_all_admin_ids():
                        try:
                            bot.send_message(admin_id, msg, reply_markup=kb)
                        except:
                            pass
                    with get_db() as conn2:
                        conn2.execute("UPDATE parking SET alert_expired_sent=1 WHERE id=?", (parking_id,))

            except Exception as e:
                print(f"[ERROR] parking alert for #{r['id']}: {repr(e)}")
    except Exception as e:
        print(f"[ERROR] job_parking_alerts: {repr(e)}")


def job_dot_alerts():
    try:
        today = get_et_now().date()
        deadline = today + timedelta(days=30)
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM dot_documents WHERE expiry_date <= ?", (deadline.strftime("%Y-%m-%d"),))
            rows = cur.fetchall()
        if not rows:
            return
        lines = ["⚠️ <b>DOT EXPIRY ALERT</b>\n"]
        for r in rows:
            try:
                exp_dt = datetime.strptime(r["expiry_date"], "%Y-%m-%d")
                days_left = (exp_dt.date() - today).days
                status = f"⛔ EXPIRED {abs(days_left)}d ago" if days_left < 0 else ("⚠️ TODAY" if days_left == 0 else f"{days_left}d left")
                lines.append(f"• <b>{h(r['unit_number'])}</b> — {exp_dt.strftime('%m/%d/%Y')} — {status}")
            except:
                continue
        if len(lines) <= 1:
            return
        full_msg = "\n".join(lines)
        for admin_id in get_all_admin_ids():
            try:
                bot.send_message(admin_id, full_msg)
            except:
                pass
    except Exception as e:
        print(f"[ERROR] job_dot_alerts: {repr(e)}")


def job_daily_parking_summary():
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM parking WHERE status='ACTIVE' ORDER BY end_date")
            rows = cur.fetchall()
        if not rows:
            return
        today = get_et_now().date()
        lines = [f"🅿️ <b>DAILY PARKING SUMMARY — {today.strftime('%m/%d/%Y')}</b>\nActive: <b>{len(rows)}</b>\n"]
        for r in rows:
            try:
                end_dt = datetime.strptime(r["end_date"], "%Y-%m-%d")
                days_left = (end_dt.date() - today).days
                status = "⛔ EXPIRED" if days_left < 0 else ("⚠️ TODAY" if days_left == 0 else f"{days_left}d")
                lines.append(f"• {h(r['unit_number'])} — {h(r['location'])} — {end_dt.strftime('%m/%d/%Y')} ({status})")
            except:
                continue
        msg = "\n".join(lines)
        for admin_id in get_all_admin_ids():
            try:
                bot.send_message(admin_id, msg)
            except:
                pass
    except Exception as e:
        print(f"[ERROR] job_daily_parking_summary: {repr(e)}")


def job_daily_pti_reminder():
    try:
        today = get_et_now().strftime("%Y-%m-%d")
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT id, title FROM groups")
            groups = cur.fetchall()
            missing = []
            for g in groups:
                cur.execute("SELECT 1 FROM pti_reports WHERE group_id=? AND date_et=? LIMIT 1", (g["id"], today))
                if not cur.fetchone():
                    missing.append(g)
        for g in missing:
            try:
                bot.send_message(g["id"], "🔔 Daily reminder: Please send today's PTI with /pti command!")
            except:
                pass
    except Exception as e:
        print(f"[ERROR] job_daily_pti_reminder: {repr(e)}")


# =========================
# HEALTH SERVER
# =========================
def start_health_server():
    port = int(os.getenv("PORT", "8080"))

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write(b"GWE Fleet Bot OK")

        def log_message(self, format, *args):
            return

    server = HTTPServer(("0.0.0.0", port), Handler)
    server.serve_forever()


# =========================
# SCHEDULER
# =========================
def start_scheduler():
    scheduler = BackgroundScheduler(timezone=ET_TZ)
    scheduler.add_job(job_parking_alerts, "interval", minutes=30)
    scheduler.add_job(job_dot_alerts, "cron", hour=9, minute=0)
    scheduler.add_job(job_dot_alerts, "cron", hour=16, minute=0)
    scheduler.add_job(job_daily_parking_summary, "cron", hour=8, minute=0)
    scheduler.add_job(job_daily_pti_reminder, "cron", hour=15, minute=0)
    scheduler.start()
    print("[SCHEDULER] Started.")
    return scheduler


# =========================
# MAIN
# =========================
if __name__ == "__main__":
    init_db()
    threading.Thread(target=start_health_server, daemon=True).start()
    start_scheduler()
    print("🚛 GWE Fleet Bot is running!")

    while True:
        try:
            bot.infinity_polling(timeout=60, long_polling_timeout=60)
        except Exception as e:
            print(f"[POLLING_ERROR] {repr(e)}")
            print("[INFO] Reconnecting in 5 seconds...")
            time.sleep(5)
