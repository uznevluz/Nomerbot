"""
==================================================================
 NOMER BOT — bitta faylga birlashtirilgan versiya
 (Raqam / Telegram Stars / Telegram Premium sotish boti, SmmUpper
 Hamkorlik API v2 orqali; aiogram 3, SQLite yoki Postgres bilan)
==================================================================
"""
import asyncio
import contextlib
import functools
import html
import json
import logging
import math
import os
import random
import re
import signal
import sqlite3
import time
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Awaitable, Callable, Dict, NamedTuple, Optional

import aiohttp
import aiosqlite
import asyncpg
from aiogram import BaseMiddleware, Bot, Dispatcher, F, Router
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramRetryAfter
from aiogram.filters import Command, CommandStart, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.base import BaseStorage, StorageKey
from aiogram.types import (
    CallbackQuery,
    CopyTextButton,
    ErrorEvent,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    TelegramObject,
)
from aiohttp import web
from dotenv import load_dotenv

load_dotenv()


# ==============================================================
# SOZLAMALAR (config)
# ==============================================================
BOT_TOKEN = os.getenv("BOT_TOKEN", "")

ADMIN_IDS = [
    int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()
]

# Raqam / Stars / Premium sotib olinganda xabar yuboriladigan kanal.
# -100 bilan boshlanuvchi kanal ID yoki @kanal_username shaklida bo'lishi mumkin.
# Bot shu kanalga ADMIN sifatida (xabar yuborish huquqi bilan) qo'shilgan bo'lishi kerak.
# Bo'sh qoldirilsa — kanalga hech narsa yuborilmaydi.
_channel_id_raw = os.getenv("CHANNEL_ID", "").strip()
CHANNEL_ID = int(_channel_id_raw) if _channel_id_raw.lstrip("-").isdigit() else _channel_id_raw

SMMUPPER_API_KEY = os.getenv("SMMUPPER_API_KEY", "")
SMMUPPER_BASE_URL = "https://smmupper.uz/api/v2"

CARD_NUMBER = os.getenv("CARD_NUMBER", "0000 0000 0000 0000")
CARD_HOLDER = os.getenv("CARD_HOLDER", "F.I.SH.")

# Narxga qo'shiladigan foyda foizi (UMUMIY/standart qiymat). Masalan 10 -> SmmUpper narxiga +10%.
# Har bir bo'lim uchun ALOHIDA ustama ham qo'yish mumkin: admin panel -> Sozlamalar -> Narx ustamasi,
# yoki .env: MARKUP_PERCENT_NUMBER / MARKUP_PERCENT_STARS / MARKUP_PERCENT_PREMIUM / MARKUP_PERCENT_UC.
# Bo'lim uchun alohida qiymat bo'lmasa, shu umumiy foiz ishlatiladi.
MARKUP_PERCENT = float(os.getenv("MARKUP_PERCENT", "0"))

DB_PATH = os.getenv("DB_PATH", "bot.db")

# Ixtiyoriy: PostgreSQL manzili (masalan Neon.tech yoki Supabase'ning BEPUL
# tarifidan olingan). Sozlansa, bot SQLite o'rniga shu Postgres'ni ishlatadi —
# shunda Render qayta ishga tushganda ham balans/tarix o'chib ketmaydi.
# Bo'sh qoldirilsa, oldingidek SQLite (bot.db) ishlatiladi.
_database_url_raw = os.getenv("DATABASE_URL", "").strip()
if _database_url_raw.startswith("postgres://"):
    _database_url_raw = "postgresql://" + _database_url_raw[len("postgres://"):]
DATABASE_URL = _database_url_raw

# Raqam uchun SMS kod kelmasa, foydalanuvchi shuncha soniyadan keyin
# o'zi pulini qaytarib olishi mumkin (10 daqiqa).
REFUND_ELIGIBLE_SECONDS = 600

# Bitta foydalanuvchidan ketma-ket so'rovlar orasidagi eng kichik oraliq
# (soniyada) — spamni cheklash uchun.
THROTTLE_SECONDS = 0.7

# Majburiy obuna (ixtiyoriy): foydalanuvchi shu kanalga a'zo bo'lmasa,
# botdan foydalana olmaydi. Bo'sh qoldirilsa — majburiy obuna o'chiq.
# @kanal_username yoki -100 bilan boshlanuvchi kanal ID bo'lishi mumkin.
# Bot shu kanalga a'zolarni tekshira olishi uchun ADMIN qilib qo'shilgan
# bo'lishi kerak (kamida "a'zolarni ko'rish" huquqi bilan).
FORCE_SUB_CHANNEL = os.getenv("FORCE_SUB_CHANNEL", "").strip()

# Foydalanuvchi "Kanalga o'tish" tugmasini bosganda ochiladigan havola.
# Ochiq kanal bo'lsa bo'sh qoldirish mumkin — @kanal_username'dan avtomatik
# hosil qilinadi. Yopiq (private) kanal bo'lsa, taklif havolasini
# (https://t.me/+...) shu yerga qo'yish kerak.
FORCE_SUB_CHANNEL_URL = os.getenv("FORCE_SUB_CHANNEL_URL", "").strip()
if not FORCE_SUB_CHANNEL_URL and FORCE_SUB_CHANNEL.startswith("@"):
    FORCE_SUB_CHANNEL_URL = f"https://t.me/{FORCE_SUB_CHANNEL.lstrip('@')}"


# ==============================================================
# MA'LUMOTLAR BAZASI (database)
# ==============================================================
"""
Ombor qatlami: standart holatda SQLite, agar .env'da DATABASE_URL sozlansa —
PostgreSQL (masalan Neon.tech yoki Supabase'ning bepul tarifidan).

Nega ikkalasi ham bor: Render'ning bepul tarifida SQLite fayli deploylar orasida
saqlanib qolishi kafolatlanmagan (server qayta ishga tushganda o'chib ketishi
mumkin). DATABASE_URL sozlansa, bot avtomatik ravishda Postgres'ga o'tadi va
balans/buyurtmalar tarixi doimiy saqlanadi — boshqa hech narsani o'zgartirish
shart emas. Har ikkala rejimda ham funksiyalarning nomi/imzosi bir xil, shuning
uchun boshqa fayllar qaysi baza ishlatilayotganidan bexabar qolaveradi.
"""


_PG = bool(DATABASE_URL)
_pool: Optional["asyncpg.Pool"] = None  # faqat Postgres rejimida ishlatiladi


_SCHEMA_SQLITE = """
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    username TEXT,
    full_name TEXT,
    balance INTEGER NOT NULL DEFAULT 0,
    banned INTEGER NOT NULL DEFAULT 0,
    referred_by INTEGER,
    referral_earnings INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    order_type TEXT NOT NULL,          -- 'number' | 'stars' | 'premium'
    ref TEXT,                          -- getOrder uchun: order_id / hash_code / number / id
    server INTEGER,
    status TEXT NOT NULL DEFAULT 'processing',   -- processing | done | refunded
    price INTEGER NOT NULL,
    details TEXT,                      -- JSON: SmmUpper javobi
    country TEXT,                      -- ISO kod ('number' turidagi buyurtmalar uchun; TOP 10 statistikasi uchun
    created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS topups (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    amount INTEGER NOT NULL,
    photo_file_id TEXT,
    status TEXT NOT NULL DEFAULT 'pending',   -- pending | approved | rejected
    created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT
);

CREATE TABLE IF NOT EXISTS fsm_data (
    storage_key TEXT PRIMARY KEY,
    state TEXT,
    data TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS support_threads (
    admin_chat_id INTEGER NOT NULL,
    message_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    PRIMARY KEY (admin_chat_id, message_id)
);

CREATE INDEX IF NOT EXISTS ix_orders_user ON orders (user_id, id);
CREATE INDEX IF NOT EXISTS ix_orders_status ON orders (status, id);
CREATE INDEX IF NOT EXISTS ix_orders_type_status ON orders (order_type, status, country);
CREATE INDEX IF NOT EXISTS ix_topups_status ON topups (status, id);
CREATE INDEX IF NOT EXISTS ix_topups_user ON topups (user_id, status);
"""

_SCHEMA_PG = """
CREATE TABLE IF NOT EXISTS users (
    user_id BIGINT PRIMARY KEY,
    username TEXT,
    full_name TEXT,
    balance BIGINT NOT NULL DEFAULT 0,
    banned BOOLEAN NOT NULL DEFAULT FALSE,
    referred_by BIGINT,
    referral_earnings BIGINT NOT NULL DEFAULT 0,
    created_at BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    order_type TEXT NOT NULL,
    ref TEXT,
    server INTEGER,
    status TEXT NOT NULL DEFAULT 'processing',
    price BIGINT NOT NULL,
    details TEXT,
    country TEXT,
    created_at BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS topups (
    id SERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    amount BIGINT NOT NULL,
    photo_file_id TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT
);

CREATE TABLE IF NOT EXISTS fsm_data (
    storage_key TEXT PRIMARY KEY,
    state TEXT,
    data TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS support_threads (
    admin_chat_id BIGINT NOT NULL,
    message_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    PRIMARY KEY (admin_chat_id, message_id)
);

CREATE INDEX IF NOT EXISTS ix_orders_user ON orders (user_id, id);
CREATE INDEX IF NOT EXISTS ix_orders_status ON orders (status, id);
CREATE INDEX IF NOT EXISTS ix_orders_type_status ON orders (order_type, status, country);
CREATE INDEX IF NOT EXISTS ix_topups_status ON topups (status, id);
CREATE INDEX IF NOT EXISTS ix_topups_user ON topups (user_id, status);
"""


@contextlib.asynccontextmanager
async def _db_sqlite():
    """SQLite ulanishini ochadi va har safar `busy_timeout`ni o'rnatadi.
    Bu bot FSM holatini ham (har bir xabar/tugma bosishda!) shu bazaga
    yozgani uchun, ko'p foydalanuvchi bir vaqtda yozsa, standart SQLite
    "table is locked" xatosini darhol berib yuborishi mumkin edi —
    busy_timeout esa shunday holatda darhol xato bermasdan, bir necha
    soniya kutib, qulf ochilishini kutadi. WAL rejimi esa init_db()da
    BIR MARTA yoqiladi (u baza faylining o'zida saqlanib qoladi, har bir
    yangi ulanishda qayta o'rnatish shart emas)."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA busy_timeout = 5000")
        yield db


async def init_db():
    global _pool
    if _PG:
        _pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
        async with _pool.acquire() as conn:
            await conn.execute(_SCHEMA_PG)
            # Eski (allaqachon ishlab turgan) bazalarda ham "country" ustuni
            # paydo bo'lishi uchun — CREATE TABLE IF NOT EXISTS mavjud
            # jadvalni o'zgartirmaydi. Postgres 9.6+ IF NOT EXISTS'ni
            # qo'llab-quvvatlaydi, shu uchun bu amal xavfsiz va qayta-qayta
            # ishga tushirsa ham xato bermaydi.
            await conn.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS country TEXT")
            await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS referred_by BIGINT")
            await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS referral_earnings BIGINT NOT NULL DEFAULT 0")
            # target_username / qty: 'stars' va 'premium' buyurtmalarida kimga
            # va qancha (Stars soni yoki Premium oy soni) sotib olinganini
            # saqlaydi — "oxirgi buyurtmani takrorlash" funksiyasi uchun kerak
            # (SmmUpper javobida bu qiymatlar har doim aks etishiga tayanib
            # bo'lmaydi, shuning uchun o'zimiz alohida saqlaymiz).
            await conn.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS target_username TEXT")
            await conn.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS qty INTEGER")
            # last_flow_msg_id: foydalanuvchiga oxirgi marta yuborilgan "oqim
            # tugagan" xabarning (masalan, "balans yetarli emas") message ID'si
            # (bot faqat shaxsiy chatda ishlagani uchun chat_id == user_id,
            # alohida ustun shart emas). Foydalanuvchi YANGI xarid oqimini
            # boshlaganda, shu eski xabarning tugmalari avtomatik olib
            # tashlanadi — ekranda ishlamaydigan eski tugmalar "yopishib"
            # qolmasligi uchun.
            await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS last_flow_msg_id BIGINT")
    else:
        async with _db_sqlite() as db:
            # WAL rejimi baza faylida doimiy saqlanadi — shuning uchun bu
            # yerda faqat BIR MARTA (har safar botni ishga tushirganda)
            # o'rnatilsa kifoya, boshqa ulanishlar avtomatik shu rejimda
            # ishlayveradi.
            await db.execute("PRAGMA journal_mode=WAL")
            await db.executescript(_SCHEMA_SQLITE)
            try:
                await db.execute("ALTER TABLE orders ADD COLUMN country TEXT")
            except Exception:
                pass  # ustun allaqachon mavjud (eski baza) — SQLite'da "IF NOT EXISTS" yo'q
            try:
                await db.execute("ALTER TABLE users ADD COLUMN referred_by INTEGER")
            except Exception:
                pass
            try:
                await db.execute("ALTER TABLE users ADD COLUMN referral_earnings INTEGER NOT NULL DEFAULT 0")
            except Exception:
                pass
            try:
                await db.execute("ALTER TABLE orders ADD COLUMN target_username TEXT")
            except Exception:
                pass
            try:
                await db.execute("ALTER TABLE orders ADD COLUMN qty INTEGER")
            except Exception:
                pass
            try:
                await db.execute("ALTER TABLE users ADD COLUMN last_flow_msg_id INTEGER")
            except Exception:
                pass
            await db.commit()


async def ensure_user(user_id: int, username: Optional[str], full_name: Optional[str], referred_by: Optional[int] = None):
    """`referred_by` faqat foydalanuvchi ENDI birinchi marta yaratilayotganda
    o'rnatiladi — INSERT OR IGNORE / ON CONFLICT DO NOTHING tufayli, agar u
    allaqachon mavjud bo'lsa, bu qiymat e'tiborsiz qoldiriladi (referal
    keyinchalik boshqacha /start bosilsa ham o'zgarmay qoladi)."""
    if _PG:
        async with _pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO users (user_id, username, full_name, balance, created_at, referred_by) "
                "VALUES ($1, $2, $3, 0, $4, $5) ON CONFLICT (user_id) DO NOTHING",
                user_id, username, full_name, int(time.time()), referred_by,
            )
            await conn.execute(
                "UPDATE users SET username = $1, full_name = $2 WHERE user_id = $3",
                username, full_name, user_id,
            )
    else:
        async with _db_sqlite() as db:
            await db.execute(
                "INSERT OR IGNORE INTO users (user_id, username, full_name, balance, created_at, referred_by) "
                "VALUES (?, ?, ?, 0, ?, ?)",
                (user_id, username, full_name, int(time.time()), referred_by),
            )
            await db.execute(
                "UPDATE users SET username = ?, full_name = ? WHERE user_id = ?",
                (username, full_name, user_id),
            )
            await db.commit()


async def get_referrer(user_id: int) -> Optional[int]:
    """Foydalanuvchini taklif qilgan kishining ID sini qaytaradi (bo'lmasa None)."""
    if _PG:
        async with _pool.acquire() as conn:
            row = await conn.fetchrow("SELECT referred_by FROM users WHERE user_id = $1", user_id)
            return row["referred_by"] if row else None
    else:
        async with _db_sqlite() as db:
            cur = await db.execute("SELECT referred_by FROM users WHERE user_id = ?", (user_id,))
            row = await cur.fetchone()
            return row[0] if row else None


async def add_referral_earning(user_id: int, amount: int):
    """Referal keshbek berilganda shu foydalanuvchining jami keshbek
    hisobiga qo'shadi (statistika ko'rsatish uchun)."""
    if _PG:
        async with _pool.acquire() as conn:
            await conn.execute(
                "UPDATE users SET referral_earnings = referral_earnings + $1 WHERE user_id = $2",
                amount, user_id,
            )
    else:
        async with _db_sqlite() as db:
            await db.execute(
                "UPDATE users SET referral_earnings = referral_earnings + ? WHERE user_id = ?",
                (amount, user_id),
            )
            await db.commit()


async def get_referral_stats(user_id: int) -> dict:
    """{'invited': taklif qilingan do'stlar soni, 'earned': jami olingan keshbek}."""
    if _PG:
        async with _pool.acquire() as conn:
            invited = await conn.fetchval("SELECT COUNT(*) FROM users WHERE referred_by = $1", user_id)
            row = await conn.fetchrow("SELECT referral_earnings FROM users WHERE user_id = $1", user_id)
            earned = row["referral_earnings"] if row else 0
    else:
        async with _db_sqlite() as db:
            cur = await db.execute("SELECT COUNT(*) FROM users WHERE referred_by = ?", (user_id,))
            invited_row = await cur.fetchone()
            invited = invited_row[0] if invited_row else 0
            cur = await db.execute("SELECT referral_earnings FROM users WHERE user_id = ?", (user_id,))
            row = await cur.fetchone()
            earned = row[0] if row else 0
    return {"invited": invited or 0, "earned": earned or 0}


async def get_balance(user_id: int) -> int:
    if _PG:
        async with _pool.acquire() as conn:
            row = await conn.fetchrow("SELECT balance FROM users WHERE user_id = $1", user_id)
            return row["balance"] if row else 0
    else:
        async with _db_sqlite() as db:
            cur = await db.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,))
            row = await cur.fetchone()
            return row[0] if row else 0


async def change_balance(user_id: int, delta: int):
    if _PG:
        async with _pool.acquire() as conn:
            await conn.execute(
                "UPDATE users SET balance = balance + $1 WHERE user_id = $2", delta, user_id
            )
    else:
        async with _db_sqlite() as db:
            await db.execute(
                "UPDATE users SET balance = balance + ? WHERE user_id = ?", (delta, user_id)
            )
            await db.commit()


async def set_last_flow_msg(user_id: int, message_id: Optional[int]):
    """Foydalanuvchiga oxirgi marta yuborilgan "oqim tugadi" xabarining
    (masalan, balans yetmasligi haqidagi) message ID'sini saqlaydi.
    message_id=None berilsa — tozalaydi (masalan, xabar allaqachon
    tozalangandan keyin)."""
    if _PG:
        async with _pool.acquire() as conn:
            await conn.execute(
                "UPDATE users SET last_flow_msg_id = $1 WHERE user_id = $2", message_id, user_id,
            )
    else:
        async with _db_sqlite() as db:
            await db.execute(
                "UPDATE users SET last_flow_msg_id = ? WHERE user_id = ?", (message_id, user_id),
            )
            await db.commit()


async def get_last_flow_msg(user_id: int) -> Optional[int]:
    if _PG:
        async with _pool.acquire() as conn:
            row = await conn.fetchrow("SELECT last_flow_msg_id FROM users WHERE user_id = $1", user_id)
    else:
        async with _db_sqlite() as db:
            cur = await db.execute("SELECT last_flow_msg_id FROM users WHERE user_id = ?", (user_id,))
            row = await cur.fetchone()
    return row[0] if row else None


async def track_flow_msg(message) -> None:
    """Yuborilgan/tahrirlangan xabarni "hozirgi faol oqim xabari" sifatida
    belgilaydi. `insufficient_balance` kabi "oqim shu yerda to'xtaydi"
    xabarlaridan keyin chaqiriladi — shunda foydalanuvchi keyinroq YANGI
    xarid oqimini boshlasa, shu xabar avtomatik "tozalanadi" (tugmalari
    olib tashlanadi). Bot faqat shaxsiy chatda ishlagani uchun
    message.chat.id — bu aynan shu foydalanuvchining user_id'si."""
    if message is not None:
        await set_last_flow_msg(message.chat.id, message.message_id)


async def clear_stale_flow_message(bot, user_id: int) -> None:
    """Foydalanuvchi YANGI xarid oqimini (Raqam/Stars/Premium/Balans
    to'ldirish) boshlaganda chaqiriladi: agar oldingi, "tugagan" oqimdan
    qolgan xabar bo'lsa, uning tugmalarini olib tashlaydi — shunda eski,
    endi ishlamaydigan tugmalar chatda abadiy osilib qolmaydi. Xabarning
    o'zi (matni) qoladi — faqat tugmalar tozalanadi, chunki matnni
    o'chirish/tahrirlash bu yerda shart emas va xavfliroq (masalan, xabar
    juda eski bo'lsa Telegram tahrirlashga ruxsat bermaydi)."""
    message_id = await get_last_flow_msg(user_id)
    if not message_id:
        return
    try:
        await bot.edit_message_reply_markup(chat_id=user_id, message_id=message_id, reply_markup=None)
    except Exception:
        pass
    await set_last_flow_msg(user_id, None)


async def try_deduct_balance(user_id: int, amount: int) -> bool:
    """Balansdan `amount`ni ayiradi, FAQAT yetarli mablag' bo'lsa — bitta atomik
    amal orqali (tekshirish va ayirish bir vaqtda). Bu ikki xaridni bir vaqtda
    tasdiqlash orqali balansni "poyga holati" bilan minusga tushirib bo'lish
    imkonini yopadi. Muvaffaqiyatli bo'lsa True, mablag' yetarli bo'lmasa
    (yoki foydalanuvchi topilmasa) False qaytaradi.
    """
    if _PG:
        async with _pool.acquire() as conn:
            result = await conn.execute(
                "UPDATE users SET balance = balance - $1 WHERE user_id = $2 AND balance >= $1",
                amount, user_id,
            )
            return result.split()[-1] != "0"
    else:
        async with _db_sqlite() as db:
            cur = await db.execute(
                "UPDATE users SET balance = balance - ? WHERE user_id = ? AND balance >= ?",
                (amount, user_id, amount),
            )
            await db.commit()
            return cur.rowcount > 0


async def is_banned(user_id: int) -> bool:
    if _PG:
        async with _pool.acquire() as conn:
            row = await conn.fetchrow("SELECT banned FROM users WHERE user_id = $1", user_id)
            return bool(row["banned"]) if row else False
    else:
        async with _db_sqlite() as db:
            cur = await db.execute("SELECT banned FROM users WHERE user_id = ?", (user_id,))
            row = await cur.fetchone()
            return bool(row[0]) if row else False


async def set_banned(user_id: int, banned: bool):
    if _PG:
        async with _pool.acquire() as conn:
            await conn.execute("UPDATE users SET banned = $1 WHERE user_id = $2", banned, user_id)
    else:
        async with _db_sqlite() as db:
            await db.execute(
                "UPDATE users SET banned = ? WHERE user_id = ?", (1 if banned else 0, user_id)
            )
            await db.commit()


async def get_all_user_ids() -> list:
    if _PG:
        async with _pool.acquire() as conn:
            rows = await conn.fetch("SELECT user_id FROM users")
            return [r["user_id"] for r in rows]
    else:
        async with _db_sqlite() as db:
            cur = await db.execute("SELECT user_id FROM users")
            rows = await cur.fetchall()
            return [r[0] for r in rows]


async def find_user(identifier: str) -> Optional[dict]:
    """ID (raqam) yoki @username bo'yicha foydalanuvchini topadi."""
    identifier = identifier.strip().lstrip("@")
    if _PG:
        async with _pool.acquire() as conn:
            if identifier.isdigit():
                row = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", int(identifier))
            else:
                row = await conn.fetchrow(
                    "SELECT * FROM users WHERE lower(username) = lower($1)", identifier
                )
            if not row:
                return None
            orders_row = await conn.fetchrow(
                "SELECT COUNT(*) AS c, COALESCE(SUM(price) FILTER (WHERE status != 'refunded'), 0) AS total "
                "FROM orders WHERE user_id = $1",
                row["user_id"],
            )
            result = dict(row)
            result["order_count"] = orders_row["c"]
            result["total_spent"] = orders_row["total"]
            return result
    else:
        async with _db_sqlite() as db:
            db.row_factory = aiosqlite.Row
            if identifier.isdigit():
                cur = await db.execute("SELECT * FROM users WHERE user_id = ?", (int(identifier),))
            else:
                cur = await db.execute(
                    "SELECT * FROM users WHERE lower(username) = lower(?)", (identifier,)
                )
            row = await cur.fetchone()
            if not row:
                return None
            cur2 = await db.execute(
                "SELECT COUNT(*), COALESCE(SUM(CASE WHEN status != 'refunded' THEN price ELSE 0 END), 0) "
                "FROM orders WHERE user_id = ?",
                (row["user_id"],),
            )
            count_row = await cur2.fetchone()
            result = dict(row)
            result["order_count"] = count_row[0]
            result["total_spent"] = count_row[1]
            return result


async def create_order(user_id: int, order_type: str, ref: Optional[str], server: Optional[int],
                        price: int, details: dict, status: str = "processing",
                        country: Optional[str] = None, target_username: Optional[str] = None,
                        qty: Optional[int] = None) -> int:
    details_json = json.dumps(details, ensure_ascii=False)
    if _PG:
        async with _pool.acquire() as conn:
            row = await conn.fetchrow(
                "INSERT INTO orders (user_id, order_type, ref, server, status, price, details, country, "
                "target_username, qty, created_at) "
                "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) RETURNING id",
                user_id, order_type, ref, server, status, price, details_json, country,
                target_username, qty, int(time.time()),
            )
            return row["id"]
    else:
        async with _db_sqlite() as db:
            cur = await db.execute(
                "INSERT INTO orders (user_id, order_type, ref, server, status, price, details, country, "
                "target_username, qty, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (user_id, order_type, ref, server, status, price, details_json, country,
                 target_username, qty, int(time.time())),
            )
            await db.commit()
            return cur.lastrowid


async def top_countries(limit: int = 10, ready: Optional[bool] = None) -> list:
    """Eng ko'p buyurtma qilingan davlatlarni (kod, soni) juftliklari
    ro'yxati sifatida, kamayish tartibida qaytaradi. Faqat "country"
    ustuni saqlangan (ya'ni shu funksiya botga qo'shilgandan keyin
    qilingan) 'number' turidagi va bekor qilinmagan buyurtmalar
    hisobga olinadi. `ready`: None — hammasi; True — faqat "Tayyor akkaunt"
    (server 3); False — faqat "Oddiy raqam" (server 1/2)."""
    if ready is True:
        server_clause = "AND server = 3 "
    elif ready is False:
        server_clause = "AND COALESCE(server, 0) != 3 "
    else:
        server_clause = ""
    sql = (
        "SELECT country, COUNT(*) AS cnt FROM orders "
        "WHERE order_type = 'number' AND country IS NOT NULL AND status != 'refunded' "
        + server_clause
    )
    if _PG:
        async with _pool.acquire() as conn:
            rows = await conn.fetch(sql + "GROUP BY country ORDER BY cnt DESC LIMIT $1", limit)
            return [(r["country"], r["cnt"]) for r in rows]
    else:
        async with _db_sqlite() as db:
            cur = await db.execute(sql + "GROUP BY country ORDER BY cnt DESC LIMIT ?", (limit,))
            rows = await cur.fetchall()
            return [(r[0], r[1]) for r in rows]


async def top_spenders(limit: int = 10) -> list:
    """Eng ko'p pul sarflagan foydalanuvchilarni (jami to'lov, buyurtmalar
    soni, username/ism bilan birga) kamayish tartibida qaytaradi. Faqat
    bekor qilinmagan (status != 'refunded') buyurtmalar hisobga olinadi."""
    if _PG:
        async with _pool.acquire() as conn:
            return await conn.fetch(
                "SELECT o.user_id, COALESCE(SUM(o.price), 0) AS total, COUNT(*) AS cnt, "
                "u.username AS username, u.full_name AS full_name "
                "FROM orders o JOIN users u ON u.user_id = o.user_id "
                "WHERE o.status != 'refunded' "
                "GROUP BY o.user_id, u.username, u.full_name "
                "ORDER BY total DESC LIMIT $1",
                limit,
            )
    else:
        async with _db_sqlite() as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                "SELECT o.user_id, COALESCE(SUM(o.price), 0) AS total, COUNT(*) AS cnt, "
                "u.username AS username, u.full_name AS full_name "
                "FROM orders o JOIN users u ON u.user_id = o.user_id "
                "WHERE o.status != 'refunded' "
                "GROUP BY o.user_id, u.username, u.full_name "
                "ORDER BY total DESC LIMIT ?",
                (limit,),
            )
            return await cur.fetchall()


async def daily_revenue(days: int = 7) -> list:
    """Oxirgi `days` kunlik tushumni kun bo'yicha, eng eskisidan eng
    yangisigacha, [(kun_str, summa), ...] ro'yxati sifatida qaytaradi
    (kunlar Postgres/SQLite farqiga qaramay Python tomonida guruhlanadi,
    shunda ikkala baza uchun ham bir xil ishlaydi)."""
    since = int(time.time()) - days * 86400
    if _PG:
        async with _pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT created_at, price FROM orders WHERE status != 'refunded' AND created_at >= $1",
                since,
            )
            pairs = [(r["created_at"], r["price"]) for r in rows]
    else:
        async with _db_sqlite() as db:
            cur = await db.execute(
                "SELECT created_at, price FROM orders WHERE status != 'refunded' AND created_at >= ?",
                (since,),
            )
            pairs = await cur.fetchall()

    buckets: Dict[str, int] = {}
    for created_at, price in pairs:
        day = datetime.fromtimestamp(created_at).strftime("%m-%d")
        buckets[day] = buckets.get(day, 0) + price

    result = []
    now = int(time.time())
    for i in range(days - 1, -1, -1):
        day = datetime.fromtimestamp(now - i * 86400).strftime("%m-%d")
        result.append((day, buckets.get(day, 0)))
    return result


async def update_order_status(order_pk: int, status: str, details: Optional[dict] = None):
    if _PG:
        async with _pool.acquire() as conn:
            if details is not None:
                await conn.execute(
                    "UPDATE orders SET status = $1, details = $2 WHERE id = $3",
                    status, json.dumps(details, ensure_ascii=False), order_pk,
                )
            else:
                await conn.execute("UPDATE orders SET status = $1 WHERE id = $2", status, order_pk)
    else:
        async with _db_sqlite() as db:
            if details is not None:
                await db.execute(
                    "UPDATE orders SET status = ?, details = ? WHERE id = ?",
                    (status, json.dumps(details, ensure_ascii=False), order_pk),
                )
            else:
                await db.execute("UPDATE orders SET status = ? WHERE id = ?", (status, order_pk))
            await db.commit()


async def get_order_row(order_pk: int):
    if _PG:
        async with _pool.acquire() as conn:
            return await conn.fetchrow("SELECT * FROM orders WHERE id = $1", order_pk)
    else:
        async with _db_sqlite() as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("SELECT * FROM orders WHERE id = ?", (order_pk,))
            return await cur.fetchone()


async def list_orders(user_id: int, limit: int = 10):
    if _PG:
        async with _pool.acquire() as conn:
            return await conn.fetch(
                "SELECT * FROM orders WHERE user_id = $1 ORDER BY id DESC LIMIT $2", user_id, limit
            )
    else:
        async with _db_sqlite() as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                "SELECT * FROM orders WHERE user_id = ? ORDER BY id DESC LIMIT ?", (user_id, limit)
            )
            return await cur.fetchall()


async def get_recent_orders(limit: int = 15, status: Optional[str] = None) -> list:
    """Admin uchun: BARCHA foydalanuvchilarning eng oxirgi buyurtmalari,
    xohlasa muayyan status bo'yicha filtrlangan holda."""
    if _PG:
        async with _pool.acquire() as conn:
            if status:
                return await conn.fetch(
                    "SELECT * FROM orders WHERE status = $1 ORDER BY id DESC LIMIT $2", status, limit
                )
            return await conn.fetch("SELECT * FROM orders ORDER BY id DESC LIMIT $1", limit)
    else:
        async with _db_sqlite() as db:
            db.row_factory = aiosqlite.Row
            if status:
                cur = await db.execute(
                    "SELECT * FROM orders WHERE status = ? ORDER BY id DESC LIMIT ?", (status, limit)
                )
            else:
                cur = await db.execute("SELECT * FROM orders ORDER BY id DESC LIMIT ?", (limit,))
            return await cur.fetchall()


async def get_last_order(user_id: int):
    """Foydalanuvchining ENG OXIRGI (turi qanday bo'lishidan qat'i nazar)
    buyurtmasini qaytaradi — 'oxirgi buyurtmani takrorlash' funksiyasi
    uchun. Topilmasa None."""
    if _PG:
        async with _pool.acquire() as conn:
            return await conn.fetchrow(
                "SELECT * FROM orders WHERE user_id = $1 ORDER BY id DESC LIMIT 1", user_id
            )
    else:
        async with _db_sqlite() as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                "SELECT * FROM orders WHERE user_id = ? ORDER BY id DESC LIMIT 1", (user_id,)
            )
            return await cur.fetchone()


async def refund_order(order_pk: int, min_age_seconds: int = 0) -> Optional[dict]:
    """Buyurtmani 'refunded' holatiga o'tkazadi va puli balansga qaytariladi —
    FAQAT hali 'processing' holatida bo'lsa (shu tufayli ikki marta qaytarib
    bo'lmaydi — bu ham atomik shart bilan ta'minlanadi) va kamida
    min_age_seconds vaqt o'tgan bo'lsa. Muvaffaqiyatli bo'lsa
    {"user_id":.., "price":..} qaytaradi, aks holda None.
    """
    row = await get_order_row(order_pk)
    if not row or row["status"] != "processing":
        return None
    if min_age_seconds and (int(time.time()) - row["created_at"]) < min_age_seconds:
        return None

    if _PG:
        async with _pool.acquire() as conn:
            result = await conn.execute(
                "UPDATE orders SET status = 'refunded' WHERE id = $1 AND status = 'processing'",
                order_pk,
            )
            if result.split()[-1] == "0":
                return None
    else:
        async with _db_sqlite() as db:
            cur = await db.execute(
                "UPDATE orders SET status = 'refunded' WHERE id = ? AND status = 'processing'",
                (order_pk,),
            )
            await db.commit()
            if cur.rowcount == 0:
                return None

    await change_balance(row["user_id"], row["price"])
    return {"user_id": row["user_id"], "price": row["price"]}


async def create_topup(user_id: int, amount: int, photo_file_id: Optional[str]) -> int:
    if _PG:
        async with _pool.acquire() as conn:
            row = await conn.fetchrow(
                "INSERT INTO topups (user_id, amount, photo_file_id, status, created_at) "
                "VALUES ($1, $2, $3, 'pending', $4) RETURNING id",
                user_id, amount, photo_file_id, int(time.time()),
            )
            return row["id"]
    else:
        async with _db_sqlite() as db:
            cur = await db.execute(
                "INSERT INTO topups (user_id, amount, photo_file_id, status, created_at) "
                "VALUES (?, ?, ?, 'pending', ?)",
                (user_id, amount, photo_file_id, int(time.time())),
            )
            await db.commit()
            return cur.lastrowid


async def get_topup(topup_id: int):
    if _PG:
        async with _pool.acquire() as conn:
            return await conn.fetchrow("SELECT * FROM topups WHERE id = $1", topup_id)
    else:
        async with _db_sqlite() as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("SELECT * FROM topups WHERE id = ?", (topup_id,))
            return await cur.fetchone()


async def get_pending_topups(limit: int = 15) -> list:
    """Barcha kutilayotgan (pending) balans to'ldirish so'rovlarini,
    foydalanuvchi ma'lumoti (username/ism) bilan birga, eng eskisidan
    boshlab qaytaradi (birinchi bo'lib kelgan birinchi ko'rib chiqilsin)."""
    if _PG:
        async with _pool.acquire() as conn:
            return await conn.fetch(
                "SELECT t.id, t.user_id, t.amount, t.created_at, "
                "u.username AS username, u.full_name AS full_name "
                "FROM topups t LEFT JOIN users u ON u.user_id = t.user_id "
                "WHERE t.status = 'pending' ORDER BY t.id ASC LIMIT $1",
                limit,
            )
    else:
        async with _db_sqlite() as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                "SELECT t.id, t.user_id, t.amount, t.created_at, "
                "u.username AS username, u.full_name AS full_name "
                "FROM topups t LEFT JOIN users u ON u.user_id = t.user_id "
                "WHERE t.status = 'pending' ORDER BY t.id ASC LIMIT ?",
                (limit,),
            )
            return await cur.fetchall()


async def set_topup_status(topup_id: int, status: str, expected_current: Optional[str] = None) -> bool:
    """Topup holatini yangilaydi. `expected_current` berilsa — FAQAT topup
    HOZIR aynan shu holatda (masalan 'pending') bo'lsagina yangilaydi, bitta
    atomik amal bilan (tekshirish va yangilash bir vaqtda) — xuddi
    `try_deduct_balance`dagi kabi. Bu ikki admin BIR XIL so'rovni deyarli bir
    vaqtda tasdiqlashi orqali balans ikki marta qo'shilib ketishining oldini
    oladi. Muvaffaqiyatli yangilansa True; `expected_current` berilgan-u,
    lekin topup ALLAQACHON boshqa holatda bo'lsa (boshqa admin ulgurgan)
    False qaytaradi."""
    if _PG:
        async with _pool.acquire() as conn:
            if expected_current is not None:
                result = await conn.execute(
                    "UPDATE topups SET status = $1 WHERE id = $2 AND status = $3",
                    status, topup_id, expected_current,
                )
                return result.split()[-1] != "0"
            await conn.execute("UPDATE topups SET status = $1 WHERE id = $2", status, topup_id)
            return True
    else:
        async with _db_sqlite() as db:
            if expected_current is not None:
                cur = await db.execute(
                    "UPDATE topups SET status = ? WHERE id = ? AND status = ?",
                    (status, topup_id, expected_current),
                )
                await db.commit()
                return cur.rowcount > 0
            await db.execute("UPDATE topups SET status = ? WHERE id = ?", (status, topup_id))
            await db.commit()
            return True


async def set_topup_amount(topup_id: int, new_amount: int) -> bool:
    """Admin tasdiqlashdan oldin summani tuzatganda ishlatiladi — masalan
    foydalanuvchi 5 000 deb ko'rsatgan, lekin aslida kartaga faqat 4 000
    tushgan bo'lsa. Faqat so'rov HALI "pending" holatida bo'lsagina
    yangilanadi (bitta atomik amal, xuddi set_topup_status'dagi kabi) —
    aks holda boshqa admin AYNAN shu payt (juda kam ehtimol, lekin
    mumkin) so'rovni ESKI summa bilan allaqachon tasdiqlab ulgurgan
    bo'lishi, keyin esa bu yerda summa "orqadan" o'zgartirilib, jadvaldagi
    yozuv haqiqatda balansga tushgan summadan farq qilib qolishi mumkin
    edi. Muvaffaqiyatli yangilansa True, so'rov endi "pending" bo'lmasa
    (boshqa admin ulgurgan) False qaytaradi."""
    if _PG:
        async with _pool.acquire() as conn:
            result = await conn.execute(
                "UPDATE topups SET amount = $1 WHERE id = $2 AND status = 'pending'",
                new_amount, topup_id,
            )
            return result.split()[-1] != "0"
    else:
        async with _db_sqlite() as db:
            cur = await db.execute(
                "UPDATE topups SET amount = ? WHERE id = ? AND status = 'pending'",
                (new_amount, topup_id),
            )
            await db.commit()
            return cur.rowcount > 0


async def get_setting(key: str, default: Optional[str] = None) -> Optional[str]:
    """Botning ichki sozlamalarini (masalan SmmUpper API kaliti) o'qiydi —
    admin panel orqali .env'ga qayta kirmasdan o'zgartirish mumkin bo'lishi
    uchun. Baza bo'sh bo'lsa `default` qaytariladi."""
    if _PG:
        async with _pool.acquire() as conn:
            row = await conn.fetchrow("SELECT value FROM settings WHERE key = $1", key)
            return row["value"] if row else default
    else:
        async with _db_sqlite() as db:
            cur = await db.execute("SELECT value FROM settings WHERE key = ?", (key,))
            row = await cur.fetchone()
            return row[0] if row else default


async def set_setting(key: str, value: str):
    if _PG:
        async with _pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO settings (key, value) VALUES ($1, $2) "
                "ON CONFLICT (key) DO UPDATE SET value = $2",
                key, value,
            )
    else:
        async with _db_sqlite() as db:
            await db.execute(
                "INSERT INTO settings (key, value) VALUES (?, ?) "
                "ON CONFLICT (key) DO UPDATE SET value = ?",
                (key, value, value),
            )
            await db.commit()


# ==============================================================
# FSM STORAGE — Postgres/SQLite'da saqlanadigan FSM holati
# ==============================================================
"""
Standart holatda Dispatcher() MemoryStorage ishlatadi: bot Render/VPS'da
qayta ishga tushsa yoki deploy bo'lsa, barcha foydalanuvchilarning FSM
holati (masalan, "summa kiritayapti", "davlat tanlayapti") butunlay
o'chib ketadi. Quyidagi PersistentStorage o'sha holatni botning o'zi
ishlatayotgan bazaga (Postgres yoki SQLite — DATABASE_URL sozlanganiga
qarab, xuddi shu yuqoridagi _PG bayrog'i orqali) yozadi.
"""


async def _fsm_get(key_str: str) -> Optional[tuple]:
    if _PG:
        async with _pool.acquire() as conn:
            row = await conn.fetchrow("SELECT state, data FROM fsm_data WHERE storage_key = $1", key_str)
            return (row["state"], row["data"]) if row else None
    else:
        async with _db_sqlite() as db:
            cur = await db.execute("SELECT state, data FROM fsm_data WHERE storage_key = ?", (key_str,))
            row = await cur.fetchone()
            return (row[0], row[1]) if row else None


async def _fsm_set_state(key_str: str, state: Optional[str]):
    if _PG:
        async with _pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO fsm_data (storage_key, state, data) VALUES ($1, $2, '{}') "
                "ON CONFLICT (storage_key) DO UPDATE SET state = $2",
                key_str, state,
            )
    else:
        async with _db_sqlite() as db:
            await db.execute(
                "INSERT INTO fsm_data (storage_key, state, data) VALUES (?, ?, '{}') "
                "ON CONFLICT (storage_key) DO UPDATE SET state = ?",
                (key_str, state, state),
            )
            await db.commit()


async def _fsm_set_data(key_str: str, payload: str):
    if _PG:
        async with _pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO fsm_data (storage_key, state, data) VALUES ($1, NULL, $2) "
                "ON CONFLICT (storage_key) DO UPDATE SET data = $2",
                key_str, payload,
            )
    else:
        async with _db_sqlite() as db:
            await db.execute(
                "INSERT INTO fsm_data (storage_key, state, data) VALUES (?, NULL, ?) "
                "ON CONFLICT (storage_key) DO UPDATE SET data = ?",
                (key_str, payload, payload),
            )
            await db.commit()


class PersistentStorage(BaseStorage):
    """MemoryStorage o'rniga ishlatiladi — restart/deploy'dan keyin ham
    foydalanuvchining FSM holati saqlanib qoladi."""

    @staticmethod
    def _key_str(key: StorageKey) -> str:
        # aiogram versiyalari orasida StorageKey maydonlari farq qilishi
        # mumkin — shuning uchun aniq maydon nomiga tayanmasdan, dataclass'ning
        # o'zini (barcha maydonlari bilan) string'ga aylantiramiz.
        return str(key)

    async def set_state(self, key: StorageKey, state=None) -> None:
        value = state.state if isinstance(state, State) else state
        await _fsm_set_state(self._key_str(key), value)

    async def get_state(self, key: StorageKey) -> Optional[str]:
        row = await _fsm_get(self._key_str(key))
        return row[0] if row else None

    async def set_data(self, key: StorageKey, data: Dict[str, Any]) -> None:
        await _fsm_set_data(self._key_str(key), json.dumps(data, ensure_ascii=False))

    async def get_data(self, key: StorageKey) -> Dict[str, Any]:
        row = await _fsm_get(self._key_str(key))
        if not row or not row[1]:
            return {}
        try:
            return json.loads(row[1])
        except (json.JSONDecodeError, TypeError):
            return {}

    async def close(self) -> None:
        pass  # ulanish global _pool/DB_PATH orqali boshqariladi — alohida yopiladigan resurs yo'q


async def save_support_thread(admin_chat_id: int, message_id: int, user_id: int):
    """Adminga yuborilgan "Yordam" xabarining message_id'sini shu
    foydalanuvchi bilan bog'laydi — admin xabarga Reply qilganda kimga
    javob yo'llashni bilish uchun."""
    if _PG:
        async with _pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO support_threads (admin_chat_id, message_id, user_id) VALUES ($1, $2, $3) "
                "ON CONFLICT (admin_chat_id, message_id) DO UPDATE SET user_id = $3",
                admin_chat_id, message_id, user_id,
            )
    else:
        async with _db_sqlite() as db:
            await db.execute(
                "INSERT INTO support_threads (admin_chat_id, message_id, user_id) VALUES (?, ?, ?) "
                "ON CONFLICT (admin_chat_id, message_id) DO UPDATE SET user_id = ?",
                (admin_chat_id, message_id, user_id, user_id),
            )
            await db.commit()


async def get_support_thread(admin_chat_id: int, message_id: int) -> Optional[int]:
    """Reply qilingan xabar qaysi foydalanuvchining Yordam so'roviga tegishli
    ekanini qaytaradi (topilmasa None)."""
    if _PG:
        async with _pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT user_id FROM support_threads WHERE admin_chat_id = $1 AND message_id = $2",
                admin_chat_id, message_id,
            )
            return row["user_id"] if row else None
    else:
        async with _db_sqlite() as db:
            cur = await db.execute(
                "SELECT user_id FROM support_threads WHERE admin_chat_id = ? AND message_id = ?",
                (admin_chat_id, message_id),
            )
            row = await cur.fetchone()
            return row[0] if row else None


async def count_other_pending_topups(user_id: int, exclude_topup_id: int) -> int:
    """Shu foydalanuvchidan hali ko'rib chiqilmagan (pending) YANA nechta
    to'ldirish so'rovi borligini hisoblaydi (joriy so'rovdan tashqari) —
    admin bitta chekni ikki marta tasdiqlab qo'ymasligi uchun ogohlantirishda
    ishlatiladi.
    """
    if _PG:
        async with _pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT COUNT(*) AS c FROM topups WHERE user_id = $1 AND status = 'pending' AND id != $2",
                user_id, exclude_topup_id,
            )
            return row["c"]
    else:
        async with _db_sqlite() as db:
            cur = await db.execute(
                "SELECT COUNT(*) FROM topups WHERE user_id = ? AND status = 'pending' AND id != ?",
                (user_id, exclude_topup_id),
            )
            row = await cur.fetchone()
            return row[0]


async def get_stats() -> dict:
    """Admin panel uchun umumiy statistika: foydalanuvchilar, buyurtmalar
    (turlari bo'yicha), tushum (bugun/hafta/oy va turlar kesimida) va
    kutilayotgan to'ldirish so'rovlari."""
    now = int(time.time())
    day_ago = now - 86400
    week_ago = now - 7 * 86400
    month_ago = now - 30 * 86400
    order_types = ("number", "stars", "premium", "uc")

    if _PG:
        async with _pool.acquire() as conn:
            async def scalar(query, *params):
                row = await conn.fetchrow(query, *params)
                val = row[0] if row else 0
                return val if val is not None else 0

            users_total = await scalar("SELECT COUNT(*) FROM users")
            users_banned = await scalar("SELECT COUNT(*) FROM users WHERE banned = TRUE")
            users_new_today = await scalar("SELECT COUNT(*) FROM users WHERE created_at >= $1", day_ago)
            balance_total = await scalar("SELECT COALESCE(SUM(balance), 0) FROM users")

            orders_by_type = {}
            revenue_by_type = {}
            for order_type in order_types:
                orders_by_type[order_type] = await scalar(
                    "SELECT COUNT(*) FROM orders WHERE order_type = $1", order_type)
                revenue_by_type[order_type] = await scalar(
                    "SELECT COALESCE(SUM(price), 0) FROM orders WHERE order_type = $1 AND status != 'refunded'",
                    order_type)
            orders_total = sum(orders_by_type.values())
            orders_today = await scalar("SELECT COUNT(*) FROM orders WHERE created_at >= $1", day_ago)

            revenue_total = await scalar("SELECT COALESCE(SUM(price), 0) FROM orders WHERE status != 'refunded'")
            revenue_today = await scalar(
                "SELECT COALESCE(SUM(price), 0) FROM orders WHERE status != 'refunded' AND created_at >= $1", day_ago)
            revenue_week = await scalar(
                "SELECT COALESCE(SUM(price), 0) FROM orders WHERE status != 'refunded' AND created_at >= $1", week_ago)
            revenue_month = await scalar(
                "SELECT COALESCE(SUM(price), 0) FROM orders WHERE status != 'refunded' AND created_at >= $1", month_ago)

            topups_pending = await scalar("SELECT COUNT(*) FROM topups WHERE status = 'pending'")
    else:
        async with _db_sqlite() as db:
            async def scalar(query, params=()):
                cur = await db.execute(query, params)
                row = await cur.fetchone()
                return row[0] if row and row[0] is not None else 0

            users_total = await scalar("SELECT COUNT(*) FROM users")
            users_banned = await scalar("SELECT COUNT(*) FROM users WHERE banned = 1")
            users_new_today = await scalar(
                "SELECT COUNT(*) FROM users WHERE created_at >= ?", (day_ago,))
            balance_total = await scalar("SELECT COALESCE(SUM(balance), 0) FROM users")

            orders_by_type = {}
            revenue_by_type = {}
            for order_type in order_types:
                orders_by_type[order_type] = await scalar(
                    "SELECT COUNT(*) FROM orders WHERE order_type = ?", (order_type,))
                revenue_by_type[order_type] = await scalar(
                    "SELECT COALESCE(SUM(price), 0) FROM orders WHERE order_type = ? AND status != 'refunded'",
                    (order_type,))
            orders_total = sum(orders_by_type.values())
            orders_today = await scalar(
                "SELECT COUNT(*) FROM orders WHERE created_at >= ?", (day_ago,))

            revenue_total = await scalar(
                "SELECT COALESCE(SUM(price), 0) FROM orders WHERE status != 'refunded'")
            revenue_today = await scalar(
                "SELECT COALESCE(SUM(price), 0) FROM orders "
                "WHERE status != 'refunded' AND created_at >= ?", (day_ago,))
            revenue_week = await scalar(
                "SELECT COALESCE(SUM(price), 0) FROM orders "
                "WHERE status != 'refunded' AND created_at >= ?", (week_ago,))
            revenue_month = await scalar(
                "SELECT COALESCE(SUM(price), 0) FROM orders "
                "WHERE status != 'refunded' AND created_at >= ?", (month_ago,))

            topups_pending = await scalar(
                "SELECT COUNT(*) FROM topups WHERE status = 'pending'")

    return {
        "users_total": users_total,
        "users_banned": users_banned,
        "users_new_today": users_new_today,
        "balance_total": balance_total,
        "orders_total": orders_total,
        "orders_by_type": orders_by_type,
        "orders_today": orders_today,
        "revenue_total": revenue_total,
        "revenue_today": revenue_today,
        "revenue_week": revenue_week,
        "revenue_month": revenue_month,
        "revenue_by_type": revenue_by_type,
        "topups_pending": topups_pending,
    }


# ==============================================================
# SMMUPPER API KLIENTI (smmupper_api)
# ==============================================================
"""
SmmUpper — Hamkorlik API v2 uchun klient.
Hujjat: https://smmupper.uz/api/v2/docs

Barcha so'rovlar GET, javob JSON. Har bir so'rovda api_key bo'lishi shart.
Muvaffaqiyat: {"success": true, ...}, xato: {"success": false, "error": "..."}.
"""
SETTINGS_KEY_API_KEY = "smmupper_api_key"


class SmmUpperError(Exception):
    """SmmUpper API 'success': false qaytarganda ko'tariladi."""

    def __init__(self, message: str, status: Optional[int] = None):
        self.message = message
        self.status = status
        super().__init__(message)


def new_request_id() -> str:
    """Xarid so'rovlari (buyStars/buyPremium/getNumber) uchun noyob request_id.
    Hujjatga ko'ra 6-64 belgi: harf, raqam, . _ - bo'lishi kerak — uuid4().hex (32 ta belgi) mos keladi.
    """
    return uuid.uuid4().hex


class SmmUpperClient:
    PRICES_CACHE_TTL = 90  # soniya — shu vaqt ichida getPrices() qayta so'ralmaydi (narxlar soniyama-soniya o'zgarmaydi)

    def __init__(self, api_key: str = SMMUPPER_API_KEY, base_url: str = SMMUPPER_BASE_URL):
        self.api_key = api_key  # .env'dagi standart qiymat — DB bo'sh bo'lsa shu ishlatiladi
        self.base_url = base_url
        self._session: Optional[aiohttp.ClientSession] = None  # so'rovlar orasida qayta ishlatiladi
        self._prices_cache: Optional[dict] = None
        self._prices_cache_ts: float = 0.0

    async def _get_session(self) -> aiohttp.ClientSession:
        """Bitta umumiy sessiyani qayta ishlatadi — har bir so'rovda yangi TCP/TLS ulanish ochish
        o'rniga. Yopilib qolgan yoki hali yaratilmagan bo'lsa — yangisini ochadi."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20))
        return self._session

    async def close(self) -> None:
        """Bot to'xtaganda (graceful shutdown) chaqiriladi — ochiq ulanishni tartibli yopadi."""
        if self._session is not None and not self._session.closed:
            await self._session.close()

    async def _current_api_key(self) -> str:
        """Admin panel orqali DB'ga saqlangan API kalit bo'lsa o'shani, aks holda
        .env (SMMUPPER_API_KEY) dagisini qaytaradi. Shu tufayli kalitni Render
        Environment'ga qayta kirmasdan, botning o'zidan yangilash mumkin."""
        return await get_setting(SETTINGS_KEY_API_KEY, default=self.api_key) or self.api_key

    async def _request(self, action: str, *, raise_on_error: bool = True,
                        _retry_on_limit: bool = True, **params) -> dict:
        current_key = await self._current_api_key()
        query = {"action": action, "api_key": current_key}
        for key, value in params.items():
            if value is not None:
                query[key] = value

        session = await self._get_session()
        async with session.get(self.base_url, params=query) as resp:
            if resp.status == 429 and _retry_on_limit:
                retry_after = 5
                try:
                    retry_after = int(resp.headers.get("Retry-After", "5"))
                except ValueError:
                    pass
                await asyncio.sleep(min(retry_after, 15))
                return await self._request(
                    action, raise_on_error=raise_on_error, _retry_on_limit=False, **params
                )

            try:
                data = await resp.json(content_type=None)
            except Exception:
                text = await resp.text()
                raise SmmUpperError(f"Server javobini o'qib bo'lmadi: {text[:200]}", resp.status)

            if raise_on_error and not data.get("success"):
                raise SmmUpperError(data.get("error", "Noma'lum xato"), resp.status)

            return data

    # 1. Balans (bizning hamkor hisobimizning SmmUpper'dagi balansi)
    async def get_balance(self) -> dict:
        return await self._request("getBalance")

    # 2. Narxlar (Stars / Premium)
    async def get_prices(self, *, force_refresh: bool = False) -> dict:
        """PRICES_CACHE_TTL soniya ichida keshdan qaytadi — Stars/Premium/UC oqimlarining har biri
        bitta xaridda bir necha marta chaqiradi, narxlar esa soniyama-soniya o'zgarmaydi.
        `force_refresh=True` keshni chetlab o'tadi.
        """
        now = time.monotonic()
        if not force_refresh and self._prices_cache is not None and now - self._prices_cache_ts < self.PRICES_CACHE_TTL:
            return self._prices_cache
        data = await self._request("getPrices")
        self._prices_cache = data
        self._prices_cache_ts = now
        return data

    # 3. Davlatlar ro'yxati
    async def available_countries(self, server: int) -> dict:
        return await self._request("available_countries", server=server)

    # 4. Raqam olish
    async def get_number(self, server: int, country: str, request_id: Optional[str] = None) -> dict:
        return await self._request(
            "getNumber", server=server, country=country, request_id=request_id
        )

    # 5. SMS kodni olish — status:"waiting" xato emas, shuning uchun raise_on_error=False
    async def get_code(self, server: int, *, hash_code: Optional[str] = None,
                        number: Optional[str] = None, id: Optional[str] = None) -> dict:
        params = {"server": server}
        if server == 1:
            params["hash_code"] = hash_code
        elif server == 2:
            params["number"] = number
        else:
            params["id"] = id
        return await self._request("getCode", raise_on_error=False, **params)

    # 6. Stars sotib olish
    async def buy_stars(self, username: str, amount: int, request_id: Optional[str] = None) -> dict:
        return await self._request(
            "buyStars", username=username, amount=amount, request_id=request_id
        )

    # 7. Premium sotib olish
    async def buy_premium(self, username: str, months: int, request_id: Optional[str] = None) -> dict:
        return await self._request(
            "buyPremium", username=username, months=months, request_id=request_id
        )

    # 7b. UC tariflari (PUBG Mobile): "price" — SIZGA amaldagi (VIP darajasiga qarab) narx
    async def get_uc_tariffs(self) -> dict:
        return await self._request("getUcTariffs")

    # 7c. UC sotib olish (PUBG Mobile): narx darhol yechiladi, buyurtma administrator tasdig'ini kutadi
    async def buy_uc(self, tariff_id: int, account_id: str, request_id: Optional[str] = None) -> dict:
        return await self._request(
            "buyUC", tariff_id=tariff_id, account_id=account_id, request_id=request_id
        )

    # 8. Buyurtma holati
    async def get_order(self, order_id) -> dict:
        return await self._request("getOrder", order_id=order_id)

    # 9. Raqamni bekor qilish/bloklash — BUNDAY AMAL YO'Q.
    # https://smmupper.uz/api/v2/docs (v2, tekshirilgan sana: shu tahrir
    # kuni) — hujjatda bor-yo'g'i 8 ta amal bor: getBalance, getPrices,
    # available_countries, getNumber, getCode, buyStars, buyPremium,
    # getOrder. Raqamni bekor qilish/bo'shatish uchun API orqali HECH
    # QANDAY yo'l yo'q, shuning uchun bu metod olib tashlandi — pastda
    # refund_number_order endi providerga hech narsa yubormay, faqat
    # foydalanuvchi balansini qaytaradi.



# ==============================================================
# NARXLASH (pricing)
# ==============================================================
SETTINGS_KEY_MARKUP = "markup_percent"   # UMUMIY ustama: alohida qiymati yo'q bo'limlar shuni ishlatadi

# Alohida narx ustamasi qo'yiladigan bo'limlar (buyurtma turlari nomlari bilan bir xil).
# "Tayyor akkaunt" ham "number" bo'limiga kiradi — uning ustamasi Raqam bilan bir xil.
MARKUP_CATEGORIES = ("number", "stars", "premium", "uc")
MAX_MARKUP_PERCENT = 1000.0   # xato kiritilgan katta qiymat (masalan 10000) narxlarni buzib yubormasligi uchun


def _parse_markup(raw) -> Optional[float]:
    """Saqlangan/kiritilgan ustamani songa aylantiradi. Bo'sh, buzuq, cheksiz (nan/inf) yoki -100% dan
    past qiymat bo'lsa None qaytaradi (bunday qiymat narxni 0 yoki manfiy qilib yuborardi)."""
    if raw is None:
        return None
    try:
        value = float(str(raw).strip().replace(",", "."))
    except (TypeError, ValueError):
        return None
    return value if math.isfinite(value) and value > -100 else None


def _markup_key(category: Optional[str]) -> str:
    return f"{SETTINGS_KEY_MARKUP}_{category}" if category else SETTINGS_KEY_MARKUP


def _env_markup(category: str) -> Optional[float]:
    """.env dagi MARKUP_PERCENT_NUMBER / _STARS / _PREMIUM / _UC (o'rnatilmagan bo'lsa None)."""
    return _parse_markup(os.getenv(f"MARKUP_PERCENT_{category.upper()}"))


async def get_markup_percent(category: Optional[str] = None) -> float:
    """Narx ustamasi (foizda). `category` (number / stars / premium / uc) berilsa — shu bo'lim uchun ALOHIDA
    qo'yilgan ustama; u yo'q bo'lsa (yoki bo'lim berilmasa) — UMUMIY ustama. Ustuvorlik (yuqoridan pastga):
      1) admin paneldan saqlangan bo'lim ustamasi;   2) .env: MARKUP_PERCENT_<BO'LIM>;
      3) admin paneldan saqlangan umumiy ustama;     4) .env: MARKUP_PERCENT."""
    if category in MARKUP_CATEGORIES:
        own = _parse_markup(await get_setting(_markup_key(category), default=None))
        if own is None:
            own = _env_markup(category)
        if own is not None:
            return own
    general = _parse_markup(await get_setting(SETTINGS_KEY_MARKUP, default=None))
    return general if general is not None else MARKUP_PERCENT


async def get_markup_overview() -> dict:
    """Admin panel uchun: {"general": umumiy foiz, "items": {bo'lim: (foiz, alohida_qo'yilganmi)}}."""
    general = await get_markup_percent()
    items = {}
    for category in MARKUP_CATEGORIES:
        own = _parse_markup(await get_setting(_markup_key(category), default=None))
        if own is None:
            own = _env_markup(category)
        items[category] = (own if own is not None else general, own is not None)
    return {"general": general, "items": items}


def _markup_multiplier(percent: float) -> float:
    """Ustama foizini ko'paytiruvchiga aylantiradi. Formula shu yerda BITTA — o'zgartirish kerak bo'lsa,
    hamma joyda (davlat tugmasi narxi, Stars narxi, xarid) birdaniga o'zgaradi."""
    return 1 + percent / 100


def _positive_number(value) -> Optional[float]:
    """Provayder javobidagi narx (son yoki son ko'rinishidagi satr): faqat MUSBAT va chekli bo'lsa qaytaradi."""
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) and number > 0 else None


async def with_markup(base_price, category: Optional[str] = None) -> int:
    """Bazaviy (SmmUpper) narxga `category` bo'limining foyda foizini qo'shib, yaxlit so'mga aylantiradi.
    Raqam, Stars, Premium va UC — har biri O'Z ustamasi bilan hisoblanadi (bo'lim berilmasa — umumiy ustama).
    Natija HECH QACHON 0 dan past bo'lmaydi. Bazaviy narx noma'lum/noto'g'ri bo'lsa ham 0 qaytadi — bu
    "narx aniqlanmadi" degani: chaqiruvchi uni SOTMASLIGI kerak (aks holda mahsulot tekinga ketib qolardi)."""
    base = _positive_number(base_price)
    if base is None:
        return 0
    percent = await get_markup_percent(category)
    return max(0, round(base * _markup_multiplier(percent)))


async def final_price(user_id: int, result: dict, est_price: int, category: str) -> int:
    """Xariddan keyingi YAKUNIY qadam: provayder javobidagi HAQIQIY narxni (+ `category` ustamasi) taxminiy
    narx bilan solishtirib, farqni balansdan hisob-kitob qiladi (ortig'ini yechadi yoki ortiqchasini darhol
    qaytaradi — settle_price_difference orqali) va foydalanuvchidan HAQIQATDA yechilgan summani qaytaradi;
    AYNAN shu summa buyurtmaga yozilishi shart. Javobda narx yo'q/noto'g'ri bo'lsa — hisob-kitob qilinmaydi,
    taxminiy narx qoladi (aks holda "farq" foydalanuvchiga to'liq qaytarilib, mahsulot tekinga ketib qolardi)."""
    raw = _positive_number((result or {}).get("price"))
    if raw is None:
        logging.warning(f"Provayder javobida narx yo'q yoki noto'g'ri ({category}) — taxminiy narx ishlatildi")
        return est_price
    actual = await with_markup(raw, category)
    if actual <= 0:
        return est_price
    return await settle_price_difference(user_id, est_price, actual)


async def settle_price_difference(user_id: int, est_price: int, actual_price: int) -> int:
    """Provayderning yakuniy narxi taxminiydan farq qilsa, farqni hisob-kitob qiladi va
    foydalanuvchidan HAQIQATDA yechilgan summani qaytaradi. Buyurtmaga (refund va statistika
    tayanadigan narxga) AYNAN shu summa yozilishi shart — aks holda yechilmagan farq ham
    keyinroq foydalanuvchiga qaytarib berilishi mumkin.
    Balans HECH QACHON manfiyga tushirilmaydi (try_deduct_balance atomik: yetmasa, hech narsa yechmaydi).
    """
    if actual_price > est_price:
        if await try_deduct_balance(user_id, actual_price - est_price):
            return actual_price
        logging.warning(
            f"Narx farqi yechilmadi (balans yetmadi): user={user_id} taxminiy={est_price} "
            f"yakuniy={actual_price} farq={actual_price - est_price}"
        )
        return est_price
    if actual_price < est_price:
        await change_balance(user_id, est_price - actual_price)
        return actual_price
    return est_price


# ---------------- Xaridni himoyalash: parallel bosish, eskirgan callback, tarmoq xatosi ----------------
_NET_ERRORS = (asyncio.TimeoutError, aiohttp.ClientError)
_API_ERRORS = (SmmUpperError,) + _NET_ERRORS   # provayder xatosi YOKI tarmoq/timeout
_FETCH_ERRORS = _API_ERRORS + (KeyError,)      # + javob kutilmagan ko'rinishda bo'lsa
_purchase_inflight: set = set()


def single_flight_purchase(handler):
    """Bitta foydalanuvchining xarid tasdig'i BIR VAQTDA faqat bitta bajariladi. Tasdiq tugmasi ketma-ket
    (SmmUpper javobini kutmasdan) ikki marta bosilsa, ikkinchisi rad etiladi — aks holda ikki marta
    yechilib, ikki marta sotib olinishi mumkin edi (holat faqat oxirida tozalanadi)."""
    @functools.wraps(handler)
    async def wrapper(callback, *args, **kwargs):
        uid = callback.from_user.id
        if uid in _purchase_inflight:
            with contextlib.suppress(Exception):
                await callback.answer("\u23F3 Buyurtma bajarilmoqda, biroz kuting...")
            return
        _purchase_inflight.add(uid)
        try:
            return await handler(callback, *args, **kwargs)
        finally:
            _purchase_inflight.discard(uid)
    return wrapper


async def _safe_answer(callback, text=None, show_alert: bool = False):
    """callback.answer — xato bersa (masalan, tugma eskirgan) e'tiborsiz: pul yechilgandan keyingi
    xato xaridni to'xtatib, puldan ayirib qo'ymasligi uchun."""
    try:
        await callback.answer(text, show_alert=show_alert)
    except Exception:
        pass


def _err_text(e: BaseException) -> str:
    """Xato matni: SmmUpperError.message, aks holda (tarmoq xatosi/timeout) qisqa tavsif."""
    return getattr(e, "message", None) or str(e) or type(e).__name__


class PurchaseUnknownError(Exception):
    """Xarid natijasi NOMA'LUM (tarmoq/timeout/5xx/409): so'rov SmmUpper'da bajarilgan bo'lishi ham mumkin."""

    def __init__(self, request_id, cause=None):
        self.request_id = request_id
        self.cause = cause
        super().__init__(f"natija noma'lum (request_id={request_id}): {cause!r}")


def _is_ambiguous_api_error(e) -> bool:
    """409 (oldingi so'rov hali ketyapti) va 5xx — natija noma'lum; qolgan xatolar (402/404/400...) — aniq rad."""
    status = getattr(e, "status", None)
    return status == 409 or (status is not None and status >= 500)


async def _call_idempotent(fn, *args, request_id=None, attempts: int = 3, **kwargs):
    """SmmUpper xaridini XUDDI SHU request_id bilan qayta urinib chaqiradi (hujjat: takroriy so'rov ikkinchi marta
    bajarilmaydi, saqlangan natija qaytadi). Aniq rad etilsa SmmUpperError ko'tariladi (pulni qaytarish xavfsiz).
    Natija noma'lum qolsa (tarmoq/5xx/409 hamma urinishda) PurchaseUnknownError ko'tariladi."""
    request_id = request_id or new_request_id()
    last = None
    for attempt in range(attempts):
        try:
            return await fn(*args, request_id=request_id, **kwargs)
        except SmmUpperError as e:
            if not _is_ambiguous_api_error(e):
                raise
            last = e
        except _NET_ERRORS as e:
            last = e
        if attempt < attempts - 1:
            await asyncio.sleep(3)
    raise PurchaseUnknownError(request_id, last)


async def _refund_unknown_purchase(bot, callback, state, est_price: int, label: str, err) -> None:
    """Xarid natijasi noma'lum (Raqam/Stars/Premium): pul foydalanuvchiga qaytariladi, adminga request_id bilan
    ogohlantirish yuboriladi (xarid SmmUpper'da amalga oshgan bo'lishi ham mumkin)."""
    user_id = callback.from_user.id
    await change_balance(user_id, est_price)
    request_id = getattr(err, "request_id", "?")
    logging.error(f"{label} xaridi: SmmUpper javobi olinmadi user={user_id} request_id={request_id}: {err}")
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(
                admin_id,
                f"\u26A0\uFE0F {label} xaridi: SmmUpper javobi olinmadi (tarmoq).\n"
                f"Foydalanuvchi ID: {user_id}, summa: {fmt_money(est_price)} so'm.\nrequest_id: {request_id}\n"
                f"Foydalanuvchi puli balansga qaytarildi. Xarid SmmUpper'da amalga oshgan bo'lishi ham mumkin "
                f"\u2014 uni SmmUpper panelida tekshiring.")
        except Exception:
            pass
    with contextlib.suppress(Exception):
        await callback.message.answer(
            "\u274C SmmUpper bilan aloqa uzildi, buyurtma tasdiqlanmadi.\n"
            "\U0001F4B0 Pulingiz balansga qaytarildi. Birozdan so'ng qayta urinib ko'ring.",
            reply_markup=main_menu(),
        )
    await state.clear()


# ==============================================================
# MATNLAR (texts)
# ==============================================================
WELCOME = (
    "\U0001F44B Assalomu alaykum!\n\n"
    "Bu bot orqali siz:\n"
    "\U0001F4F1 Virtual raqam (SMS kod uchun)\n"
    "\u2B50 Telegram Stars\n"
    "\U0001F48E Telegram Premium\n"
    "\U0001F3AE PUBG Mobile UC\n\n"
    "sotib olishingiz mumkin. Quyidagi menyudan tanlang \U0001F447"
)

BTN_HELP = "\U0001F195 Yordam"
BTN_BALANCE = "\U0001F4B0 Balans"
BTN_TOPUP = "\U0001F4B3 Balansni to'ldirish"
BTN_NUMBER = "\U0001F4F1 Raqam"
BTN_STARS = "\u2B50 Stars"
BTN_PREMIUM = "\U0001F48E Premium"
BTN_UC = "\U0001F3AE PUBG UC"
BTN_ORDERS = "\U0001F4CB Buyurtmalarim"
BTN_CHECK_ORDER = "\U0001F50E Buyurtma ID orqali"
BTN_REPEAT_ORDER = "\U0001F501 Qayta buyurtma"
BTN_BACK = "\u2B05\uFE0F Orqaga"
BTN_CANCEL = "\u274C Bekor qilish"
BTN_CHECK_CODE = "\U0001F504 Kodni tekshirish"
BTN_CONFIRM = "\u2705 Sotib olish"

REFERRAL_CASHBACK_PERCENT = 1  # taklif qilingan do'st balans to'ldirsa, shu foizi taklif qilgan odamga keshbek sifatida qo'shiladi

# Asosiy (pastki) menyu tugmalari uchun qabul qilinadigan matnlar: hozirgi qisqa yorliq + ESKI (uzunroq)
# yorliqlar. Foydalanuvchida eski klaviatura qolib ketgan bo'lishi mumkin (Telegram uni bot yangisini
# yuborguncha almashtirmaydi) — eski tugma bosilsa ham bot javob berishi uchun ikkalasi ham ishlaydi.
MENU_HELP = frozenset({BTN_HELP})
MENU_BALANCE = frozenset({BTN_BALANCE})
MENU_NUMBER = frozenset({BTN_NUMBER, "\U0001F4F1 Raqam sotib olish"})
MENU_STARS = frozenset({BTN_STARS, "\u2B50 Stars sotib olish"})
MENU_PREMIUM = frozenset({BTN_PREMIUM, "\U0001F48E Premium sotib olish"})
MENU_UC = frozenset({BTN_UC, "\U0001F3AE UC sotib olish"})
MENU_ORDERS = frozenset({BTN_ORDERS})
MENU_CHECK_ORDER = frozenset({BTN_CHECK_ORDER})
MENU_REPEAT_ORDER = frozenset({BTN_REPEAT_ORDER, "\U0001F501 Oxirgini takrorlash"})

# Bosh menyudagi tugma matnlari — bular FSM holatida turgan "erkin matn"
# handlerlar tomonidan "username" yoki "summa" deb noto'g'ri qabul qilinmasligi kerak.
RESERVED_TEXTS = {
    BTN_TOPUP, BTN_CANCEL,
    *MENU_HELP, *MENU_BALANCE, *MENU_NUMBER, *MENU_STARS, *MENU_PREMIUM,
    *MENU_UC, *MENU_ORDERS, *MENU_CHECK_ORDER, *MENU_REPEAT_ORDER,
}


def fmt_money(amount) -> str:
    return f"{int(amount):,}".replace(",", " ")


def balance_text(amount: int) -> str:
    return f"\U0001F4B0 Balansingiz: {fmt_money(amount)} so'm"


def referral_text(link: str, invited: int = 0, earned: int = 0) -> str:
    return (
        f"\U0001F381 Do'stlaringizni taklif qiling!\n\n"
        f"Sizning shaxsiy havolangiz:\n{link}\n\n"
        f"Taklif qilgan do'stingiz balansini to'ldirsa, sizga har safar "
        f"to'ldirilgan summaning {REFERRAL_CASHBACK_PERCENT}% miqdorida keshbek "
        f"beriladi \u2014 avtomatik ravishda balansingizga qo'shiladi.\n\n"
        f"\U0001F4CA Statistikangiz:\n"
        f"\U0001F465 Taklif qilingan do'stlar: {invited}\n"
        f"\U0001F4B0 Jami olingan keshbek: {fmt_money(earned)} so'm"
    )


def insufficient_balance(price: int, balance: int) -> str:
    return (
        f"\u274C Balans yetarli emas!\n"
        f"\U0001F4B0 Kerakli summa: {fmt_money(price)} so'm\n"
        f"\U0001F4B3 Sizning balansingiz: {fmt_money(balance)} so'm\n"
        f"Balansingizni to'ldirib, qaytadan urinib ko'ring."
    )


MIN_TOPUP = 5000
TOPUP_ASK_AMOUNT = (
    "Necha so'mga balansni to'ldirmoqchisiz? Summani kiriting (masalan: 50000).\n"
    "Eng kam summa: " + fmt_money(MIN_TOPUP) + " so'm."
)
TOPUP_NOT_A_NUMBER = "Iltimos, faqat musbat son kiriting. Masalan: 50000"


def topup_instructions(amount: int, card_number: str, card_holder: str) -> str:
    return (
        f"\U0001F4B3 {fmt_money(amount)} so'mni quyidagi kartaga o'tkazing:\n\n"
        f"{card_number}\n"
        f"{card_holder}\n\n"
        f"To'lov chekining skrinshotini shu yerga (rasm sifatida) yuboring \U0001F447"
    )


TOPUP_SENT_TO_ADMIN = "\u2705 So'rovingiz adminga yuborildi. Tasdiqlangach, balansingiz to'ldiriladi."
TOPUP_SEND_PHOTO = "Iltimos, to'lov chekining skrinshotini rasm sifatida yuboring."
TOPUP_REJECTED_USER = "\u274C Balansni to'ldirish so'rovingiz rad etildi. Admin bilan bog'laning."


def topup_approved_text(amount: int, balance: int) -> str:
    return (
        f"\u2705 Balansingiz {fmt_money(amount)} so'mga to'ldirildi. "
        f"Yangi balans: {fmt_money(balance)} so'm"
    )


NO_ORDERS_YET = "Hali buyurtmalar yo'q."


def _mask_phone(number) -> str:
    """Raqamni kanalga chiqarishdan oldin qisman yashiradi — oxirgi 4 ta
    raqam '****' bilan almashtiriladi, qolgani ko'rinadi (masalan
    +573159063519 -> +57315906****)."""
    digits = re.sub(r"\D", "", str(number))
    if not digits:
        return str(number)
    if len(digits) <= 4:
        return "+" + "*" * len(digits)
    return "+" + digits[:-4] + "*" * 4


def channel_number_notice(buyer: str, country: str, price: int, number: str = "") -> str:
    lines = [
        f"\U0001F464 Xaridor: {buyer}",
        f"\U0001F30E Mamlakat: {country_flag(country)} {country_display_name(country)}",
    ]
    if number:
        lines.append(f"\U0001F4F2 Raqam: {_mask_phone(number)}")
    lines.append(f"\U0001F4B0 To'lov: {fmt_money(price)} so'm")
    return "\n".join(lines)


def channel_stars_notice(buyer: str, target_username: str, amount: int, price: int) -> str:
    return (
        f"\U0001F464 Xaridor: {buyer}\n"
        f"\U0001F3AF Kimga: @{target_username}\n"
        f"\u2B50 Miqdor: {amount} Stars\n"
        f"\U0001F4B0 To'lov: {fmt_money(price)} so'm"
    )


def channel_premium_notice(buyer: str, target_username: str, months: int, price: int) -> str:
    return (
        f"\U0001F464 Xaridor: {buyer}\n"
        f"\U0001F3AF Kimga: @{target_username}\n"
        f"\U0001F4C5 Muddat: {months} oy\n"
        f"\U0001F4B0 To'lov: {fmt_money(price)} so'm"
    )


def admin_user_card(user: dict, referral: Optional[dict] = None) -> str:
    banned = "\U0001F6AB Ha" if user.get("banned") else "Yo'q"
    username = f"@{user['username']}" if user.get("username") else "\u2014"
    lines = [
        f"\U0001F464 {user.get('full_name') or '\u2014'} ({username})",
        f"\U0001F522 ID: {user['user_id']}",
        f"\U0001F4B0 Balans: {fmt_money(user['balance'])} so'm",
        f"\U0001F4E6 Buyurtmalar soni: {user.get('order_count', 0)}",
        f"\U0001F4B5 Jami xarid: {fmt_money(user.get('total_spent', 0))} so'm",
    ]
    if referral is not None:
        lines.append(f"\U0001F381 Referallar: {referral.get('invited', 0)} ta (keshbek: {fmt_money(referral.get('earned', 0))} so'm)")
    lines.append(f"\U0001F6AB Bloklangan: {banned}")
    return "\n".join(lines)


def balance_adjusted_by_admin(amount: int, new_balance: int) -> str:
    if amount >= 0:
        return (
            f"\U0001F4B0 Balansingizga {fmt_money(amount)} so'm qo'shildi.\n"
            f"Joriy balans: {fmt_money(new_balance)} so'm"
        )
    return (
        f"\U0001F4B0 Balansingizdan {fmt_money(-amount)} so'm ayirildi.\n"
        f"Joriy balans: {fmt_money(new_balance)} so'm"
    )


def duplicate_topup_warning(count: int) -> str:
    return (
        f"\n\n\u26A0\uFE0F Diqqat: bu foydalanuvchidan yana {count} ta kutilayotgan "
        f"so'rov bor \u2014 ikkalasini ham tasdiqlab qo'ymang!"
    )


ADMIN_TITLE = "\U0001F527 Admin panel"

ASK_FIND_USER = "Qidirmoqchi bo'lgan foydalanuvchi ID raqamini yoki @username'ini yuboring:"
ASK_BALANCE_ID = "Balansini o'zgartirmoqchi bo'lgan foydalanuvchi ID raqamini yuboring:"
ASK_BALANCE_AMOUNT = (
    "Endi miqdorni yuboring.\n"
    "Qo'shish uchun: 50000\n"
    "Ayirish uchun: -20000"
)
ASK_BAN_ID = "Bloklamoqchi bo'lgan foydalanuvchi ID raqamini yuboring:"
ASK_UNBAN_ID = "Blokdan chiqarmoqchi bo'lgan foydalanuvchi ID raqamini yuboring:"
ASK_BROADCAST_TEXT = "Barcha foydalanuvchilarga yuboriladigan xabar matnini kiriting:"
ASK_API_KEY = (
    "SmmUpper hamkorlik API kalitini yuboring (SmmUpper botida \"API (hamkorlik)\" "
    "bo'limidan olinadi).\n\n"
    "Bu kalit botning bazasiga saqlanadi va Render'ga qayta kirmasdan darhol ishlay boshlaydi."
)

NOT_A_VALID_ID = "\u274C Noto'g'ri ID. Faqat raqam yuboring."
NOT_A_VALID_AMOUNT = "\u274C Noto'g'ri miqdor. Masalan: 50000 yoki -20000"


def api_key_saved(masked: str) -> str:
    return f"\u2705 API kalit saqlandi: {masked}\n\nEndi barcha so'rovlar shu kalit orqali yuboriladi."


def current_api_key_line(masked: str) -> str:
    return f"\U0001F511 Joriy API kalit: {masked}"


MARKUP_ICONS = {"number": "\U0001F4F1", "stars": "\u2B50", "premium": "\U0001F48E", "uc": "\U0001F3AE"}
MARKUP_NAMES = {"number": "Raqam", "stars": "Stars", "premium": "Premium", "uc": "PUBG UC"}
MARKUP_LABELS = {c: f"{MARKUP_ICONS[c]} {MARKUP_NAMES[c]}" for c in MARKUP_CATEGORIES}


def ask_markup_percent(target: str) -> str:
    """Ustama foizini so'rash matni. target: bo'lim ("number"/"stars"/"premium"/"uc") yoki "all" (hammasiga)."""
    if target == "all":
        scope = (
            "\U0001F310 Bu foiz \U0001F4F1 Raqam, \u2B50 Stars, \U0001F48E Premium va \U0001F3AE PUBG UC \u2014 "
            "HAMMASIGA bir xil qo'yiladi."
        )
        reset_hint = ""
    else:
        scope = f"{MARKUP_LABELS[target]} uchun ALOHIDA foiz qo'yiladi (boshqa bo'limlarga tegmaydi)."
        reset_hint = "\nUmumiy foizga qaytarish uchun: -"
    return (
        "Narxlarga qo'shiladigan foyda foizini kiriting (masalan: 10 \u2014 SmmUpper narxining ustiga "
        "+10% qo'shib sotish degani).\n"
        f"Ustama qo'ymaslik uchun: 0{reset_hint}\n\n{scope}"
    )


NOT_A_VALID_PERCENT = "\u274C Noto'g'ri qiymat. Faqat son kiriting, masalan: 10 yoki 0"
INVALID_MARKUP_RANGE = (
    "\u274C Ustama -100% dan katta bo'lishi kerak (aks holda narx 0 yoki "
    "manfiy bo'lib, foydalanuvchilar mahsulotni bepul olib qolishi mumkin). "
    "Boshqa qiymat kiriting."
)
INVALID_MARKUP_MAX = (
    f"\u274C Ustama juda katta (eng ko'pi bilan {MAX_MARKUP_PERCENT:g}%). "
    "Xato yozmadingizmi? Boshqa qiymat kiriting."
)


def markup_saved(percent: float, target: str = "all") -> str:
    if target == "all":
        return f"\u2705 Barcha bo'limlar uchun ustama saqlandi: {percent:g}%\n\nYangi buyurtmalarda shu foiz qo'llaniladi."
    return (
        f"\u2705 {MARKUP_LABELS[target]} uchun ustama saqlandi: {percent:g}%\n\n"
        "Yangi buyurtmalarda shu foiz qo'llaniladi."
    )


def markup_reset_text(target: str, percent: float) -> str:
    return (
        f"\u2705 {MARKUP_LABELS[target]} endi umumiy ustamadan foydalanadi: {percent:g}%"
    )


def markup_panel_text(overview: dict) -> str:
    """Narx ustamasi paneli matni: har bir bo'lim uchun joriy foiz (alohida qo'yilmagan bo'lsa — "umumiy")."""
    lines = ["\U0001F4C8 Narx ustamasi", ""]
    for category in MARKUP_CATEGORIES:
        percent, custom = overview["items"][category]
        lines.append(f"{MARKUP_LABELS[category]}: {percent:g}%" + ("" if custom else " (umumiy)"))
    lines += [
        "",
        f"\U0001F310 Umumiy: {overview['general']:g}%",
        "",
        "Narx = SmmUpper narxi + ustama. O'zgartirmoqchi bo'lgan bo'limni tanlang \U0001F447",
    ]
    return "\n".join(lines)


def markup_short_line(overview: dict) -> str:
    """Sozlamalar ekranidagi bir qatorli xulosa."""
    parts = " \u00B7 ".join(
        f"{MARKUP_ICONS[c]} {overview['items'][c][0]:g}%" for c in MARKUP_CATEGORIES
    )
    return f"\U0001F4C8 Narx ustamasi: {parts}"


FORCE_SUB_PROMPT = (
    "\U0001F510 Botdan foydalanish uchun avval quyidagi kanalga a'zo bo'ling, "
    "so'ng \u2705 tugmasini bosing."
)
FORCE_SUB_STILL_NOT = "\u274C Hali kanalga a'zo emassiz. Avval a'zo bo'ling, keyin qayta tekshiring."
FORCE_SUB_OK = "\u2705 Rahmat! Endi botdan foydalanishingiz mumkin."

ASK_CHANNEL_ID = (
    "Raqam/Stars/Premium/UC sotib olinganda xabar yuboriladigan kanalni yuboring.\n\n"
    "\u2022 Kanal @username'i bo'lsa: @kanalim\n"
    "\u2022 Yopiq kanal bo'lsa: -100 bilan boshlanuvchi ID (masalan: -1001234567890)\n\n"
    "O'chirib qo'yish uchun: 0\n\n"
    "\u26A0\uFE0F Botni shu kanalga oldindan ADMIN qilib (xabar yuborish huquqi bilan) "
    "qo'shib qo'ying, aks holda xabar yuborolmaydi."
)
NOT_A_VALID_CHANNEL = "\u274C Noto'g'ri format. @kanalim yoki -100... ko'rinishida yuboring."


def current_channel_line(channel) -> str:
    if not channel:
        return "\U0001F4E2 Xarid kanali: \u2014 (sozlanmagan)"
    return f"\U0001F4E2 Xarid kanali: {channel}"


def channel_saved_ok(channel) -> str:
    return f"\u2705 Kanal saqlandi: {channel}\n\nBot shu kanalga test xabar yubora oldi \u2014 hammasi tayyor."


def channel_saved_warning(channel, error: str) -> str:
    return (
        f"\u26A0\uFE0F Kanal saqlandi: {channel}\n\n"
        f"Lekin test xabar yuborishda xatolik chiqdi: {error}\n"
        f"Botni shu kanalga ADMIN qilib qo'shganingizni tekshiring."
    )


CHANNEL_DISABLED = "\u2705 Xarid kanali o'chirildi. Endi hech qaerga xabar yuborilmaydi."

ASK_CARD_NUMBER = (
    "Balans to'ldirishda foydalanuvchilarga ko'rsatiladigan karta raqamini yuboring "
    "(masalan: 8600 1234 5678 9012)."
)
ASK_CARD_HOLDER = "Endi karta egasining F.I.SH.ni yuboring (masalan: Aziz Karimov)."


def current_card_line(card_number: str, card_holder: str) -> str:
    return f"\U0001F4B3 Joriy karta: {card_number} ({card_holder})"


def card_saved(card_number: str, card_holder: str) -> str:
    return f"\u2705 Karta ma'lumotlari saqlandi:\n{card_number}\n{card_holder}"


ASK_FORCE_SUB_CHANNEL = (
    "Qo'shmoqchi bo'lgan majburiy obuna kanalini yuboring.\n\n"
    "\u2022 Ochiq kanal: @kanalim\n"
    "\u2022 Yopiq kanal: -100 bilan boshlanuvchi ID\n\n"
    "\u26A0\uFE0F Botni shu kanalga oldindan ADMIN qilib (a'zolarni ko'rish huquqi "
    "bilan) qo'shib qo'ying."
)
ASK_FORCE_SUB_URL = (
    "Endi \"Kanalga o'tish\" tugmasi ochadigan havolani yuboring "
    "(masalan: https://t.me/+AbCdEfGh).\n\n"
    "Ochiq kanal bo'lsa va @username'dan avtomatik hosil qilinishini xohlasangiz: -"
)
NOT_A_VALID_FORCE_SUB_CHANNEL = "\u274C Noto'g'ri format. @kanalim yoki -100... ko'rinishida yuboring."


ASK_REFUND_SECONDS = (
    "Raqam uchun SMS kod kelmasa, foydalanuvchi necha SONIYADAN keyin pulini "
    "o'zi qaytarib olishi mumkinligini kiriting (masalan: 600 — bu 10 daqiqa)."
)
NOT_A_VALID_SECONDS = "\u274C Noto'g'ri qiymat. Faqat musbat son kiriting, masalan: 600"


def current_refund_seconds_line(seconds: int) -> str:
    return f"\u23F1 Pul qaytarish kutish vaqti: {seconds} soniya (~{seconds // 60} daqiqa)"


def refund_seconds_saved(seconds: int) -> str:
    return f"\u2705 Saqlandi: {seconds} soniya (~{seconds // 60} daqiqa)"


ASK_ADD_ADMIN_ID = "Admin qilib qo'shmoqchi bo'lgan foydalanuvchining ID raqamini yuboring:"
ALREADY_ADMIN = "\u2139\uFE0F Bu foydalanuvchi allaqachon admin."


_TYPE_LABEL_STATS = {
    "number": "\U0001F4F1 Raqam",
    "stars": "\u2B50 Stars",
    "premium": "\U0001F48E Premium",
    "uc": "\U0001F3AE UC",
}


def stats_text(s: dict) -> str:
    by_type_lines = "\n".join(
        f"   {_TYPE_LABEL_STATS[k]}: {s['orders_by_type'].get(k, 0)}"
        for k in ("number", "stars", "premium", "uc")
    )
    revenue_by_type_lines = "\n".join(
        f"   {_TYPE_LABEL_STATS[k]}: {fmt_money(s['revenue_by_type'].get(k, 0))} so'm"
        for k in ("number", "stars", "premium", "uc")
    )
    return (
        f"\U0001F4CA Statistika\n\n"
        f"\U0001F465 Foydalanuvchilar\n"
        f"   Jami: {s['users_total']}\n"
        f"   Bugun qo'shilgan: {s['users_new_today']}\n"
        f"   Bloklangan: {s['users_banned']}\n"
        f"   Balanslar jami (botning \"qarzi\"): {fmt_money(s['balance_total'])} so'm\n\n"
        f"\U0001F4E6 Buyurtmalar\n"
        f"   Jami: {s['orders_total']}\n"
        f"{by_type_lines}\n"
        f"   Bugun: {s['orders_today']}\n\n"
        f"\U0001F4B5 Tushum (refund qilinganlar hisobga olinmagan)\n"
        f"   Bugun: {fmt_money(s['revenue_today'])} so'm\n"
        f"   Shu hafta (7 kun): {fmt_money(s['revenue_week'])} so'm\n"
        f"   Shu oy (30 kun): {fmt_money(s['revenue_month'])} so'm\n"
        f"   Jami: {fmt_money(s['revenue_total'])} so'm\n\n"
        f"\U0001F4B5 Tur bo'yicha tushum (jami)\n"
        f"{revenue_by_type_lines}\n\n"
        f"\u23F3 Kutilayotgan balans to'ldirish so'rovlari: {s['topups_pending']}"
    )


def revenue_graph_text(daily: list) -> str:
    """Kunlik tushumni ustunli (bar chart) ko'rinishida, oddiy Unicode
    belgilar bilan chizadi — tashqi grafik kutubxonasi (masalan
    matplotlib) talab qilinmaydi, shuning uchun har qanday serverda,
    qo'shimcha o'rnatishsiz ishlayveradi."""
    if not daily or all(v == 0 for _, v in daily):
        return "\U0001F4C8 Kunlik tushum (oxirgi kunlar)\n\nHali ma'lumot yo'q."

    max_val = max(v for _, v in daily) or 1
    bar_width = 18
    lines = ["\U0001F4C8 Kunlik tushum (oxirgi kunlar)\n"]
    for day, value in daily:
        filled = round((value / max_val) * bar_width) if max_val else 0
        bar = "\u2588" * filled + "\u2591" * (bar_width - filled)
        lines.append(f"{day}  {bar}  {fmt_money(value)}")
    return "\n".join(lines)


# ==============================================================
# TUGMALAR (keyboards)
# ==============================================================
# Bot API 9.4 (2026-02-09) — tugmalarga rang berish uchun "style" maydoni
# qo'shildi (KeyboardButton va InlineKeyboardButton). Ruxsat etilgan qiymatlar
# faqat shular: "danger" (qizil), "success" (yashil), "primary" (koʼk).
# Aiogram kutubxonasi bu maydonni hali rasman "tanimagan" versiyada boʼlsa ham,
# Telegram obyektlari noma'lum maydonlarni qabul qilib, serverga toʼgʼri
# yuborib beradi — shuning uchun bu yerda oddiy satr sifatida ishlatiladi.
# Bot API 9.4 ni qo'llaydigan yangi aiogram versiyalarida bu maydon rasman bor; eskilarida Telegram obyektlari
# noma'lum maydonlarni qabul qilib, serverga o'zi yuborib beradi. Muhitingizda ranglar muammo tug'dirsa (kutubxona yoki
# Telegram qo'llamasa) — .env ga BUTTON_STYLES=0 yozing: tugmalar ranglarsiz, lekin XATOSIZ ishlaydi.
# Bundan tashqari ishga tushishda _probe_button_style_support() kutubxonani o'zi tekshiradi va kerak bo'lsa
# ranglarni avtomatik o'chiradi.
BUTTON_STYLES_ENABLED = os.getenv("BUTTON_STYLES", "1").strip().lower() not in ("0", "false", "no", "off")
STYLE_DANGER = "danger" if BUTTON_STYLES_ENABLED else None    # qizil — bekor qilish / rad etish / orqaga
STYLE_SUCCESS = "success" if BUTTON_STYLES_ENABLED else None  # yashil — tasdiqlash / tasdiqlangan amallar
STYLE_PRIMARY = "primary" if BUTTON_STYLES_ENABLED else None  # koʼk — asosiy tanlov tugmalari


def _probe_button_style_support() -> None:
    """Ishga tushganda BIR marta: aiogram `style` maydonini serverga yuboriladigan tugma obyektiga
    qo'shyaptimi, tekshiradi. Qo'shmasa yoki xato bersa — ranglar o'chiriladi (bot ranglarsiz ishlayveradi,
    hech narsa buzilmaydi)."""
    global STYLE_DANGER, STYLE_SUCCESS, STYLE_PRIMARY
    if not BUTTON_STYLES_ENABLED:
        logging.info("Tugma ranglari o'chirilgan (BUTTON_STYLES=0)")
        return
    try:
        probe = InlineKeyboardButton(text="x", callback_data="x", style="primary")
        supported = probe.model_dump(exclude_none=True).get("style") == "primary"
    except Exception:
        supported = False
    if not supported:
        STYLE_DANGER = STYLE_SUCCESS = STYLE_PRIMARY = None
        logging.warning("Aiogram tugma 'style' maydonini qo'llamayapti — tugma ranglari o'chirildi.")


def _kb(rows: list) -> InlineKeyboardMarkup:
    """Inline klaviatura yig'ish: BO'SH qatorlar tashlab yuboriladi (Telegram bo'sh qatorli
    klaviaturani rad etadi va butun xabar yuborilmay qoladi)."""
    return InlineKeyboardMarkup(inline_keyboard=[list(row) for row in rows if row])


def nav_rows(back_callback: Optional[str] = None, *, cancel: bool = True) -> list:
    """Pastki navigatsiya QATORLARI ro'yxati: "Orqaga" va "Bekor qilish" BITTA qatorda (avval ikki qator
    edi). Natija qatorlar ro'yxati va u BO'SH bo'lishi ham mumkin (hech narsa so'ralmasa), shuning uchun
    uni `rows.extend(nav_rows(...))` bilan qo'shing — bo'sh natija hech qanday bo'sh qator hosil qilmaydi."""
    row = []
    if back_callback:
        row.append(InlineKeyboardButton(text=BTN_BACK, callback_data=back_callback, style=STYLE_PRIMARY))
    if cancel:
        row.append(InlineKeyboardButton(text=BTN_CANCEL, callback_data="cancel", style=STYLE_DANGER))
    return [row] if row else []


COPY_TEXT_MAX = 256   # Telegram: CopyTextButton matni 1..256 belgi


def _copy_text(value) -> Optional[str]:
    """Nusxalanadigan matn: bo'sh yoki 256 belgidan uzun bo'lsa None (Telegram bunday tugmali BUTUN xabarni rad etadi)."""
    text = str(value).strip() if value is not None else ""
    return text if 0 < len(text) <= COPY_TEXT_MAX else None


def _copy_button(label: str, value) -> Optional[InlineKeyboardButton]:
    """"Nusxalash" tugmasi; qiymat yaroqsiz bo'lsa None (tugma umuman qo'shilmaydi)."""
    text = _copy_text(value)
    if not text:
        return None
    return InlineKeyboardButton(text=label, copy_text=CopyTextButton(text=text))


def main_menu() -> ReplyKeyboardMarkup:
    """Asosiy menyu: 4 qator x 2 ustun (avval 6 qator edi). Xarid bo'limlari tepada."""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_NUMBER, style=STYLE_PRIMARY), KeyboardButton(text=BTN_STARS, style=STYLE_PRIMARY)],
            [KeyboardButton(text=BTN_PREMIUM, style=STYLE_PRIMARY), KeyboardButton(text=BTN_UC, style=STYLE_PRIMARY)],
            [KeyboardButton(text=BTN_BALANCE), KeyboardButton(text=BTN_ORDERS)],
            [KeyboardButton(text=BTN_REPEAT_ORDER), KeyboardButton(text=BTN_HELP)],
        ],
        resize_keyboard=True,
        input_field_placeholder="Bo'limni tanlang \U0001F447",
    )


def balance_menu() -> InlineKeyboardMarkup:
    return _kb([
        [InlineKeyboardButton(text=BTN_TOPUP, callback_data="topup:start", style=STYLE_PRIMARY)],
        [InlineKeyboardButton(text="\U0001F381 Do'stlarni taklif qilish", callback_data="referral:info", style=STYLE_PRIMARY)],
    ])


def cancel_inline(back_callback: Optional[str] = None) -> InlineKeyboardMarkup:
    return _kb(nav_rows(back_callback))


NUMBER_TYPE_TEXT = "Qanday raqam kerak?\n\n\U0001F4F1 Oddiy raqam \u2014 SMS kod uchun."
PICK_COUNTRY_TEXT = "Davlatni tanlang (narx \u2014 so'mda):"


def number_type_menu() -> InlineKeyboardMarkup:
    rows = [[
        InlineKeyboardButton(text="\U0001F4F1 Oddiy raqam", callback_data="numtype:regular", style=STYLE_PRIMARY),
        InlineKeyboardButton(text="\U0001F510 Tayyor akkaunt", callback_data="numtype:ready", style=STYLE_PRIMARY),
    ]]
    rows.extend(nav_rows())
    return _kb(rows)


NUMBER_PURCHASE_WARNING = (
    "\u26A0\uFE0F DIQQAT! Muhim ogohlantirish\n\n"
    "\u2139\uFE0F Raqam sotib olishdan avval o'qing:\n\n"
    "1\uFE0F\u20E3 Raqam muzlashi yoki bloklanishi mumkin (Telegramning o'z "
    "siyosati tufayli, bizdan emas).\n"
    "\u274C Rasmiy Telegram ilovasidan foydalanmang\n"
    "\u2705 Ishonchli, norasmiy ilovadan foydalaning\n"
    "\u2764\uFE0F Maslahat: Telegraph\n\n"
    "2\uFE0F\u20E3 Sarflangan pul QAYTARILMAYDI. \"Sotib olish\"ni bossangiz, "
    "buyurtma darhol amalga oshadi.\n\n"
    "3\uFE0F\u20E3 Kirish kodi va 2FA parol bexato keladi. Kod kelmasa \u2014 "
    "aloqangizni almashtiring (Wi-Fi \u2194 mobil internet).\n\n"
    "4\uFE0F\u20E3 Agar akkaunt spam cheklovida bo'lsa, @spambot orqali "
    "soniyalar ichida ochiladi.\n\n"
    "Rozimisiz?"
)


def number_warning_menu() -> InlineKeyboardMarkup:
    return _kb([[
        InlineKeyboardButton(text="\u2705 Roziman", callback_data="numwarn:confirm", style=STYLE_SUCCESS),
        InlineKeyboardButton(text=BTN_CANCEL, callback_data="cancel", style=STYLE_DANGER),
    ]])


# ==============================================================
# DAVLAT NOMLARI (ISO kod -> o'zbekcha nom)
# ==============================================================
# SmmUpper API davlatlarni faqat qisqa ISO kod bilan qaytaradi (KG, GE, AZ...).
# Lug'atda topilmagan kod chiqib qolsa, kodning o'zi ko'rsatiladi — shuning
# uchun yangi/kam uchraydigan kod qo'shilib qolsa ham bot xatosiz ishlayveradi.
COUNTRY_NAMES: Dict[str, str] = {
    "AD": "Andorra", "AE": "BAA", "AF": "Afg'oniston", "AG": "Antigua va Barbuda",
    "AI": "Angilya", "AL": "Albaniya", "AM": "Armaniston", "AO": "Angola",
    "AQ": "Antarktida", "AR": "Argentina", "AS": "Amerika Samoasi", "AT": "Avstriya",
    "AU": "Avstraliya", "AW": "Aruba", "AX": "Oland orollari", "AZ": "Ozarbayjon",
    "BA": "Bosniya va Gertsegovina", "BB": "Barbados", "BD": "Bangladesh", "BE": "Belgiya",
    "BF": "Burkina-Faso", "BG": "Bolgariya", "BH": "Bahrayn", "BI": "Burundi",
    "BJ": "Benin", "BL": "Sen-Bartelemi", "BM": "Bermuda orollari", "BN": "Bruney",
    "BO": "Boliviya", "BQ": "Boneyr, Sint-Estatius va Saba", "BR": "Braziliya", "BS": "Bagama orollari",
    "BT": "Butan", "BV": "Buve oroli", "BW": "Botsvana", "BY": "Belarus",
    "BZ": "Beliz", "CA": "Kanada", "CC": "Kokos orollari", "CD": "Kongo DR",
    "CF": "Markaziy Afrika Respublikasi", "CG": "Kongo", "CH": "Shveytsariya", "CI": "Kot-d'Ivuar",
    "CK": "Kuk orollari", "CL": "Chili", "CM": "Kamerun", "CN": "Xitoy",
    "CO": "Kolumbiya", "CR": "Kosta-Rika", "CU": "Kuba", "CV": "Kabo-Verde",
    "CW": "Kurasao", "CX": "Rojdestvo oroli", "CY": "Kipr", "CZ": "Chexiya",
    "DE": "Germaniya", "DJ": "Jibuti", "DK": "Daniya", "DM": "Dominika",
    "DO": "Dominikan Respublikasi", "DZ": "Jazoir", "EC": "Ekvador", "EE": "Estoniya",
    "EG": "Misr", "EH": "G'arbiy Sahro", "ER": "Eritreya", "ES": "Ispaniya",
    "ET": "Efiopiya", "FI": "Finlyandiya", "FJ": "Fiji", "FK": "Folklend orollari",
    "FM": "Mikroneziya", "FO": "Farer orollari", "FR": "Fransiya", "GA": "Gabon",
    "GB": "Buyuk Britaniya", "GD": "Grenada", "GE": "Gruziya", "GF": "Frantsuz Gvianasi",
    "GG": "Gernsi", "GH": "Gana", "GI": "Gibraltar", "GL": "Grenlandiya",
    "GM": "Gambiya", "GN": "Gvineya", "GP": "Gvadelupa", "GQ": "Ekvatorial Gvineya",
    "GR": "Gretsiya", "GS": "Janubiy Georgiya va Janubiy Sandvich orollari", "GT": "Gvatemala", "GU": "Guam",
    "GW": "Gvineya-Bisau", "GY": "Gayana", "HK": "Gonkong", "HM": "Gerd va Makdonald orollari",
    "HN": "Gonduras", "HR": "Xorvatiya", "HT": "Gaiti", "HU": "Vengriya",
    "ID": "Indoneziya", "IE": "Irlandiya", "IL": "Isroil", "IM": "Men oroli",
    "IN": "Hindiston", "IO": "Britaniyaning Hind okeanidagi hududi", "IQ": "Iroq", "IR": "Eron",
    "IS": "Islandiya", "IT": "Italiya", "JE": "Jersi", "JM": "Yamayka",
    "JO": "Iordaniya", "JP": "Yaponiya", "KE": "Keniya", "KG": "Qirg'iziston",
    "KH": "Kambodja", "KI": "Kiribati", "KM": "Komor orollari", "KN": "Sent-Kits va Nevis",
    "KP": "Shimoliy Koreya", "KR": "Janubiy Koreya", "KW": "Kuvayt", "KY": "Kayman orollari",
    "KZ": "Qozog'iston", "LA": "Laos", "LB": "Livan", "LC": "Sent-Lyusiya",
    "LI": "Lixtenshteyn", "LK": "Shri-Lanka", "LR": "Liberiya", "LS": "Lesoto",
    "LT": "Litva", "LU": "Lyuksemburg", "LV": "Latviya", "LY": "Liviya",
    "MA": "Marokash", "MC": "Monako", "MD": "Moldova", "ME": "Chernogoriya",
    "MF": "Sen-Marten", "MG": "Madagaskar", "MH": "Marshall orollari", "MK": "Shimoliy Makedoniya",
    "ML": "Mali", "MM": "Myanma", "MN": "Mongoliya", "MO": "Makao",
    "MP": "Shimoliy Marian orollari", "MQ": "Martinika", "MR": "Mavritaniya", "MS": "Montserrat",
    "MT": "Malta", "MU": "Mavrikiy", "MV": "Maldiv orollari", "MW": "Malavi",
    "MX": "Meksika", "MY": "Malayziya", "MZ": "Mozambik", "NA": "Namibiya",
    "NC": "Yangi Kaledoniya", "NE": "Niger", "NF": "Norfolk oroli", "NG": "Nigeriya",
    "NI": "Nikaragua", "NL": "Niderlandiya", "NO": "Norvegiya", "NP": "Nepal",
    "NR": "Nauru", "NU": "Niue", "NZ": "Yangi Zelandiya", "OM": "Ummon",
    "PA": "Panama", "PE": "Peru", "PF": "Frantsuz Polineziyasi", "PG": "Papua-Yangi Gvineya",
    "PH": "Filippin", "PK": "Pokiston", "PL": "Polsha", "PM": "Sen-Pyer va Mikelon",
    "PN": "Pitkern orollari", "PR": "Puerto-Riko", "PS": "Falastin", "PT": "Portugaliya",
    "PW": "Palau", "PY": "Paragvay", "QA": "Qatar", "RE": "Reyunion",
    "RO": "Ruminiya", "RS": "Serbiya", "RU": "Rossiya", "RW": "Ruanda",
    "SA": "Saudiya Arabistoni", "SB": "Solomon orollari", "SC": "Seyshel orollari", "SD": "Sudan",
    "SE": "Shvetsiya", "SG": "Singapur", "SH": "Muqaddas Yelena oroli", "SI": "Sloveniya",
    "SJ": "Svalbard va Yan-Mayen", "SK": "Slovakiya", "SL": "Serra-Leone", "SM": "San-Marino",
    "SN": "Senegal", "SO": "Somali", "SR": "Surinam", "SS": "Janubiy Sudan",
    "ST": "San-Tome va Prinsipi", "SV": "Salvador", "SX": "Sint-Marten", "SY": "Suriya",
    "SZ": "Esvatini", "TC": "Turks va Kaykos orollari", "TD": "Chad", "TF": "Fransiyaning janubiy hududlari",
    "TG": "Togo", "TH": "Tailand", "TJ": "Tojikiston", "TK": "Tokelau",
    "TL": "Sharqiy Timor", "TM": "Turkmaniston", "TN": "Tunis", "TO": "Tonga",
    "TR": "Turkiya", "TT": "Trinidad va Tobago", "TV": "Tuvalu", "TW": "Tayvan",
    "TZ": "Tanzaniya", "UA": "Ukraina", "UG": "Uganda", "US": "AQSH",
    "UY": "Urugvay", "UZ": "O'zbekiston", "VA": "Vatikan", "VC": "Sent-Vinsent va Grenadin",
    "VE": "Venesuela", "VG": "Britaniya Virjin orollari", "VI": "AQSH Virjin orollari", "VN": "Vetnam",
    "VU": "Vanuatu", "WF": "Uellis va Futuna", "WS": "Samoa", "XK": "Kosovo",
    "YE": "Yaman", "YT": "Mayotta", "ZA": "JAR", "ZM": "Zambiya",
    "ZW": "Zimbabve",
}

COUNTRIES_PAGE_SIZE = 20  # bitta sahifada nechta davlat (10 qator x 2 ustun)


def country_display_name(code: str) -> str:
    return COUNTRY_NAMES.get(code.upper(), code.upper())


def country_flag(code: str) -> str:
    """ISO kodni bayroq emojisiga aylantiradi (masalan 'KG' -> \U0001F1F0\U0001F1EC).
    Unicode Regional Indicator Symbol'lardan foydalanadi. Faqat COUNTRY_NAMES
    lug'atida bor (ya'ni nomi ham ma'lum) kodlar uchun bayroq qaytaradi —
    aks holda bo'sh satr, chunki noma'lum/soxta kod uchun mazmunsiz "bayroq"
    ko'rsatishning ma'nosi yo'q."""
    code = code.upper()
    if code not in COUNTRY_NAMES or len(code) != 2:
        return ""
    return "".join(chr(0x1F1E6 + (ord(ch) - ord("A"))) for ch in code)


COUNTRY_BUTTON_NAME_MAX = 13   # tugmada davlat nomi shundan uzun bo'lsa "…" bilan qisqartiriladi (narx doim ko'rinsin)


def _country_button_label(code: str, info: dict, markup_percent: float = 0.0) -> str:
    """Davlat tugmasi matni: "🇹🇷 Turkiya · 25 000" (narx so'mda — buni ro'yxat sarlavhasi aytadi).
    Qisqa yorliq: ikki ustunli tugmada narx Telegram tomonidan kesilib qolmasin. To'liq nom tanlagandan
    keyingi tasdiqlash ekranida ko'rinadi."""
    price = info.get("price", "?")
    name = country_display_name(code)
    if len(name) > COUNTRY_BUTTON_NAME_MAX:
        name = name[:COUNTRY_BUTTON_NAME_MAX - 1].rstrip() + "\u2026"
    flag = country_flag(code)
    label_name = f"{flag} {name}" if flag else name
    if isinstance(price, (int, float)):
        shown_price = max(0, round(float(price) * _markup_multiplier(markup_percent)))
        return f"{label_name} \u00B7 {fmt_money(shown_price)}"
    return label_name


def _normalize_query(text: str) -> str:
    """Qidiruvda apostrof turlari va katta/kichik harf farqini bekor qiladi."""
    for ch in ("'", "\u2018", "\u2019", "\u02bb", "\u02bc", "`"):
        text = text.replace(ch, "")
    return text.lower().strip()


def _sorted_country_items(countries: dict, sort: str = "name"):
    if sort == "price":
        def price_key(kv):
            price = kv[1].get("price")
            return price if isinstance(price, (int, float)) else float("inf")
        return sorted(countries.items(), key=price_key)
    return sorted(countries.items(), key=lambda kv: country_display_name(kv[0]))


# davlatlar ro'yxatini qanday ko'rsatish rejimi -> (saralash turi, sahifalash prefiksi)
_COUNTRY_MENU_MODES = {
    "browse": ("name", "ctypg"),
    "cheap": ("price", "ctycpg"),
    "search": ("name", "ctyspg"),
}


def countries_menu(server: int, countries: dict, page: int = 0, *, mode: str = "browse", markup_percent: float = 0.0) -> InlineKeyboardMarkup:
    """Davlatlar ro'yxatini to'liq nom (+ bayroq) bilan, sahifalab ko'rsatadi
    (bitta ulkan ro'yxat o'rniga). `mode`:
      - "browse": alifbo tartibida, pastda TOP10/Arzon/Qidirish tugmalari (bitta qatorda)
      - "cheap":  narx bo'yicha arzondan qimmatga, "orqaga" (ro'yxatga) bilan
      - "search": qidiruv natijalari, "qayta qidirish" + "orqaga" (ro'yxatga) bilan
    `markup_percent` — narxlarni ko'rsatishda qo'shiladigan ustama (admin
    panelda o'zgartirilganda shu yerdagi ko'rinish ham darhol yangilanishi
    uchun chaqiruvchi joyda oldindan hisoblab, parametr sifatida beriladi).
    Chaqiruvchi ro'yxat uchun "number" bo'limi ustamasini beradi.
    """
    sort_key, pg_prefix = _COUNTRY_MENU_MODES[mode]
    items = _sorted_country_items(countries, sort=sort_key)
    total_pages = max(1, (len(items) + COUNTRIES_PAGE_SIZE - 1) // COUNTRIES_PAGE_SIZE)
    page = max(0, min(page, total_pages - 1))
    chunk = items[page * COUNTRIES_PAGE_SIZE: page * COUNTRIES_PAGE_SIZE + COUNTRIES_PAGE_SIZE]

    rows = []
    row = []
    for code, info in chunk:
        row.append(InlineKeyboardButton(text=_country_button_label(code, info, markup_percent), callback_data=f"cty:{server}:{code}", style=STYLE_PRIMARY))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    if total_pages > 1:
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton(text="\u25C0\uFE0F", callback_data=f"{pg_prefix}:{server}:{page - 1}", style=STYLE_PRIMARY))
        nav.append(InlineKeyboardButton(text=f"{page + 1}/{total_pages}", callback_data="ctynoop", style=STYLE_PRIMARY))
        if page < total_pages - 1:
            nav.append(InlineKeyboardButton(text="\u25B6\uFE0F", callback_data=f"{pg_prefix}:{server}:{page + 1}", style=STYLE_PRIMARY))
        rows.append(nav)

    if mode == "browse":
        rows.append([
            InlineKeyboardButton(text="\U0001F3C6 TOP 10", callback_data=f"ctytop:{server}", style=STYLE_PRIMARY),
            InlineKeyboardButton(text="\U0001F4C9 Arzon", callback_data=f"ctycpg:{server}:0", style=STYLE_PRIMARY),
            InlineKeyboardButton(text="\U0001F50D Qidirish", callback_data=f"ctysearch:{server}", style=STYLE_PRIMARY),
        ])
        rows.extend(nav_rows("numback:type"))
    elif mode == "search":
        rows.append([InlineKeyboardButton(text="\U0001F50D Qayta qidirish", callback_data=f"ctysearch:{server}", style=STYLE_PRIMARY)])
        rows.extend(nav_rows(f"ctyback:{server}"))
    else:  # "cheap"
        rows.extend(nav_rows(f"ctyback:{server}"))

    return _kb(rows)


def confirm_menu(confirm_data: str, back_callback: Optional[str] = None) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(text=BTN_CONFIRM, callback_data=confirm_data, style=STYLE_SUCCESS)]]
    rows.extend(nav_rows(back_callback))
    return _kb(rows)


STARS_PRESETS = (50, 100, 250, 500, 1000)


def stars_amount_menu() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text=str(n), callback_data=f"starsamt:{n}", style=STYLE_PRIMARY) for n in STARS_PRESETS],
        [InlineKeyboardButton(text="\u270F\uFE0F Boshqa summa", callback_data="starsamt:custom", style=STYLE_PRIMARY)],
    ]
    rows.extend(nav_rows("starsback:username"))
    return _kb(rows)


def premium_months_menu(prices: Optional[Dict[int, int]] = None) -> InlineKeyboardMarkup:
    prices = prices or {}

    def label(months: int) -> str:
        price = prices.get(months)
        if isinstance(price, (int, float)) and price > 0:
            return f"\U0001F451 {months} oy \u2014 {fmt_money(round(price))} so'm"
        return f"\U0001F451 {months} oy"

    rows = [
        [InlineKeyboardButton(text=label(months), callback_data=f"premmonths:{months}", style=STYLE_PRIMARY)]
        for months in (3, 6, 12)
    ]
    rows.extend(nav_rows())
    return _kb(rows)


def check_code_menu(order_pk: int, copy_number: Optional[str] = None) -> InlineKeyboardMarkup:
    # "Pulni qaytarish" ATAYIN alohida qatorda: qaytarish qaytarib bo'lmaydigan amal, shuning uchun
    # "Kodni tekshirish" bilan yonma-yon qo'yilib tasodifan bosilib ketmasligi kerak.
    rows = []
    copy_btn = _copy_button("\U0001F4CB Raqamni nusxalash", copy_number)
    if copy_btn:
        rows.append([copy_btn])
    rows.append([InlineKeyboardButton(text=BTN_CHECK_CODE, callback_data=f"numcheck:{order_pk}", style=STYLE_PRIMARY)])
    rows.append([InlineKeyboardButton(text="\U0001F4B8 Pulni qaytarish", callback_data=f"numrefund:{order_pk}", style=STYLE_DANGER)])
    return _kb(rows)


def _code_copy_menu(code, password: str = "") -> InlineKeyboardMarkup:
    """SMS kodi (va bo'lsa — 2FA parol) uchun aniq "nusxalash" tugmalari.
    <code> teglari orqali ham (matnni bosib turib) nusxalab bo'ladi, lekin
    alohida tugma ancha aniqroq va barcha foydalanuvchilar uchun tushunarli.
    Kod ham, parol ham bo'lsa — ikkalasi BITTA qatorda qisqa yorliq bilan;
    faqat bittasi bo'lsa — to'liq yorliq bilan. Telegram CopyTextButton bo'sh
    matnni qabul qilmaydi (1-256 belgi) — bunday qiymatda tugma qo'shilmaydi
    (aks holda BUTUN xabar yuborilmay qolar edi)."""
    entries = [
        ("\U0001F4CB Kod", "\U0001F4CB Kodni nusxalash", code),
        ("\U0001F510 Parol", "\U0001F510 Parolni nusxalash", password),
    ]
    valid = [(short, full, value) for short, full, value in entries if _copy_text(value)]
    use_short = len(valid) > 1
    buttons = [_copy_button(short if use_short else full, value) for short, full, value in valid]
    return _kb([buttons])


def _card_copy_menu(card_number: str) -> InlineKeyboardMarkup:
    """To'ldirish uchun ko'rsatilgan plastik karta raqamini bitta bosishda
    nusxalash tugmasi — pul o'tkazishda raqamni qo'lda terib xato
    qilmaslik uchun. Bo'sh/bo'sh joy bo'lsa tugma qo'shilmaydi (yuqoridagi
    izohga qarang). "Bekor qilish" bilan bitta qatorda."""
    row = []
    copy_btn = _copy_button("\U0001F4CB Karta raqami", card_number)
    if copy_btn:
        row.append(copy_btn)
    row.append(InlineKeyboardButton(text=BTN_CANCEL, callback_data="cancel", style=STYLE_DANGER))
    return _kb([row])



def _admin_back(label: str, callback_data: str) -> list:
    """Admin bo'limlaridagi qisqa "orqaga" QATORI (qizil tugma). Uzun "...ga qaytish" o'rniga qisqa nom."""
    return [InlineKeyboardButton(text=f"\u2B05\uFE0F {label}", callback_data=callback_data, style=STYLE_DANGER)]


def admin_menu(owner: bool = False) -> InlineKeyboardMarkup:
    """Asosiy admin panel — ixcham: bo'limlarga guruhlangan, har biri
    bosilganda xabar tahrirlanib (edit) tegishli kichik menyuga o'tadi."""
    rows = [
        [
            InlineKeyboardButton(text="\U0001F465 Foydalanuvchilar", callback_data="adm:menu:users", style=STYLE_PRIMARY),
            InlineKeyboardButton(text="\U0001F4CA Statistika", callback_data="adm:stats", style=STYLE_PRIMARY),
        ],
        [
            InlineKeyboardButton(text="\U0001F4B3 To'lovlar", callback_data="adm:payments", style=STYLE_PRIMARY),
            InlineKeyboardButton(text="\U0001F6D2 Buyurtmalar", callback_data="adm:orders:menu", style=STYLE_PRIMARY),
        ],
        [
            InlineKeyboardButton(text="\u2699\uFE0F Sozlamalar", callback_data="adm:menu:settings", style=STYLE_PRIMARY),
            InlineKeyboardButton(text="\U0001F510 Majburiy obuna", callback_data="adm:forcesub", style=STYLE_PRIMARY),
        ],
        [
            InlineKeyboardButton(text="\U0001F464 Adminlar", callback_data="adm:admins", style=STYLE_PRIMARY),
            InlineKeyboardButton(text="\U0001F4E2 Xabar yuborish", callback_data="adm:broadcast", style=STYLE_PRIMARY),
        ],
    ]
    last = [InlineKeyboardButton(text="\U0001F504 Yangilash", callback_data="adm:refresh", style=STYLE_PRIMARY)]
    if owner:
        # Avto-to'lov bo'limi FAQAT .env dagi asosiy adminlarga ko'rinadi (shaxsiy).
        last.insert(0, InlineKeyboardButton(text="\U0001F916 Avto-to'lov", callback_data="adm:apay", style=STYLE_PRIMARY))
    rows.append(last)
    return _kb(rows)


def admin_cancel_menu() -> InlineKeyboardMarkup:
    return _kb([_admin_back("Admin panel", "adm:refresh")])


def stats_menu() -> InlineKeyboardMarkup:
    return _kb([
        [
            InlineKeyboardButton(text="\U0001F3C6 Top userlar", callback_data="adm:topusers", style=STYLE_PRIMARY),
            InlineKeyboardButton(text="\U0001F30D Top davlatlar", callback_data="adm:topcountries", style=STYLE_PRIMARY),
        ],
        [InlineKeyboardButton(text="\U0001F4C8 Grafik (7 kun)", callback_data="adm:graph", style=STYLE_PRIMARY)],
        _admin_back("Admin panel", "adm:refresh"),
    ])


def users_admin_menu() -> InlineKeyboardMarkup:
    return _kb([
        [
            InlineKeyboardButton(text="\U0001F50E Qidirish", callback_data="adm:find", style=STYLE_PRIMARY),
            InlineKeyboardButton(text="\U0001F4B0 Balans", callback_data="adm:balance", style=STYLE_PRIMARY),
        ],
        [
            InlineKeyboardButton(text="\U0001F6AB Bloklash", callback_data="adm:ban", style=STYLE_DANGER),
            InlineKeyboardButton(text="\u2705 Blokdan chiqarish", callback_data="adm:unban", style=STYLE_SUCCESS),
        ],
        _admin_back("Orqaga", "adm:refresh"),
    ])


def users_cancel_menu() -> InlineKeyboardMarkup:
    return _kb([_admin_back("Foydalanuvchilar", "adm:menu:users")])


def admin_user_actions_menu(user_id: int, banned: bool) -> InlineKeyboardMarkup:
    """Foydalanuvchi profili kartasi ostidagi tezkor amallar — ID'ni qayta
    kiritmasdan, to'g'ridan-to'g'ri shu foydalanuvchi ustida ishlash uchun."""
    ban_btn = (
        InlineKeyboardButton(text="\u2705 Blokdan chiqarish", callback_data=f"adm:quickunban:{user_id}", style=STYLE_SUCCESS)
        if banned else
        InlineKeyboardButton(text="\U0001F6AB Bloklash", callback_data=f"adm:quickban:{user_id}", style=STYLE_DANGER)
    )
    return _kb([
        [
            InlineKeyboardButton(text="\U0001F4B0 Balans", callback_data=f"adm:quickbalance:{user_id}", style=STYLE_PRIMARY),
            InlineKeyboardButton(text="\U0001F6D2 Buyurtmalari", callback_data=f"adm:orders:user:{user_id}", style=STYLE_PRIMARY),
        ],
        [ban_btn],
        _admin_back("Foydalanuvchilar", "adm:menu:users"),
    ])


def settings_admin_menu() -> InlineKeyboardMarkup:
    return _kb([
        [
            InlineKeyboardButton(text="\U0001F511 API kalit", callback_data="adm:apikey", style=STYLE_PRIMARY),
            InlineKeyboardButton(text="\U0001F4C8 Narx ustamasi", callback_data="adm:markup", style=STYLE_PRIMARY),
        ],
        [
            InlineKeyboardButton(text="\U0001F4E2 Xarid kanali", callback_data="adm:channel", style=STYLE_PRIMARY),
            InlineKeyboardButton(text="\U0001F4B3 To'lov kartasi", callback_data="adm:card", style=STYLE_PRIMARY),
        ],
        [InlineKeyboardButton(text="\u23F1 Pul qaytarish vaqti", callback_data="adm:refundtime", style=STYLE_PRIMARY)],
        _admin_back("Orqaga", "adm:refresh"),
    ])


def settings_cancel_menu() -> InlineKeyboardMarkup:
    return _kb([_admin_back("Sozlamalar", "adm:menu:settings")])


def markup_admin_menu(overview: dict) -> InlineKeyboardMarkup:
    """Narx ustamasi paneli: har bir bo'lim tugmasida JORIY foiz ko'rinib turadi (matnni o'qimasdan ham)."""
    def category_button(category: str) -> InlineKeyboardButton:
        percent, _custom = overview["items"][category]
        return InlineKeyboardButton(
            text=f"{MARKUP_LABELS[category]}: {percent:g}%",
            callback_data=f"adm:mk:{category}",
            style=STYLE_PRIMARY,
        )

    return _kb([
        [category_button("number"), category_button("stars")],
        [category_button("premium"), category_button("uc")],
        [InlineKeyboardButton(text="\U0001F310 Hammasiga bir xil", callback_data="adm:mk:all", style=STYLE_PRIMARY)],
        _admin_back("Sozlamalar", "adm:menu:settings"),
    ])


def markup_cancel_menu() -> InlineKeyboardMarkup:
    return _kb([_admin_back("Ustamalar", "adm:markup")])


def forcesub_admin_menu(channels: list) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text=f"\u274C {c.get('channel')}", callback_data=f"adm:fs:del:{i}", style=STYLE_DANGER)]
        for i, c in enumerate(channels)
    ]
    rows.append([InlineKeyboardButton(text="\u2795 Kanal qo'shish", callback_data="adm:fs:add", style=STYLE_PRIMARY)])
    rows.append(_admin_back("Orqaga", "adm:refresh"))
    return _kb(rows)


def fs_cancel_menu() -> InlineKeyboardMarkup:
    return _kb([_admin_back("Kanallar", "adm:forcesub")])


def admins_admin_menu(extra_ids: list) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text=f"\u274C {uid}", callback_data=f"adm:adm:del:{uid}", style=STYLE_DANGER)]
        for uid in extra_ids
    ]
    rows.append([InlineKeyboardButton(text="\u2795 Admin qo'shish", callback_data="adm:adm:add", style=STYLE_PRIMARY)])
    rows.append(_admin_back("Orqaga", "adm:refresh"))
    return _kb(rows)


def admins_cancel_menu() -> InlineKeyboardMarkup:
    return _kb([_admin_back("Adminlar", "adm:admins")])


USERS_MENU_TEXT = "\U0001F465 Foydalanuvchilar\n\nKerakli amalni tanlang:"


def _is_valid_button_url(url: str) -> bool:
    """Telegram inline tugmasi uchun url= faqat http(s):// yoki tg:// bilan
    boshlanishi kerak — aks holda Telegram butun xabarni rad etadi (faqat
    tugmani emas). Havola noto'g'ri formatda saqlanib qolgan bo'lsa ham,
    shu tekshiruv orqasida majburiy-obuna xabari umuman yubormay qolib
    ketmaydi."""
    return bool(url) and url.startswith(("http://", "https://", "tg://"))


def force_sub_menu(channels: list) -> InlineKeyboardMarkup:
    rows = []
    multiple = len(channels) > 1
    for i, item in enumerate(channels, start=1):
        url = (item or {}).get("url") or ""
        if _is_valid_button_url(url):
            label = f"\U0001F4E2 {i}-kanalga o'tish" if multiple else "\U0001F4E2 Kanalga o'tish"
            rows.append([InlineKeyboardButton(text=label, url=url)])
    rows.append([InlineKeyboardButton(text="\u2705 A'zo bo'ldim", callback_data="forcesub:check", style=STYLE_SUCCESS)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def topup_review_menu(topup_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="\u2705 Tasdiqlash", callback_data=f"topup:approve:{topup_id}", style=STYLE_SUCCESS),
            InlineKeyboardButton(text="\u274C Rad etish", callback_data=f"topup:reject:{topup_id}", style=STYLE_DANGER),
        ],
        [InlineKeyboardButton(text="\u270F\uFE0F Summani tuzatish", callback_data=f"topup:edit:{topup_id}", style=STYLE_PRIMARY)],
    ])


# ==============================================================
# FSM HOLATLARI (states)
# ==============================================================
class TopUp(StatesGroup):
    amount = State()
    photo = State()


class BuyNumber(StatesGroup):
    choosing_country = State()
    searching_country = State()
    confirming = State()


class BuyStars(StatesGroup):
    username = State()
    amount = State()
    confirming = State()


class BuyPremium(StatesGroup):
    months = State()
    username = State()
    confirming = State()


class BuyUC(StatesGroup):
    tariff = State()
    account = State()
    confirming = State()


class HelpRequest(StatesGroup):
    message = State()


class CheckOrder(StatesGroup):
    order_id = State()


class AdminPanel(StatesGroup):
    find_user = State()
    balance_id = State()
    balance_amount = State()
    ban_id = State()
    unban_id = State()
    broadcast_text = State()
    api_key = State()
    markup_percent = State()
    channel_id = State()
    card_number = State()
    card_holder = State()
    force_sub_channel = State()
    force_sub_url = State()
    refund_seconds = State()
    add_admin_id = State()
    order_search_id = State()
    order_search_user = State()
    topup_amount = State()
    apay_card_number = State()
    apay_card_holder = State()
    apay_ttl = State()


# ==============================================================
# UMUMIY HANDLERLAR (handler_common)
# ==============================================================
router_common = Router(name="common")
# MUHIM: router_admin ATAYIN shu yerda, boshqa routerlar bilan birga
# e'lon qilinadi (garchi uning handlerlari faylning pastida, "ADMIN PANEL
# HANDLER" bo'limida joylashgan bo'lsa ham). Sababi: pastroqda,
# router_start bo'limi ichida (support_reply_filter uchun)
# "@router_admin.message(...)" ishlatiladi — Python esa faylni yuqoridan
# pastga o'qiganda, shu qatorga yetguncha "router_admin" nomi allaqachon
# mavjud bo'lishi kerak. Aks holda xuddi shu joyda "NameError: name
# 'router_admin' is not defined" xatosi bilan bot ISHGA TUSHISHNING O'ZIDA
# qulab tushadi (Render logida aynan shu xato ko'ringan edi).
router_admin = Router(name="admin")

SETTINGS_KEY_CHANNEL_ID = "channel_id"
SETTINGS_KEY_CARD_NUMBER = "card_number"
SETTINGS_KEY_CARD_HOLDER = "card_holder"
SETTINGS_KEY_FORCE_SUB_CHANNEL = "force_sub_channel"
SETTINGS_KEY_FORCE_SUB_URL = "force_sub_url"
SETTINGS_KEY_FORCE_SUB_CHANNELS = "force_sub_channels_v2"  # JSON: [{"channel": ..., "url": ...}, ...]
SETTINGS_KEY_REFUND_SECONDS = "refund_eligible_seconds"
SETTINGS_KEY_EXTRA_ADMINS = "extra_admins"
_NOT_SUBSCRIBED_STATUSES = {"left", "kicked"}


def is_free_text(message: Message) -> bool:
    """FSM holatida 'erkin matn' kutayotgan handlerlar uchun filtr:
    agar foydalanuvchi bosh menyu tugmalaridan birini bossa, bu matnni
    username/summa deb noto'g'ri qabul qilmaslik uchun False qaytaradi —
    shunda navbatdagi (tegishli) handler ishga tushadi.
    """
    return bool(message.text) and message.text not in RESERVED_TEXTS


def _parse_channel_id(raw: str):
    raw = (raw or "").strip()
    if not raw:
        return None
    if raw.lstrip("-").isdigit():
        return int(raw)
    return raw


async def get_channel_id():
    """Admin panel orqali bazaga saqlangan kanal bo'lsa o'shani, aks holda
    .env (CHANNEL_ID) dagisini qaytaradi. Hech biri sozlanmagan bo'lsa None."""
    raw = await get_setting(SETTINGS_KEY_CHANNEL_ID, default=None)
    if raw is not None:
        return _parse_channel_id(raw)
    return CHANNEL_ID or None


async def get_card_info():
    """Admin panel orqali bazaga saqlangan karta ma'lumoti bo'lsa o'shani,
    aks holda .env (CARD_NUMBER / CARD_HOLDER) dagisini qaytaradi."""
    card_number = await get_setting(SETTINGS_KEY_CARD_NUMBER, default=CARD_NUMBER)
    card_holder = await get_setting(SETTINGS_KEY_CARD_HOLDER, default=CARD_HOLDER)
    return card_number, card_holder


async def get_force_sub_channels() -> list:
    """Majburiy obuna kanallari ro'yxatini qaytaradi — har bir element
    {"channel": ..., "url": ...} ko'rinishida. Yangi (bir nechta kanalli)
    formatda saqlangan bo'lsa o'shani o'qiydi; aks holda eski bitta-kanalli
    sozlamadan (yoki .env'dan) bir martalik migratsiya qiladi — shunda
    yangilanishdan keyin ilgari sozlangan kanal yo'qolib qolmaydi."""
    raw = await get_setting(SETTINGS_KEY_FORCE_SUB_CHANNELS, default=None)
    if raw:
        try:
            data = json.loads(raw)
            if isinstance(data, list):
                return data
        except (json.JSONDecodeError, TypeError):
            pass

    old_channel = await get_setting(SETTINGS_KEY_FORCE_SUB_CHANNEL, default=None)
    channel = old_channel if old_channel is not None else FORCE_SUB_CHANNEL
    if not channel:
        return []
    old_url = await get_setting(SETTINGS_KEY_FORCE_SUB_URL, default=None)
    url = old_url if old_url is not None else FORCE_SUB_CHANNEL_URL
    if not url and channel.startswith("@"):
        url = f"https://t.me/{channel.lstrip('@')}"
    return [{"channel": channel, "url": url}]


async def set_force_sub_channels(channels: list):
    await set_setting(SETTINGS_KEY_FORCE_SUB_CHANNELS, json.dumps(channels, ensure_ascii=False))


async def add_force_sub_channel(channel: str, url: str):
    channels = await get_force_sub_channels()
    channels.append({"channel": channel, "url": url})
    await set_force_sub_channels(channels)


async def remove_force_sub_channel(index: int):
    channels = await get_force_sub_channels()
    if 0 <= index < len(channels):
        channels.pop(index)
        await set_force_sub_channels(channels)


async def get_refund_eligible_seconds() -> int:
    """Admin panel orqali bazaga saqlangan pul-qaytarish kutish vaqti (soniyada)
    bo'lsa o'shani, aks holda standart REFUND_ELIGIBLE_SECONDS qiymatini qaytaradi."""
    raw = await get_setting(SETTINGS_KEY_REFUND_SECONDS, default=str(REFUND_ELIGIBLE_SECONDS))
    try:
        return int(raw)
    except (TypeError, ValueError):
        return REFUND_ELIGIBLE_SECONDS


async def get_extra_admin_ids() -> list:
    """Bootstrap ADMIN_IDS (.env)ga qo'shimcha ravishda, admin panel orqali
    qo'shilgan adminlar ro'yxatini bazadan o'qiydi."""
    raw = await get_setting(SETTINGS_KEY_EXTRA_ADMINS, default="")
    return [int(x) for x in raw.split(",") if x.strip().isdigit()]


async def add_extra_admin(user_id: int) -> bool:
    """Ro'yxatga qo'shadi. Qaytariladi: True — qo'shildi,
    False — allaqachon ro'yxatda bor edi."""
    ids = await get_extra_admin_ids()
    if user_id in ids:
        return False
    ids.append(user_id)
    await set_setting(SETTINGS_KEY_EXTRA_ADMINS, ",".join(str(x) for x in ids))
    return True


async def remove_extra_admin(user_id: int):
    ids = await get_extra_admin_ids()
    if user_id in ids:
        ids.remove(user_id)
        await set_setting(SETTINGS_KEY_EXTRA_ADMINS, ",".join(str(x) for x in ids))


async def notify_channel(bot, text: str, order_type: str = ""):
    """Raqam / Stars / Premium sotib olinganda kanalga xabar yuboradi.
    Kanal sozlanmagan bo'lsa — hech narsa qilmaydi. Kanalga yuborish
    muvaffaqiyatsiz bo'lsa (masalan, bot hali admin qilib qo'shilmagan bo'lsa),
    xatolik foydalanuvchiga ta'sir qilmasligi uchun jimgina e'tiborsiz qoldiriladi.
    `order_type` sarlavhada qaysi turdagi buyurtma ekanini ko'rsatadi
    ("number"/"stars"/"premium") — aks holda xabar faqat umumiy "BUYURTMA"
    deb chiqib, o'qiganlar bu Raqammi, Starsmi yoki Premiummi bilolmas edi.
    """
    channel_id = await get_channel_id()
    if not channel_id:
        return
    try:
        username = (await bot.get_me()).username
        dbl = "\u2550" * 18
        thin = "\u2500" * 21
        type_labels = {
            "number": "\U0001F4F1 Raqam",
            "stars": "\u2B50 Stars",
            "premium": "\U0001F48E Premium",
            "uc": "\U0001F3AE UC",
        }
        type_label = type_labels.get(order_type, "")
        header = f"     \U0001F195 BUYURTMA \u2014 {type_label}" if type_label else "     \U0001F195 BUYURTMA"
        full_text = (
            f"\u2554{dbl}\u2557\n"
            f"{header}\n"
            f"\u255A{dbl}\u255D\n\n"
            f"{text}\n\n"
            f"\u256D{thin}\u256E\n"
            "\U0001F916 Buyurtma manzili:\n"
            f"\U0001F449 @{username}\n"
            f"\u2570{thin}\u256F\n\n"
            "\u2705 Tezkor \u2022 Qulay \u2022 Ishonchli"
        )
        await bot.send_message(channel_id, full_text)
    except Exception:
        pass


_SUB_CACHE_TTL_SECONDS = 600  # 10 daqiqa — kamida 5-10 daqiqa keshlash tavsiyasiga ko'ra
_sub_cache: Dict[int, tuple] = {}  # user_id -> (a'zo_mi: bool, tekshirilgan_vaqt: float)


async def is_subscribed(bot, user_id: int, force_refresh: bool = False) -> bool:
    """Barcha majburiy obuna kanallariga foydalanuvchi a'zo-yo'qligini
    tekshiradi — a'zo hisoblanishi uchun RO'YXATDAGI HAMMASIGA a'zo bo'lishi
    kerak. Ro'yxat bo'sh bo'lsa — tekshirilmaydi (True qaytadi).
    Bot biror kanalga admin qilib qo'shilmagan yoki boshqa sabab bilan
    tekshira olmasa — botni butunlay to'xtatib qo'ymaslik uchun xavfsiz
    tomonga (o'sha kanal bo'yicha "a'zo") og'ib ketiladi.

    Natija _SUB_CACHE_TTL_SECONDS davomida xotirada keshlanadi: aks holda
    har bir xabar/tugma bosishda Telegram'ga get_chat_member so'rovi ketib,
    ko'p faol foydalanuvchida FloodWait (429) xavfini tug'diradi. "\u2705 A'zo
    bo'ldim" tugmasi force_refresh=True bilan chaqiradi, shu uchun kesh hali
    eskirmagan bo'lsa ham darhol jonli holatni ko'rsatadi.
    """
    channels = await get_force_sub_channels()
    if not channels:
        return True

    now = time.monotonic()
    if not force_refresh:
        cached = _sub_cache.get(user_id)
        if cached is not None and now - cached[1] < _SUB_CACHE_TTL_SECONDS:
            return cached[0]

    result = True
    for item in channels:
        channel = (item or {}).get("channel")
        if not channel:
            continue
        try:
            member = await bot.get_chat_member(channel, user_id)
            if member.status in _NOT_SUBSCRIBED_STATUSES:
                result = False
                break
        except TelegramBadRequest:
            continue
        except Exception:
            continue

    _sub_cache[user_id] = (result, now)
    return result


async def send_force_sub_prompt(bot, chat_id: int):
    channels = await get_force_sub_channels()
    try:
        await bot.send_message(
            chat_id, FORCE_SUB_PROMPT,
            reply_markup=force_sub_menu(channels),
        )
    except Exception:
        # Havola bilan yuborish muvaffaqiyatsiz bo'lsa (masalan, kanal
        # o'chirilgan/noto'g'ri bo'lsa ham) — kamida "A'zo bo'ldim"
        # tugmasi bilan urinib ko'ramiz, aks holda foydalanuvchi hech
        # qanday xabar olmay, botni "ishlamayapti" deb o'ylab qoladi.
        try:
            await bot.send_message(chat_id, FORCE_SUB_PROMPT, reply_markup=force_sub_menu([]))
        except Exception:
            pass


@router_common.callback_query(F.data == "forcesub:check")
async def force_sub_check(callback: CallbackQuery, bot):
    if await is_subscribed(bot, callback.from_user.id, force_refresh=True):
        # MUHIM: bu yerda ham ensure_user() chaqirilishi shart — aks holda
        # majburiy obunadan o'tgan, lekin hali /start bosmagan foydalanuvchi
        # bazada umuman ko'rinmay qoladi (admin unga balans qo'sholmaydi,
        # statistikada hisoblanmaydi, broadcast unga yetib bormaydi).
        await ensure_user(callback.from_user.id, callback.from_user.username, callback.from_user.full_name)
        try:
            await callback.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        await callback.message.answer(FORCE_SUB_OK, reply_markup=main_menu())
        await callback.answer()
    else:
        await callback.answer(FORCE_SUB_STILL_NOT, show_alert=True)


@router_common.callback_query(F.data == "home")
async def go_home(callback: CallbackQuery, state: FSMContext):
    """"Bekor qilish"dan farqli o'laroq (bu "harakat to'xtatildi" deb
    tuyuladi), bu tugma neytral — foydalanuvchi shunchaki bosh menyuga
    qaytmoqchi bo'lganda ishlatiladi.

    MUHIM: pastdagi katta menyu (reply-klaviatura) faqat YANGI xabar bilan
    qaytariladi — Telegram uni mavjud xabarni tahrirlash orqali ulashga
    ruxsat bermaydi. Shuning uchun eski inline xabarning faqat tugmalari
    tozalanadi (matni o'zgarmay qoladi — xuddi clear_stale_flow_message()
    kabi), katta menyu esa alohida, qisqa xabar bilan qaytadan ko'rsatiladi."""
    await state.clear()
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        try:
            await callback.message.delete()
        except Exception:
            pass
    await callback.message.answer("\U0001F3E0 Bosh menyu.", reply_markup=main_menu())
    await callback.answer()


@router_common.callback_query(F.data == "cancel")
async def cancel_any(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    # Yangi xabar bilan "uzaytirish" o'rniga, eski so'rovning o'zini tahrirlab
    # tugmalarini olib tashlaymiz — shunda suhbatda ortiqcha xabarlar
    # to'planib qolmaydi. Tahrirlab bo'lmasa (masalan, rasm bilan yuborilgan
    # yoki juda eski xabar), o'chirib, faqat o'shanda yangi xabar yuboramiz.
    # MUHIM: edit_text'da reply_markup ko'rsatilmasa, Telegram ESKI
    # tugmalarni saqlab qoladi (o'chirmaydi) — shuning uchun bo'sh
    # InlineKeyboardMarkup ANIQ berilishi shart, aks holda "Bekor qilindi"
    # yozuvi ostida eski (endi ishlamaydigan) tugmalar osilib qolaveradi.
    #
    # Pastdagi katta menyu (reply-klaviatura) ham xuddi go_home()'dagidek —
    # faqat YANGI xabar bilan qaytariladi, shuning uchun har doim (tahrirlash
    # muvaffaqiyatli bo'lsa ham) alohida xabar bilan qayta yuboriladi.
    try:
        await callback.message.edit_text("\u274C Bekor qilindi.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[]))
    except Exception:
        try:
            await callback.message.delete()
        except Exception:
            pass
    await callback.message.answer("\U0001F3E0 Asosiy menyu.", reply_markup=main_menu())
    await callback.answer()


async def hide_main_menu(message: Message) -> None:
    """Pastdagi doimiy menyuni (asosiy reply-klaviatura) yashiradi, lekin
    suhbatda ortiqcha xabar qoldirmaydi. Telegram'da reply-klaviaturani olib
    tashlash faqat YANGI xabar bilan mumkin (mavjud xabarni tahrirlab emas)
    — shuning uchun ko'rinmas belgili "texnik" xabar yuboriladi va klaviatura
    yashiringandan so'ng shu zahoti o'chiriladi; xabarning o'zi ekranda
    umuman ko'rinmaydi.

    Ko'p bosqichli xarid oqimlariga kirishda (Balans, Raqam/Stars/Premium
    sotib olish, Yordam, Admin panel) chaqiriladi — shundan keyingi barcha
    qadamlar inline tugmalar bilan davom etadi, "\U0001F3E0 Bosh sahifa"
    yoki "\u274C Bekor qilish" bosilgandagina katta menyu qaytadan chiqadi
    (qarang: go_home, cancel_any)."""
    try:
        sent = await message.answer("\u2063", reply_markup=ReplyKeyboardRemove())
        await sent.delete()
    except Exception:
        pass


# ==============================================================
# START HANDLER (handler_start)
# ==============================================================
router_start = Router(name="start")


@router_start.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext, command: CommandObject, bot):
    await state.clear()

    referred_by = None
    payload = (command.args or "").strip()
    if payload.startswith("ref"):
        ref_id_str = payload[3:]
        if ref_id_str.isdigit():
            candidate = int(ref_id_str)
            if candidate != message.from_user.id:
                referred_by = candidate

    # MUHIM: ensure_user() (demak — referal ID'ni saqlash) doim, majburiy
    # obuna holatidan qat'i nazar, shu yerda, ENG BIRINCHI amalga oshiriladi.
    # ForceSubMiddleware endi /start xabarini shu handlerga yetkazishdan
    # oldin bloklamaydi (aks holda referal payload — Telegram uni qayta
    # yubormagani uchun — butunlay yo'qolib qolar edi). Faqat XUSH KELIBSIZ
    # menyusi obunaga bog'liq — u pastda, is_subscribed tekshiruvidan keyin.
    await ensure_user(message.from_user.id, message.from_user.username, message.from_user.full_name, referred_by=referred_by)

    if not await is_subscribed(bot, message.from_user.id):
        await send_force_sub_prompt(bot, message.chat.id)
        return

    await message.answer(WELCOME, reply_markup=main_menu())


ASK_HELP_MESSAGE = (
    "\U0001F195 Savolingiz yoki muammoingizni yozing \u2014 adminlarga yuboramiz.\n\n"
    "Iloji boricha batafsil yozing (masalan, buyurtma raqami yoki skrinshot bilan)."
)
HELP_SENT_TO_USER = "\u2705 Xabaringiz adminlarga yuborildi. Tez orada javob berishadi."
HELP_INTRO = "\U0001F195 Yordam\n\nQuyidagilardan birini tanlang:"
FAQ_INTRO = "\U0001F4DA Ko'p beriladigan savollar:"


def help_forward_text(user, text: str) -> str:
    username_part = f"@{user.username}" if user.username else "\u2014 (username yo'q)"
    return (
        f"\U0001F195 Yordam so'rovi\n\n"
        f"\U0001F464 {user.full_name} ({username_part})\n"
        f"\U0001F194 ID: {user.id}\n\n"
        f"\U0001F4AC Xabar:\n{text}\n\n"
        f"\u21A9\uFE0F Javob berish uchun shu xabarga Reply qiling \u2014 yozganingiz "
        f"avtomatik ravishda foydalanuvchiga yetkaziladi."
    )


def help_menu() -> InlineKeyboardMarkup:
    rows = [[
        InlineKeyboardButton(text="\U0001F4DA Savollar", callback_data="help:faq", style=STYLE_PRIMARY),
        InlineKeyboardButton(text="\U0001F4AC Adminga yozish", callback_data="help:admin", style=STYLE_PRIMARY),
    ]]
    rows.extend(nav_rows())
    return _kb(rows)


def faq_menu() -> InlineKeyboardMarkup:
    """Savollar ro'yxati: qisqa yorliqlar, ikki ustunda (to'liq savol javob xabarining boshida ko'rinadi)."""
    def question(text: str, key: str) -> InlineKeyboardButton:
        return InlineKeyboardButton(text=text, callback_data=f"faq:{key}", style=STYLE_PRIMARY)

    return _kb([
        [question("\U0001F4F1 Raqam olish", "buy_number"), question("\u2B50 Stars / Premium", "buy_stars")],
        [question("\U0001F3AE PUBG UC", "buy_uc"), question("\U0001F4B0 To'lov", "payment")],
        [question("\u23F3 Kod vaqti", "code_time"), question("\u274C Kod kelmasa", "code_missing")],
        [InlineKeyboardButton(text=BTN_BACK, callback_data="help:menu", style=STYLE_PRIMARY)],
    ])


def faq_answer_menu() -> InlineKeyboardMarkup:
    return _kb(nav_rows("help:faq"))


# Statik FAQ javoblari. "code_missing" bu yerda YO'Q — u refund kutish
# vaqtiga (admin sozlamasi, o'zgarishi mumkin) bog'liq bo'lgani uchun
# faq_code_missing_text() orqali har safar joriy qiymatdan dinamik tuziladi.
FAQ_ANSWERS = {
    "buy_number": (
        "\U0001F4F1 Qanday raqam sotib olaman?\n\n"
        f"1\uFE0F\u20E3 Asosiy menyudan \u00ab{BTN_NUMBER}\u00bb tugmasini bosing.\n"
        "2\uFE0F\u20E3 Raqam turini va davlatni tanlang.\n"
        "3\uFE0F\u20E3 Ko'rsatilgan narxni tasdiqlang.\n"
        "4\uFE0F\u20E3 SMS kodi odatda o'zi avtomatik keladi \u2014 kelmasa, "
        "\U0001F504 \u00abKodni tekshirish\u00bb tugmasini bosing."
    ),
    "buy_stars": (
        "\u2B50 Stars / \U0001F48E Premium qanday sotib olaman?\n\n"
        f"1\uFE0F\u20E3 Asosiy menyudan \u00ab{BTN_STARS}\u00bb yoki \u00ab{BTN_PREMIUM}\u00bb ni tanlang.\n"
        "2\uFE0F\u20E3 Kimga sotib olayotganingizni (@username) kiriting.\n"
        "3\uFE0F\u20E3 Miqdorni (Stars) yoki muddatni (Premium) tanlang.\n"
        "4\uFE0F\u20E3 Narxni tasdiqlang \u2014 buyurtma darhol yuboriladi."
    ),
    "buy_uc": (
        "\U0001F3AE PUBG UC qanday sotib olaman?\n\n"
        f"1\uFE0F\u20E3 Asosiy menyudan \u00ab{BTN_UC}\u00bb tugmasini bosing.\n"
        "2\uFE0F\u20E3 Tarifni tanlang.\n"
        "3\uFE0F\u20E3 PUBG Mobile ID raqamingizni kiriting (faqat raqam, 6\u201312 xona) \u2014 ID ni "
        "diqqat bilan tekshiring, noto'g'ri ID ga yuborilgan UC qaytarilmaydi.\n"
        "4\uFE0F\u20E3 Narxni tasdiqlang \u2014 UC avtomatik yuboriladi; biroz kechiksa, "
        "tayyor bo'lganda o'zimiz xabar beramiz."
    ),
    "payment": (
        "\U0001F4B0 To'lov qanday qilinadi?\n\n"
        f"\u00ab{BTN_TOPUP}\u00bb tugmasini bosing, summani kiriting (kamida "
        f"{fmt_money(MIN_TOPUP)} so'm), ko'rsatilgan karta raqamiga o'tkazing "
        "va to'lov chekining skrinshotini yuboring. Admin tasdiqlagach, "
        "balansingiz avtomatik to'ldiriladi."
    ),
    "code_time": (
        "\u23F3 Kod qancha vaqtda keladi?\n\n"
        "Odatda bir necha soniya-daqiqa ichida, avtomatik keladi. Agar "
        "kechiksa, \U0001F504 \u00abKodni tekshirish\u00bb tugmasi orqali "
        "istalgan vaqt qo'lda tekshirishingiz mumkin."
    ),
}


async def faq_code_missing_text() -> str:
    wait_min = max(1, (await get_refund_eligible_seconds()) // 60)
    return (
        "\u274C Kod kelmasa nima qilaman?\n\n"
        "Kuting yoki \U0001F504 \u00abKodni tekshirish\u00bb orqali qayta "
        f"tekshiring. {wait_min} daqiqadan keyin ham kelmasa, shu buyurtma "
        "ichidagi \U0001F4B8 \u00abPulni qaytarish\u00bb tugmasi orqali "
        "pulingizni qaytarib olishingiz mumkin."
    )


@router_start.message(F.text.in_(MENU_HELP))
async def help_start(message: Message, state: FSMContext):
    await state.clear()
    await hide_main_menu(message)
    await message.answer(HELP_INTRO, reply_markup=help_menu())


@router_start.callback_query(F.data == "help:menu")
async def help_show_menu(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text(HELP_INTRO, reply_markup=help_menu())
    await callback.answer()


@router_start.callback_query(F.data == "help:faq")
async def help_show_faq(callback: CallbackQuery):
    await callback.message.edit_text(FAQ_INTRO, reply_markup=faq_menu())
    await callback.answer()


@router_start.callback_query(F.data.startswith("faq:"))
async def faq_show_answer(callback: CallbackQuery):
    key = callback.data.split(":", 1)[1]
    text = await faq_code_missing_text() if key == "code_missing" else FAQ_ANSWERS.get(key)
    if not text:
        await callback.answer()
        return
    await callback.message.edit_text(text, reply_markup=faq_answer_menu())
    await callback.answer()


@router_start.callback_query(F.data == "help:admin")
async def help_contact_admin(callback: CallbackQuery, state: FSMContext):
    await state.set_state(HelpRequest.message)
    await callback.message.edit_text(ASK_HELP_MESSAGE, reply_markup=cancel_inline())
    await callback.answer()


@router_start.message(HelpRequest.message, is_free_text)
async def help_receive(message: Message, state: FSMContext, bot):
    await state.clear()
    admin_ids = set(ADMIN_IDS) | set(await get_extra_admin_ids())
    text = help_forward_text(message.from_user, message.text)
    for admin_id in admin_ids:
        try:
            sent = await bot.send_message(admin_id, text)
            await save_support_thread(admin_id, sent.message_id, message.from_user.id)
        except Exception:
            pass
    await message.answer(HELP_SENT_TO_USER, reply_markup=main_menu())


@router_start.message(HelpRequest.message, F.photo | F.document | F.video | F.voice | F.audio | F.video_note | F.animation)
async def help_receive_media(message: Message, state: FSMContext, bot):
    """Yordam so'rovida skrinshot/fayl yuborilganda: adminlarga sarlavha (kim yuborgani) va
    medianing o'zi nusxalanadi. Adminning Reply'i ikkala xabarga ham ishlaydi."""
    await state.clear()
    admin_ids = set(ADMIN_IDS) | set(await get_extra_admin_ids())
    head_text = help_forward_text(message.from_user, "\U0001F4CE Rasm/fayl yuborildi (pastdagi xabarda).")
    for admin_id in admin_ids:
        try:
            head = await bot.send_message(admin_id, head_text)
            await save_support_thread(admin_id, head.message_id, message.from_user.id)
            copied = await bot.copy_message(
                chat_id=admin_id, from_chat_id=message.chat.id, message_id=message.message_id,
                reply_to_message_id=head.message_id,
            )
            await save_support_thread(admin_id, copied.message_id, message.from_user.id)
        except Exception:
            pass
    await message.answer(HELP_SENT_TO_USER, reply_markup=main_menu())


async def support_reply_filter(message: Message):
    """Admin biror Yordam-so'rovi xabariga Reply qilganda ishga tushadi.
    Filter False qaytarsa, aiogram avtomatik keyingi handlerga o'tadi —
    shuning uchun bu boshqa admin oqimlariga (broadcast va h.k.) xalaqit
    bermaydi."""
    if not message.reply_to_message:
        return False
    if not await _is_admin(message.from_user.id):
        return False
    target_user_id = await get_support_thread(message.chat.id, message.reply_to_message.message_id)
    if not target_user_id:
        return False
    return {"support_user_id": target_user_id}


@router_admin.message(support_reply_filter)
async def admin_support_reply(message: Message, bot, support_user_id: int):
    """Adminning javobini (matn, rasm, ovozli xabar — istalgan turi)
    o'zgarishsiz foydalanuvchiga yetkazadi."""
    try:
        await bot.copy_message(
            chat_id=support_user_id,
            from_chat_id=message.chat.id,
            message_id=message.message_id,
        )
    except Exception:
        await message.reply("\u274C Foydalanuvchiga yetkazib bo'lmadi \u2014 balki botni bloklagan.")
        return
    await message.reply("\u2705 Foydalanuvchiga yuborildi.")


# ==============================================================
# BALANS HANDLER (handler_balance)
# ==============================================================
router_balance = Router(name="balance")


@router_balance.message(F.text.in_(MENU_BALANCE))
async def show_balance(message: Message, state: FSMContext):
    await state.clear()
    balance = await get_balance(message.from_user.id)
    await hide_main_menu(message)
    await message.answer(balance_text(balance), reply_markup=balance_menu())


@router_balance.callback_query(F.data == "topup:start")
async def topup_start(callback: CallbackQuery, state: FSMContext, bot):
    await clear_stale_flow_message(bot, callback.from_user.id)
    await state.set_state(TopUp.amount)
    await callback.message.edit_text(TOPUP_ASK_AMOUNT, reply_markup=cancel_inline())
    await callback.answer()


@router_balance.callback_query(F.data == "referral:info")
async def referral_info(callback: CallbackQuery, bot):
    me = await bot.get_me()
    link = f"https://t.me/{me.username}?start=ref{callback.from_user.id}"
    stats = await get_referral_stats(callback.from_user.id)
    await callback.message.answer(referral_text(link, stats["invited"], stats["earned"]))
    await callback.answer()


@router_balance.message(TopUp.amount, is_free_text)
async def topup_amount(message: Message, state: FSMContext):
    text = message.text.strip().replace(" ", "")
    if not text.isdigit() or int(text) <= 0:
        await message.answer(TOPUP_NOT_A_NUMBER)
        return

    amount = int(text)
    if amount < MIN_TOPUP:
        await message.answer(f"\u274C Eng kam to'ldirish summasi: {fmt_money(MIN_TOPUP)} so'm. Qayta kiriting.")
        return

    # Avto-to'lov (karta) tayyor bo'lsa — unikal summa va karta ko'rsatiladi; aks holda eski usul (chek).
    if await _autopay_offer(message, state, amount):
        return

    await state.update_data(amount=amount)
    await state.set_state(TopUp.photo)
    card_number, card_holder = await get_card_info()
    await message.answer(topup_instructions(amount, card_number, card_holder), reply_markup=_card_copy_menu(card_number))


@router_balance.message(TopUp.photo, F.photo)
async def topup_photo(message: Message, state: FSMContext, bot):
    data = await state.get_data()
    amount = data.get("amount", 0)
    photo_file_id = message.photo[-1].file_id

    topup_id = await create_topup(message.from_user.id, amount, photo_file_id)
    await state.clear()

    other_pending = await count_other_pending_topups(message.from_user.id, topup_id)

    caption = (
        f"\U0001F195 Balans to'ldirish so'rovi\n"
        f"\U0001F464 {message.from_user.full_name} (ID: {message.from_user.id})\n"
        f"\U0001F4B0 Summa: {fmt_money(amount)} so'm\n"
        f"\U0001F522 So'rov ID: {topup_id}"
    )
    if other_pending:
        caption += duplicate_topup_warning(other_pending)

    for admin_id in (set(ADMIN_IDS) | set(await get_extra_admin_ids())):
        try:
            await bot.send_photo(
                admin_id, photo_file_id, caption=caption,
                reply_markup=topup_review_menu(topup_id),
            )
        except Exception:
            pass

    await message.answer(TOPUP_SENT_TO_ADMIN, reply_markup=main_menu())


@router_balance.message(TopUp.photo, is_free_text)
async def topup_photo_missing(message: Message):
    await message.answer(TOPUP_SEND_PHOTO)


# ==============================================================
# RAQAM HANDLER (handler_numbers)
# ==============================================================
router_numbers = Router(name="numbers")
# SmmUpper klienti — shu yagona nusxa Raqam/Stars/Premium/Buyurtmalar/Admin
# bo'limlarining barchasida ishlatiladi (stateless, shuning uchun bo'lishish xavfsiz).
client = SmmUpperClient()

POLL_ATTEMPTS = 12
POLL_DELAY_SECONDS = 5

# asyncio faqat "kuchsiz" (weak) havola saqlaydi — agar boshqa hech kim
# task'ga havola ushlab turmasa, u tugamasdan turib "chiqindi" deb yig'ib
# tashlanishi mumkin. Shuning uchun fon tekshiruvlari shu to'plamda ushlab
# turiladi (tugagach avtomatik chiqarib tashlanadi).
_background_tasks: set = set()


@router_numbers.message(F.text.in_(MENU_NUMBER))
async def start_number_flow(message: Message, state: FSMContext, bot):
    await state.clear()
    await clear_stale_flow_message(bot, message.from_user.id)
    await hide_main_menu(message)
    await message.answer(NUMBER_PURCHASE_WARNING, reply_markup=number_warning_menu())


@router_numbers.callback_query(F.data == "numwarn:confirm")
async def number_warning_confirmed(callback: CallbackQuery):
    await callback.message.edit_text(NUMBER_TYPE_TEXT, reply_markup=number_type_menu())
    await callback.answer()


async def _fetch_countries_with_fallback(primary_server: int, fallback_server: Optional[int]):
    try:
        data = await client.available_countries(primary_server)
        countries = data.get("countries") or {}
        if countries:
            return primary_server, countries
    except _API_ERRORS:
        pass

    if fallback_server:
        try:
            data = await client.available_countries(fallback_server)
            return fallback_server, (data.get("countries") or {})
        except _API_ERRORS:
            pass

    return primary_server, {}


@router_numbers.callback_query(F.data == "numback:type")
async def number_back_to_type(callback: CallbackQuery):
    await callback.message.edit_text(NUMBER_TYPE_TEXT, reply_markup=number_type_menu())
    await callback.answer()


@router_numbers.callback_query(F.data == "numtype:regular")
async def choose_regular(callback: CallbackQuery, state: FSMContext):
    await callback.answer("Davlatlar yuklanmoqda...")
    server, countries = await _fetch_countries_with_fallback(1, 2)
    if not countries:
        await callback.message.edit_text(
            "\u274C Hozircha mavjud davlat yo'q. Birozdan so'ng qayta urinib ko'ring.",
            reply_markup=cancel_inline(back_callback="numback:type"),
        )
        return

    await state.set_state(BuyNumber.choosing_country)
    await state.update_data(server=server, countries=countries)
    percent = await get_markup_percent("number")
    await callback.message.edit_text(PICK_COUNTRY_TEXT, reply_markup=countries_menu(server, countries, markup_percent=percent))


@router_numbers.callback_query(F.data == "numtype:ready")
async def choose_ready(callback: CallbackQuery, state: FSMContext):
    """"Tayyor akkaunt" (server 3): oqim "Oddiy raqam" bilan bir xil (davlat -> tasdiq -> xarid -> kod),
    faqat mahsulot boshqa. Narx ustamasi Raqam bilan bir xil ("number" bo'limi)."""
    await callback.answer("Yuklanmoqda...")
    try:
        data = await client.available_countries(3)
        countries = data.get("countries") or {}
    except _API_ERRORS as e:
        await callback.message.edit_text(f"\u274C Tayyor akkauntlar hozircha mavjud emas: {_err_text(e)}",
                                          reply_markup=cancel_inline(back_callback="numback:type"))
        return

    if not countries:
        await callback.message.edit_text("\u274C Hozircha tayyor akkaunt yo'q.",
                                          reply_markup=cancel_inline(back_callback="numback:type"))
        return

    await state.set_state(BuyNumber.choosing_country)
    await state.update_data(server=3, countries=countries)
    percent = await get_markup_percent("number")
    await callback.message.edit_text(PICK_COUNTRY_TEXT, reply_markup=countries_menu(3, countries, markup_percent=percent))


# ---------- Davlatlar ro'yxatini varaqlash va qidirish ----------

@router_numbers.callback_query(BuyNumber.choosing_country, F.data == "ctynoop")
async def country_page_indicator(callback: CallbackQuery):
    """Sahifa raqami ko'rsatkichi — bosilganda hech narsa qilmaydi,
    faqat Telegram'ning "yuklanmoqda" aylanasini to'xtatadi."""
    await callback.answer()


@router_numbers.callback_query(BuyNumber.choosing_country, F.data.startswith("ctypg:"))
async def country_change_page(callback: CallbackQuery, state: FSMContext):
    _, server_str, page_str = callback.data.split(":")
    data = await state.get_data()
    countries = data.get("countries", {})
    percent = await get_markup_percent("number")
    await callback.message.edit_text(
        PICK_COUNTRY_TEXT,
        reply_markup=countries_menu(int(server_str), countries, page=int(page_str), markup_percent=percent),
    )
    await callback.answer()


@router_numbers.callback_query(BuyNumber.choosing_country, F.data.startswith("ctycpg:"))
async def country_change_cheap_page(callback: CallbackQuery, state: FSMContext):
    """Bitta handler ham 'Arzon davlatlar' tugmasini (page=0 bilan boshlanadi),
    ham shu ro'yxat ichidagi keyingi/oldingi sahifalarni boshqaradi."""
    _, server_str, page_str = callback.data.split(":")
    data = await state.get_data()
    countries = data.get("countries", {})
    percent = await get_markup_percent("number")
    await callback.message.edit_text(
        "\U0001F4C9 Arzon davlatlar (so'mda):",
        reply_markup=countries_menu(int(server_str), countries, page=int(page_str), mode="cheap", markup_percent=percent),
    )
    await callback.answer()


@router_numbers.callback_query(BuyNumber.choosing_country, F.data.startswith("ctytop:"))
async def country_show_top(callback: CallbackQuery, state: FSMContext):
    """Buyurtmalar tarixidan eng ko'p sotib olingan 10 ta davlatni chiqaradi.
    Faqat SHU tur (Oddiy raqam yoki Tayyor akkaunt — server bo'yicha) buyurtmalari sanaladi va faqat hozir
    mavjud davlatlar ko'rsatiladi.
    Eslatma: bu hisoblash faqat shu ustun qo'shilgandan keyingi (yangi)
    buyurtmalarni sanaydi — eski buyurtmalarda davlat alohida saqlanmagan
    edi, shuning uchun bot yangilangandan keyingi xaridlar asosida
    to'planib boradi."""
    server = int(callback.data.split(":")[1])
    data = await state.get_data()
    countries = data.get("countries", {})
    # Ko'proq olib, mavjudlarini saralaymiz: eng ommabop davlat hozir tugab qolgan bo'lsa ham ro'yxat to'lib turadi.
    top = await top_countries(30, ready=(server == 3))
    matched = [(code, cnt) for code, cnt in top if code in countries][:10]

    back_and_cancel = nav_rows(f"ctyback:{server}")

    if not matched:
        await callback.message.edit_text(
            "\U0001F3C6 Hozircha statistika yo'q. Birinchi xaridlardan so'ng "
            "shu yerda eng ko'p sotib olingan davlatlar chiqadi.",
            reply_markup=_kb(back_and_cancel),
        )
        await callback.answer()
        return

    rows = []
    row = []
    percent = await get_markup_percent("number")
    for code, _cnt in matched:
        info = countries[code]
        row.append(InlineKeyboardButton(text=_country_button_label(code, info, percent), callback_data=f"cty:{server}:{code}", style=STYLE_PRIMARY))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.extend(back_and_cancel)

    await callback.message.edit_text(
        "\U0001F3C6 Eng ko'p sotib olingan davlatlar (so'mda):",
        reply_markup=_kb(rows),
    )
    await callback.answer()


@router_numbers.callback_query(BuyNumber.choosing_country, F.data.startswith("ctyspg:"))
async def country_change_search_page(callback: CallbackQuery, state: FSMContext):
    _, server_str, page_str = callback.data.split(":")
    data = await state.get_data()
    results = data.get("search_results", {})
    percent = await get_markup_percent("number")
    await callback.message.edit_text(
        "\U0001F50D Qidiruv natijalari:",
        reply_markup=countries_menu(int(server_str), results, page=int(page_str), mode="search", markup_percent=percent),
    )
    await callback.answer()


@router_numbers.callback_query(BuyNumber.choosing_country, F.data.startswith("ctysearch:"))
async def country_search_prompt(callback: CallbackQuery, state: FSMContext):
    await state.set_state(BuyNumber.searching_country)
    await callback.message.edit_text(
        "Davlat nomini yozing (masalan: turkiya):",
        reply_markup=cancel_inline(),
    )
    await callback.answer()


@router_numbers.callback_query(F.data.startswith("ctyback:"))
async def country_search_exit(callback: CallbackQuery, state: FSMContext):
    """Qidiruv natijalaridan to'liq ro'yxatga qaytish. Holatni har doim
    (choosing_country'dan ham, searching_country'dan ham) qabul qiladi —
    "hech narsa topilmadi" xabaridan keyin ham shu tugma ishlashi kerak."""
    server = int(callback.data.split(":")[1])
    data = await state.get_data()
    countries = data.get("countries", {})
    await state.set_state(BuyNumber.choosing_country)
    percent = await get_markup_percent("number")
    await callback.message.edit_text(PICK_COUNTRY_TEXT, reply_markup=countries_menu(server, countries, markup_percent=percent))
    await callback.answer()


@router_numbers.message(BuyNumber.searching_country, is_free_text)
async def country_search_run(message: Message, state: FSMContext):
    data = await state.get_data()
    server = data.get("server")
    countries = data.get("countries", {})
    query = _normalize_query(message.text)

    results = {
        code: info for code, info in countries.items()
        if query in _normalize_query(country_display_name(code)) or query in code.lower()
    }

    if not results:
        await message.answer(
            "Hech narsa topilmadi. Boshqa nom bilan qayta urinib ko'ring, "
            "yoki ro'yxatga qayting.",
            reply_markup=_kb(nav_rows(f"ctyback:{server}")),
        )
        return  # holat searching_country'da qoladi — darhol qayta yozish mumkin

    await state.update_data(search_results=results)
    await state.set_state(BuyNumber.choosing_country)
    percent = await get_markup_percent("number")
    await message.answer(
        f"\U0001F50D Qidiruv natijalari ({len(results)} ta):",
        reply_markup=countries_menu(server, results, mode="search", markup_percent=percent),
    )


def _number_confirm_text(server: int, country: str, price: int, balance: int) -> str:
    """Raqam/akkaunt xaridini tasdiqlash ekrani matni (davlat tanlagandan keyin ham, "Qayta buyurtma"da ham)."""
    kind_line = "\U0001F510 Tur: Tayyor akkaunt\n" if server == 3 else ""
    return (
        f"{kind_line}"
        f"\U0001F30D Davlat: {country_flag(country)} {country_display_name(country)}\n"
        f"\U0001F4B5 Narx: {fmt_money(price)} so'm\n"
        f"\U0001F4B0 Balansingiz: {fmt_money(balance)} so'm"
    )


def _number_confirm_question(server: int) -> str:
    what = "akkauntni" if server == 3 else "raqamni"
    return f"\U0001F4F1 Ushbu {what} sotib olishni tasdiqlaysizmi?"


@router_numbers.callback_query(BuyNumber.choosing_country, F.data.startswith("cty:"))
async def choose_country(callback: CallbackQuery, state: FSMContext):
    _, server_str, country = callback.data.split(":")
    server = int(server_str)

    data = await state.get_data()
    countries = data.get("countries", {})
    info = countries.get(country) or {}
    price = await with_markup(info.get("price"), "number")

    # Himoya: tugma eskirgan (boshqa ro'yxatdan qolgan), davlat endi ro'yxatda yo'q yoki provayder narx
    # bermagan bo'lsa — 0 so'mlik ("tekin") xaridga YO'L QO'YILMAYDI (avval bunday holatda narx 0 bo'lib qolardi).
    if price <= 0 or data.get("server") != server:
        await callback.answer("Bu davlat hozir mavjud emas \u2014 ro'yxatni yangilab, qayta tanlang.", show_alert=True)
        return

    balance = await get_balance(callback.from_user.id)
    await state.update_data(country=country, price=price)
    await state.set_state(BuyNumber.confirming)

    text = _number_confirm_text(server, country, price, balance)

    if balance < price:
        sent = await callback.message.edit_text(text + "\n\n" + insufficient_balance(price, balance),
                                                  reply_markup=balance_menu())
        await track_flow_msg(sent)
        await state.clear()
        await callback.answer()
        return

    await callback.message.edit_text(
        text + "\n\n" + _number_confirm_question(server),
        reply_markup=confirm_menu("buynum:confirm", back_callback="numback:country"),
    )
    await callback.answer()


@router_numbers.callback_query(BuyNumber.confirming, F.data == "numback:country")
async def number_back_to_country(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    server = data.get("server")
    countries = data.get("countries")
    if not server or not countries:
        await callback.answer("Ro'yxatga qaytib bo'lmadi, qaytadan tanlang.", show_alert=True)
        return
    await state.set_state(BuyNumber.choosing_country)
    percent = await get_markup_percent("number")
    await callback.message.edit_text(PICK_COUNTRY_TEXT, reply_markup=countries_menu(server, countries, markup_percent=percent))
    await callback.answer()


@router_numbers.callback_query(BuyNumber.confirming, F.data == "buynum:confirm")
@single_flight_purchase
async def confirm_number(callback: CallbackQuery, state: FSMContext, bot):
    data = await state.get_data()
    server = data["server"]
    country = data["country"]
    est_price = data["price"]

    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

    # Balansni SHU YERDA, bitta atomik amal bilan tekshirib-va-yechib qo'yamiz —
    # SmmUpper'ga ketadigan (sekin) so'rovdan OLDIN. Aks holda ikkita xaridni bir
    # vaqtda tasdiqlash orqali balansni race condition bilan minusga tushirish
    # mumkin bo'lardi.
    if not await try_deduct_balance(callback.from_user.id, est_price):
        balance = await get_balance(callback.from_user.id)
        sent = await callback.message.answer(insufficient_balance(est_price, balance), reply_markup=balance_menu())
        await track_flow_msg(sent)
        await state.clear()
        await callback.answer()
        return

    await _safe_answer(callback, "Amalga oshirilmoqda...")

    try:
        result = await _call_idempotent(client.get_number, server, country)
    except PurchaseUnknownError as e:
        await _refund_unknown_purchase(bot, callback, state, est_price, "Raqam", e)
        return
    except SmmUpperError as e:
        await change_balance(callback.from_user.id, est_price)
        await callback.message.answer(
            f"\u274C Xatolik: {e.message}\n\U0001F4B0 Pulingiz balansga qaytarildi.",
            reply_markup=main_menu(),
        )
        await state.clear()
        return

    charged_price = await final_price(callback.from_user.id, result, est_price, "number")

    ref = result.get("hash_code") or result.get("number") or (
        str(result["id"]) if result.get("id") is not None else None)
    order_pk = await create_order(
        user_id=callback.from_user.id,
        order_type="number",
        ref=ref,
        server=server,
        price=charged_price,
        details=result,
        status="processing",
        country=country,
    )

    number = result.get("number", "?")

    buyer = callback.from_user.full_name
    if callback.from_user.username:
        buyer += f" (@{callback.from_user.username})"
    await notify_channel(bot, channel_number_notice(buyer, country, charged_price, number), order_type="number")

    sent = await callback.message.answer(
        f"\u2705 Raqam muvaffaqiyatli olindi!\n"
        f"\U0001F4F1 Raqam: <code>{html.escape(str(number))}</code>\n"
        f"\U0001F4B5 Narx: {fmt_money(charged_price)} so'm\n"
        f"\u23F3 SMS tasdiqlash kodi kutilmoqda...\n"
        f"Iltimos, biroz kuting.",
        reply_markup=check_code_menu(order_pk, copy_number=number),
        parse_mode="HTML",
    )

    await state.clear()
    task = asyncio.create_task(
        _poll_code(bot, callback.from_user.id, order_pk, server, result, sent.message_id)
    )
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)


async def _try_get_code(server: int, result: dict) -> dict:
    if server == 1:
        return await client.get_code(server, hash_code=result.get("hash_code"))
    elif server == 2:
        return await client.get_code(server, number=result.get("number"))
    else:
        return await client.get_code(server, id=result.get("id"))


async def _clear_buttons(bot, user_id: int, message_id: Optional[int]):
    """Buyurtma allaqachon yakunlanganda "Tekshirish/Qaytarish" tugmalarini
    eski xabardan olib tashlaydi — aks holda foydalanuvchi keraksiz tugmani
    bosishda davom etaverishi mumkin (o'zi xavfli emas, lekin chalkashtiradi).
    """
    if not message_id:
        return
    try:
        await bot.edit_message_reply_markup(chat_id=user_id, message_id=message_id, reply_markup=None)
    except Exception:
        pass


async def _poll_code(bot, user_id: int, order_pk: int, server: int, result: dict,
                      purchase_message_id: Optional[int] = None):
    for _ in range(POLL_ATTEMPTS):
        await asyncio.sleep(POLL_DELAY_SECONDS)
        try:
            data = await _try_get_code(server, result)
        except SmmUpperError:
            continue

        if data.get("success") and data.get("code"):
            code = data.get("code")
            password = data.get("password") or ""
            text = f"\u2705 SMS kodi qabul qilindi!\n\U0001F522 Kod: <code>{html.escape(str(code))}</code>"
            if password:
                text += f"\n\U0001F510 2FA parol: <code>{html.escape(str(password))}</code>"
            await update_order_status(order_pk, "done", {**result, "code": code, "password": password})
            await _clear_buttons(bot, user_id, purchase_message_id)
            try:
                await bot.send_message(user_id, text, parse_mode="HTML", reply_markup=_code_copy_menu(code, password))
            except Exception:
                pass
            return

    try:
        await bot.send_message(
            user_id,
            "\u231B Kod hali kelmadi. Pastdagi tugma orqali istalgan vaqt tekshirishingiz mumkin.",
            reply_markup=check_code_menu(order_pk, copy_number=result.get("number")),
        )
    except Exception:
        pass


async def _check_and_report_number_code(callback: CallbackQuery, order_pk: int, row, clear_markup: bool) -> None:
    """"Raqam" turidagi buyurtma uchun SMS kodni SmmUpper'dan so'raydi va
    natijani foydalanuvchiga ko'rsatadi. numcheck: (xariddan keyingi tugma)
    va ordercheck: (buyurtmalar ro'yxatidagi umumiy tugma) — ikkalasi ham
    shu funksiyadan foydalanadi: "raqam" buyurtmalarida haqiqiy order_id
    yo'q (faqat hash_code/number/id bor), shuning uchun holatni har doim
    getCode orqali (getOrder emas) tekshirish kerak."""
    result = json.loads(row["details"]) if row["details"] else {}
    server = row["server"]

    try:
        data = await _try_get_code(server, result)
    except SmmUpperError as e:
        await callback.answer(f"Xatolik: {e.message}", show_alert=True)
        return

    if data.get("success") and data.get("code"):
        code = data.get("code")
        password = data.get("password") or ""
        await update_order_status(order_pk, "done", {**result, "code": code, "password": password})
        if clear_markup:
            try:
                await callback.message.edit_reply_markup(reply_markup=None)
            except Exception:
                pass
        # Popup alert (show_alert) matnni nusxalashga imkon bermaydi — shuning
        # uchun kodni alohida, <code> bilan formatlangan xabar sifatida
        # yuboramiz, bosib nusxa olish uchun.
        text = f"\u2705 SMS kodi qabul qilindi!\n\U0001F522 Kod: <code>{html.escape(str(code))}</code>"
        if password:
            text += f"\n\U0001F510 2FA parol: <code>{html.escape(str(password))}</code>"
        await callback.answer()
        await callback.message.answer(text, parse_mode="HTML", reply_markup=_code_copy_menu(code, password))
    else:
        await callback.answer("\u23F3 Kod hali kelmagan. Birozdan so'ng qayta tekshiring.", show_alert=True)


@router_numbers.callback_query(F.data.startswith("numcheck:"))
async def manual_check_code(callback: CallbackQuery):
    order_pk = int(callback.data.split(":")[1])
    row = await get_order_row(order_pk)
    if not row or row["user_id"] != callback.from_user.id:
        await callback.answer("Buyurtma topilmadi.", show_alert=True)
        return
    if row["status"] != "processing":
        await callback.answer("Bu buyurtma allaqachon yakunlangan.", show_alert=True)
        return
    await _check_and_report_number_code(callback, order_pk, row, clear_markup=True)


@router_numbers.callback_query(F.data.startswith("numrefund:"))
async def refund_number_order(callback: CallbackQuery, bot):
    order_pk = int(callback.data.split(":")[1])
    row = await get_order_row(order_pk)
    if not row or row["user_id"] != callback.from_user.id:
        await callback.answer("Buyurtma topilmadi.", show_alert=True)
        return
    if row["status"] != "processing":
        await callback.answer("Bu buyurtma allaqachon yakunlangan.", show_alert=True)
        return

    elapsed = int(time.time()) - row["created_at"]
    refund_wait = await get_refund_eligible_seconds()
    if elapsed < refund_wait:
        wait_min = (refund_wait - elapsed) // 60 + 1
        await callback.answer(
            f"\u23F3 Hali erta \u2014 SMS kelishi mumkin. Yana ~{wait_min} daqiqadan so'ng qaytadan urinib ko'ring.",
            show_alert=True,
        )
        return

    refund = await refund_order(order_pk, min_age_seconds=refund_wait)
    if not refund:
        await callback.answer("Pulni qaytarib bo'lmadi \u2014 balki allaqachon yakunlangan.", show_alert=True)
        return

    # SmmUpper Hamkorlik API v2 hujjatida (smmupper.uz/api/v2/docs) raqamni
    # bekor qilish/bo'shatish uchun HECH QANDAY endpoint yo'q — bor-yo'g'i
    # 8 ta amal bor (getBalance...getOrder), ular orasida "cancel" yo'q.
    # Shuning uchun bu yerda providerga hech narsa yuborilmaydi — yuqoridagi
    # balans qaytarish (refund_order) kifoya. Raqamning o'zi provayder
    # tomonida "band" holida qolishi mumkin — buni hal qilishning yagona
    # yo'li ular bilan to'g'ridan-to'g'ri (API'dan tashqari) bog'lanish.
    # Kelajakda ko'plab "band" qolgan raqamlarni birdan hisoblash kerak
    # bo'lib qolsa deb, shu holatlar log'ga yozib qo'yiladi (admin'larga
    # xabar YUBORILMAYDI — bu doimiy, kutilgan holat, xatolik emas).
    logging.info(
        f"Raqam refund qilindi, lekin provayderda bo'shatilmadi (bunday "
        f"endpoint yo'q): order={order_pk} server={row['server']} ref={row['ref']}"
    )

    new_balance = await get_balance(callback.from_user.id)
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await callback.message.answer(
        f"\U0001F4B8 {fmt_money(refund['price'])} so'm balansingizga qaytarildi.\n"
        f"\U0001F4B0 Joriy balans: {fmt_money(new_balance)} so'm",
        reply_markup=main_menu(),
    )
    await callback.answer()


# ==============================================================
# STARS HANDLER (handler_stars)
# ==============================================================
router_stars = Router(name="stars")

USERNAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{4,31}$")
MIN_STARS = 50


@router_stars.message(F.text.in_(MENU_STARS))
async def start_stars_flow(message: Message, state: FSMContext, bot):
    await state.clear()
    await clear_stale_flow_message(bot, message.from_user.id)
    await state.set_state(BuyStars.username)
    await hide_main_menu(message)
    await message.answer(
        "Kimga Stars sotib olamiz? Telegram username kiriting (masalan: durov).",
        reply_markup=cancel_inline(),
    )


@router_stars.callback_query(BuyStars.amount, F.data == "starsback:username")
async def stars_back_to_username(callback: CallbackQuery, state: FSMContext):
    await state.set_state(BuyStars.username)
    await callback.message.edit_text(
        "Kimga Stars sotib olamiz? Telegram username kiriting (masalan: durov).",
        reply_markup=cancel_inline(),
    )
    await callback.answer()


async def _stars_per_unit_text() -> str:
    """Stars uchun narx: SmmUpper'ning har bir Star narxi + "stars" bo'limi ustamasi."""
    prices = await client.get_prices()
    percent = await get_markup_percent("stars")
    per_star = prices["stars"]["price_per_star"] * _markup_multiplier(percent)
    return f"Nechta Stars? (min {MIN_STARS}, yoki summani yozib yuboring)\n\U0001F4B5 Narx: {per_star:.1f} so'm/Star"


@router_stars.callback_query(BuyStars.confirming, F.data == "starsback:amount")
async def stars_back_to_amount(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    username = data.get("username")
    if not username:
        await callback.answer("Orqaga qaytib bo'lmadi, qaytadan boshlang.", show_alert=True)
        return
    try:
        text = await _stars_per_unit_text()
    except _FETCH_ERRORS as e:
        await callback.answer(f"Narxlarni olib bo'lmadi: {_err_text(e)}", show_alert=True)
        return
    await state.set_state(BuyStars.amount)
    await callback.message.edit_text(text, reply_markup=stars_amount_menu())
    await callback.answer()


@router_stars.message(BuyStars.username, is_free_text)
async def stars_username(message: Message, state: FSMContext):
    username = message.text.strip().lstrip("@")
    if not USERNAME_RE.match(username):
        await message.answer("Username noto'g'ri ko'rinadi. Qayta kiriting (masalan: durov).")
        return

    try:
        text = await _stars_per_unit_text()
    except _FETCH_ERRORS as e:
        await message.answer(f"\u274C Narxlarni olib bo'lmadi: {_err_text(e)}")
        return

    await state.update_data(username=username)
    await state.set_state(BuyStars.amount)
    await message.answer(text, reply_markup=stars_amount_menu())


@router_stars.callback_query(BuyStars.amount, F.data.startswith("starsamt:"))
async def stars_amount_choice(callback: CallbackQuery, state: FSMContext):
    value = callback.data.split(":", 1)[1]
    if value == "custom":
        await callback.message.edit_text(f"Nechta Stars kerak? Sonini yozing (min {MIN_STARS}).",
                                          reply_markup=cancel_inline())
        await callback.answer()
        return

    await callback.answer()
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await _process_stars_amount(callback.message, state, int(value), callback.from_user.id)


@router_stars.message(BuyStars.amount, is_free_text)
async def stars_amount_text(message: Message, state: FSMContext):
    text = message.text.strip()
    if not text.isdigit():
        await message.answer("Iltimos, faqat son kiriting.")
        return
    await _process_stars_amount(message, state, int(text), message.from_user.id)


async def _process_stars_amount(message: Message, state: FSMContext, amount: int, user_id: int):
    if amount < MIN_STARS:
        await message.answer(f"Minimal miqdor — {MIN_STARS} ta Stars.")
        return

    try:
        prices = await client.get_prices()
        price_per_star = prices["stars"]["price_per_star"]
    except _FETCH_ERRORS as e:
        await message.answer(f"\u274C Narxlarni olib bo'lmadi: {_err_text(e)}")
        return

    price = await with_markup(price_per_star * amount, "stars")
    balance = await get_balance(user_id)
    data = await state.get_data()
    username = data.get("username")

    await state.update_data(amount=amount, price=price)
    await state.set_state(BuyStars.confirming)

    text = (
        f"\U0001F464 Kimga: @{username}\n"
        f"\u2B50 Miqdor: {amount}\n"
        f"\U0001F4B5 Narx: {fmt_money(price)} so'm\n"
        f"\U0001F4B0 Balansingiz: {fmt_money(balance)} so'm"
    )

    if balance < price:
        sent = await message.answer(text + "\n\n" + insufficient_balance(price, balance),
                                     reply_markup=balance_menu())
        await track_flow_msg(sent)
        await state.clear()
        return

    await message.answer(text + "\n\n\u2B50 Ushbu Starsni sotib olishni tasdiqlaysizmi?", reply_markup=confirm_menu("buystars:confirm", back_callback="starsback:amount"))


@router_stars.callback_query(BuyStars.confirming, F.data == "buystars:confirm")
@single_flight_purchase
async def confirm_stars(callback: CallbackQuery, state: FSMContext, bot):
    data = await state.get_data()
    username = data["username"]
    amount = data["amount"]
    est_price = data["price"]

    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

    if not await try_deduct_balance(callback.from_user.id, est_price):
        balance = await get_balance(callback.from_user.id)
        sent = await callback.message.answer(insufficient_balance(est_price, balance), reply_markup=balance_menu())
        await track_flow_msg(sent)
        await state.clear()
        await callback.answer()
        return

    await _safe_answer(callback, "Amalga oshirilmoqda...")

    try:
        result = await _call_idempotent(client.buy_stars, username, amount)
    except PurchaseUnknownError as e:
        await _refund_unknown_purchase(bot, callback, state, est_price, "Stars", e)
        return
    except SmmUpperError as e:
        await change_balance(callback.from_user.id, est_price)
        await callback.message.answer(
            f"\u274C Xatolik: {e.message}\n\U0001F4B0 Pulingiz balansga qaytarildi.",
            reply_markup=main_menu(),
        )
        await state.clear()
        return

    charged_price = await final_price(callback.from_user.id, result, est_price, "stars")
    order_pk = await create_order(
        user_id=callback.from_user.id,
        order_type="stars",
        ref=result.get("order_id"),
        server=None,
        price=charged_price,
        details=result,
        status="processing",
        target_username=username,
        qty=amount,
    )

    buyer = callback.from_user.full_name
    if callback.from_user.username:
        buyer += f" (@{callback.from_user.username})"
    await notify_channel(bot, channel_stars_notice(buyer, username, amount, charged_price), order_type="stars")

    await callback.message.answer(
        f"\u2705 Buyurtma qabul qilindi!\n"
        f"\U0001F464 @{username}\n"
        f"\u2B50 {amount} Stars\n"
        f"\U0001F4B5 {fmt_money(charged_price)} so'm\n"
        f"\U0001F522 Buyurtma raqami: {result.get('order_id')} (#{order_pk})\n\n"
        f"Holatini «{BTN_ORDERS}» bo'limidan kuzatishingiz mumkin.",
        reply_markup=main_menu(),
    )
    await state.clear()


# ==============================================================
# PREMIUM HANDLER (handler_premium)
# ==============================================================
router_premium = Router(name="premium")


async def _premium_prices_text() -> tuple:
    """Premium narxlari: SmmUpper narxi + "premium" bo'limi ustamasi. (matn, {oy: narx}) qaytaradi."""
    raw_prices = (await client.get_prices())["premium"]
    prices: Dict[int, int] = {}
    for m in (3, 6, 12):
        prices[m] = await with_markup(raw_prices[str(m)]["price"], "premium")
    text = (
        "\U0001F48E Narxlar:\n"
        f"\u2022 3 oy: {fmt_money(prices[3])} so'm\n"
        f"\u2022 6 oy: {fmt_money(prices[6])} so'm\n"
        f"\u2022 12 oy: {fmt_money(prices[12])} so'm\n\n"
        "Necha oylik Premium kerak?"
    )
    return text, prices


@router_premium.message(F.text.in_(MENU_PREMIUM))
async def start_premium_flow(message: Message, state: FSMContext, bot):
    await state.clear()
    await clear_stale_flow_message(bot, message.from_user.id)

    try:
        text, prices = await _premium_prices_text()
    except _FETCH_ERRORS as e:
        await message.answer(f"\u274C Narxlarni olib bo'lmadi: {_err_text(e)}")
        return

    await state.set_state(BuyPremium.months)
    await hide_main_menu(message)
    await message.answer(text, reply_markup=premium_months_menu(prices))


@router_premium.callback_query(BuyPremium.months, F.data.startswith("premmonths:"))
async def premium_months_choice(callback: CallbackQuery, state: FSMContext):
    months = int(callback.data.split(":")[1])
    await state.update_data(months=months)
    await state.set_state(BuyPremium.username)
    await callback.message.edit_text("Kimga? Telegram username kiriting (masalan: durov).",
                                      reply_markup=cancel_inline(back_callback="premback:months"))
    await callback.answer()


@router_premium.callback_query(BuyPremium.username, F.data == "premback:months")
async def premium_back_to_months(callback: CallbackQuery, state: FSMContext):
    try:
        text, prices = await _premium_prices_text()
    except _FETCH_ERRORS as e:
        await callback.answer(f"Narxlarni olib bo'lmadi: {_err_text(e)}", show_alert=True)
        return
    await state.set_state(BuyPremium.months)
    await callback.message.edit_text(text, reply_markup=premium_months_menu(prices))
    await callback.answer()


@router_premium.callback_query(BuyPremium.confirming, F.data == "premback:username")
async def premium_back_to_username(callback: CallbackQuery, state: FSMContext):
    await state.set_state(BuyPremium.username)
    await callback.message.edit_text("Kimga? Telegram username kiriting (masalan: durov).",
                                      reply_markup=cancel_inline(back_callback="premback:months"))
    await callback.answer()


@router_premium.message(BuyPremium.username, is_free_text)
async def premium_username(message: Message, state: FSMContext):
    username = message.text.strip().lstrip("@")
    if not USERNAME_RE.match(username):
        await message.answer("Username noto'g'ri ko'rinadi. Qayta kiriting (masalan: durov).")
        return

    data = await state.get_data()
    months = data["months"]
    await _process_premium_choice(message, state, username, months, message.from_user.id)


async def _process_premium_choice(message: Message, state: FSMContext, username: str, months: int, user_id: int):
    try:
        prices = await client.get_prices()
        base_price = prices["premium"][str(months)]["price"]
    except _FETCH_ERRORS as e:
        await message.answer(f"\u274C Narxni olib bo'lmadi: {_err_text(e)}")
        return

    price = await with_markup(base_price, "premium")
    balance = await get_balance(user_id)

    await state.update_data(username=username, months=months, price=price)
    await state.set_state(BuyPremium.confirming)

    text = (
        f"\U0001F464 Kimga: @{username}\n"
        f"\U0001F48E Muddat: {months} oy\n"
        f"\U0001F4B5 Narx: {fmt_money(price)} so'm\n"
        f"\U0001F4B0 Balansingiz: {fmt_money(balance)} so'm"
    )

    if balance < price:
        sent = await message.answer(text + "\n\n" + insufficient_balance(price, balance),
                                     reply_markup=balance_menu())
        await track_flow_msg(sent)
        await state.clear()
        return

    await message.answer(text + "\n\n\U0001F48E Ushbu Premiumni sotib olishni tasdiqlaysizmi?", reply_markup=confirm_menu("buyprem:confirm", back_callback="premback:username"))


@router_premium.callback_query(BuyPremium.confirming, F.data == "buyprem:confirm")
@single_flight_purchase
async def confirm_premium(callback: CallbackQuery, state: FSMContext, bot):
    data = await state.get_data()
    username = data["username"]
    months = data["months"]
    est_price = data["price"]

    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

    if not await try_deduct_balance(callback.from_user.id, est_price):
        balance = await get_balance(callback.from_user.id)
        sent = await callback.message.answer(insufficient_balance(est_price, balance), reply_markup=balance_menu())
        await track_flow_msg(sent)
        await state.clear()
        await callback.answer()
        return

    await _safe_answer(callback, "Amalga oshirilmoqda...")

    try:
        result = await _call_idempotent(client.buy_premium, username, months)
    except PurchaseUnknownError as e:
        await _refund_unknown_purchase(bot, callback, state, est_price, "Premium", e)
        return
    except SmmUpperError as e:
        await change_balance(callback.from_user.id, est_price)
        await callback.message.answer(
            f"\u274C Xatolik: {e.message}\n\U0001F4B0 Pulingiz balansga qaytarildi.",
            reply_markup=main_menu(),
        )
        await state.clear()
        return

    charged_price = await final_price(callback.from_user.id, result, est_price, "premium")
    order_pk = await create_order(
        user_id=callback.from_user.id,
        order_type="premium",
        ref=result.get("order_id"),
        server=None,
        price=charged_price,
        details=result,
        status="processing",
        target_username=username,
        qty=months,
    )

    buyer = callback.from_user.full_name
    if callback.from_user.username:
        buyer += f" (@{callback.from_user.username})"
    await notify_channel(bot, channel_premium_notice(buyer, username, months, charged_price), order_type="premium")

    await callback.message.answer(
        f"\u2705 Buyurtma qabul qilindi!\n"
        f"\U0001F464 @{username}\n"
        f"\U0001F48E {months} oy Premium\n"
        f"\U0001F4B5 {fmt_money(charged_price)} so'm\n"
        f"\U0001F522 Buyurtma raqami: {result.get('order_id')} (#{order_pk})\n\n"
        f"Holatini «{BTN_ORDERS}» bo'limidan kuzatishingiz mumkin.",
        reply_markup=main_menu(),
    )
    await state.clear()


# ==============================================================
# UC HANDLER (PUBG Mobile) — SmmUpper Hamkorlik API v2: getUcTariffs / buyUC / getOrder
# ==============================================================
# Qanday ishlaydi (hujjat: https://smmupper.uz/api/v2/docs):
#   1. Tarif tanlanadi (narx = SmmUpper'ning SIZGA amaldagi narxi + ustama foizi).
#   2. Foydalanuvchi PUBG Mobile ID sini kiritadi (faqat raqam, 6-12 xona).
#   3. Balansdan narx atomik yechiladi va BUYURTMA DARHOL YOZILADI (write-ahead: request_id bilan),
#      keyin buyUC yuboriladi. Jarayon shu orada to'xtab qolsa (deploy/SIGTERM) yoki tarmoq uzilsa —
#      pul ham, so'rov ham yo'qolmaydi: fon jarayoni XUDDI SHU request_id bilan qayta yuboradi
#      (SmmUpper takroriy so'rovni ikkinchi marta bajarmaydi).
#   4. UC QO'LDA yuboriladi: buyurtma SmmUpper administratori tasdig'ini kutadi (status "processing").
#      Fonda har ~90 soniyada holat tekshiriladi:
#        done          -> foydalanuvchiga "UC yuborildi" xabari (+ kanalga xabar);
#        failed/error  -> narx foydalanuvchi balansiga QAYTARILADI (faqat bir marta — atomik).
#      "processing/pending/waiting/review" — kutiladi (refund qilinmaydi).
router_uc = Router(name="uc")

UC_ID_RE = re.compile(r"^\d{6,12}$")     # PUBG Mobile ID: faqat raqam, 6-12 xona (hujjatga ko'ra)
UC_POLL_SECONDS = 90                     # kutilayotgan UC buyurtmalari shuncha vaqtda bir tekshiriladi
UC_POLL_BATCH = 40                       # bir aylanishda ko'pi bilan nechta buyurtma tekshiriladi
UC_RECOVER_AFTER = 180                   # soniya: yuborilmay qolgan buyurtmani qayta urinish (handler tugatishga ulgursin)
UC_RECOVER_GIVE_UP = 2 * 3600            # shundan keyin natija noma'lum buyurtma bekor qilinib, pul qaytariladi
UC_STALE_AFTER = 24 * 3600               # shuncha vaqt "processing" tursa — adminga bir marta eslatiladi
UC_MAX_TARIFF_BUTTONS = 90               # Telegram: bitta klaviaturada ko'pi bilan 100 ta tugma
UC_TARIFFS_TEXT = "\U0001F3AE PUBG Mobile UC\n\nTarifni tanlang:"
UC_ASK_ID = (
    "\U0001F3AE PUBG Mobile ID raqamingizni kiriting (faqat raqam, 6\u201312 xona).\n"
    "Masalan: 5123456789\n\n"
    "\u26A0\uFE0F ID ni diqqat bilan tekshiring \u2014 UC noto'g'ri ID ga yuborilsa, qaytarib bo'lmaydi."
)
_UC_FAILED_STATUSES = {"failed", "error", "cancelled", "canceled", "rejected", "refunded"}
_uc_sweep_cursor = {"last_id": 0}          # navbat kursori: oxirgi tekshirilgan buyurtma ID si
_uc_stale_alerted: set = set()


def _uc_mask_id(account_id) -> str:
    """Kanalga chiqarishdan oldin PUBG ID ni qisman yashiradi (51******89)."""
    s = str(account_id or "")
    if len(s) <= 4:
        return "*" * len(s)
    return s[:2] + "*" * (len(s) - 4) + s[-2:]


def channel_uc_notice(buyer: str, account_id, total_uc: int, price: int) -> str:
    return (
        f"\U0001F464 Xaridor: {buyer}\n"
        f"\U0001F3AE PUBG ID: {_uc_mask_id(account_id)}\n"
        f"\U0001F48E Miqdor: {total_uc} UC\n"
        f"\U0001F4B0 To'lov: {fmt_money(price)} so'm"
    )


def _uc_details_from_row(row) -> dict:
    try:
        details = json.loads(row["details"]) if row["details"] else {}
        return details if isinstance(details, dict) else {}
    except Exception:
        return {}


def _uc_account_from_row(row) -> str:
    return str(_uc_details_from_row(row).get("account_id") or "")


async def _uc_alert_admins(bot, text: str):
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, text)
        except Exception:
            pass


async def load_uc_tariffs() -> list:
    """SmmUpper'dan joriy UC tariflarini oladi va ustama qo'shilgan narxni (price) hisoblaydi.
    Qaytadi: [{tariff_id, uc_amount, bonus_uc, total_uc, price}, ...] (UC bo'yicha o'sish tartibida)."""
    data = await client.get_uc_tariffs()
    out = []
    for t in data.get("tariffs") or []:
        try:
            tid = int(t["tariff_id"])
            base = float(t["price"])
            amount = int(t.get("uc_amount") or 0)
            bonus = int(t.get("bonus_uc") or 0)
            total = int(t.get("total_uc") or (amount + bonus))
        except (KeyError, TypeError, ValueError):
            continue
        if base <= 0 or total <= 0:
            continue                       # buzuq yoki "bepul" ko'rinadigan tarifni sotmaymiz
        price = await with_markup(base, "uc")
        if price <= 0:
            continue
        out.append({"tariff_id": tid, "uc_amount": amount, "bonus_uc": bonus, "total_uc": total, "price": price})
    out.sort(key=lambda x: (x["total_uc"], x["tariff_id"]))
    return out


def _uc_tariff_title(t: dict) -> str:
    if t["bonus_uc"]:
        return f"{t['uc_amount']}+{t['bonus_uc']} UC"
    return f"{t['total_uc']} UC"


def uc_tariffs_menu(tariffs: list) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(
        text=f"\U0001F3AE {_uc_tariff_title(t)} \u2014 {fmt_money(t['price'])} so'm",
        callback_data=f"uctariff:{t['tariff_id']}", style=STYLE_PRIMARY)] for t in tariffs[:UC_MAX_TARIFF_BUTTONS]]
    rows.extend(nav_rows())
    return _kb(rows)


_UC_FETCH_ERRORS = (SmmUpperError, KeyError) + _NET_ERRORS


@router_uc.message(F.text.in_(MENU_UC))
async def start_uc_flow(message: Message, state: FSMContext, bot):
    await state.clear()
    await clear_stale_flow_message(bot, message.from_user.id)
    try:
        tariffs = await load_uc_tariffs()
    except _UC_FETCH_ERRORS as e:
        await message.answer(f"\u274C Narxlarni olib bo'lmadi: {e}")
        return
    if not tariffs:
        await message.answer("\u274C Hozircha UC tariflari mavjud emas. Keyinroq urinib ko'ring.")
        return
    await state.set_state(BuyUC.tariff)
    await hide_main_menu(message)
    await message.answer(UC_TARIFFS_TEXT, reply_markup=uc_tariffs_menu(tariffs))


@router_uc.callback_query(BuyUC.tariff, F.data.startswith("uctariff:"))
async def uc_tariff_choice(callback: CallbackQuery, state: FSMContext):
    try:
        tariff_id = int(callback.data.split(":")[1])
    except (IndexError, ValueError):
        await callback.answer()
        return
    try:
        tariffs = await load_uc_tariffs()
    except _UC_FETCH_ERRORS as e:
        await callback.answer(f"Narxlarni olib bo'lmadi: {e}", show_alert=True)
        return
    tariff = next((t for t in tariffs if t["tariff_id"] == tariff_id), None)
    if not tariff:
        # tarif o'chirilgan/yangilangan — ro'yxatni yangilab ko'rsatamiz (buyUC 404 bermasligi uchun)
        await callback.answer("Bu tarif endi mavjud emas \u2014 ro'yxat yangilandi.", show_alert=True)
        if tariffs:
            await callback.message.edit_text(UC_TARIFFS_TEXT, reply_markup=uc_tariffs_menu(tariffs))
        return
    await state.update_data(tariff_id=tariff_id)
    await state.set_state(BuyUC.account)
    await callback.message.edit_text(UC_ASK_ID, reply_markup=cancel_inline(back_callback="ucback:tariff"))
    await callback.answer()


@router_uc.callback_query(BuyUC.account, F.data == "ucback:tariff")
async def uc_back_to_tariffs(callback: CallbackQuery, state: FSMContext):
    try:
        tariffs = await load_uc_tariffs()
    except _UC_FETCH_ERRORS as e:
        await callback.answer(f"Narxlarni olib bo'lmadi: {e}", show_alert=True)
        return
    if not tariffs:
        await callback.answer("Hozircha UC tariflari mavjud emas.", show_alert=True)
        return
    await state.set_state(BuyUC.tariff)
    await callback.message.edit_text(UC_TARIFFS_TEXT, reply_markup=uc_tariffs_menu(tariffs))
    await callback.answer()


@router_uc.callback_query(BuyUC.confirming, F.data == "ucback:account")
async def uc_back_to_account(callback: CallbackQuery, state: FSMContext):
    await state.set_state(BuyUC.account)
    await callback.message.edit_text(UC_ASK_ID, reply_markup=cancel_inline(back_callback="ucback:tariff"))
    await callback.answer()


@router_uc.message(BuyUC.account, is_free_text)
async def uc_account(message: Message, state: FSMContext):
    account_id = re.sub(r"\s+", "", message.text or "")
    if not UC_ID_RE.match(account_id):
        await message.answer("\u274C ID noto'g'ri. Faqat raqam, 6\u201312 xona bo'lishi kerak. Masalan: 5123456789")
        return
    data = await state.get_data()
    tariff_id = data.get("tariff_id")
    if not tariff_id:
        await state.clear()
        await message.answer("Sessiya tugagan. \u00abUC sotib olish\u00bb orqali qaytadan boshlang.", reply_markup=main_menu())
        return
    await _process_uc_choice(message, state, tariff_id, account_id, message.from_user.id)


async def _process_uc_choice(message: Message, state: FSMContext, tariff_id: int, account_id: str, user_id: int):
    try:
        tariffs = await load_uc_tariffs()
    except _UC_FETCH_ERRORS as e:
        await message.answer(f"\u274C Narxni olib bo'lmadi: {e}")
        return
    tariff = next((t for t in tariffs if t["tariff_id"] == int(tariff_id)), None)
    if not tariff:
        await state.clear()
        await message.answer(
            "\u274C Bu tarif hozircha mavjud emas. \u00abUC sotib olish\u00bb orqali boshqa tarif tanlang.",
            reply_markup=main_menu())
        return

    price = tariff["price"]
    balance = await get_balance(user_id)
    await state.update_data(tariff_id=tariff["tariff_id"], account_id=account_id, price=price,
                            total_uc=tariff["total_uc"])
    await state.set_state(BuyUC.confirming)

    text = (
        f"\U0001F3AE PUBG ID: {account_id}\n"
        f"\U0001F48E UC: {_uc_tariff_title(tariff)}\n"
        f"\U0001F4B5 Narx: {fmt_money(price)} so'm\n"
        f"\U0001F4B0 Balansingiz: {fmt_money(balance)} so'm"
    )
    if balance < price:
        sent = await message.answer(text + "\n\n" + insufficient_balance(price, balance), reply_markup=balance_menu())
        await track_flow_msg(sent)
        await state.clear()
        return
    await message.answer(
        text + "\n\n\u26A0\uFE0F ID to'g'riligini tekshiring. UC administrator tasdiqlagandan so'ng yuboriladi.\n\n"
        "\U0001F3AE Ushbu UC ni sotib olishni tasdiqlaysizmi?",
        reply_markup=confirm_menu("buyuc:confirm", back_callback="ucback:account"))


# ---------------- Buyurtma bilan ishlash (dual-mode SQL) ----------------
async def _uc_claim_ref(order_pk: int, ref: str) -> bool:
    """Buyurtmaga SmmUpper order_id sini FAQAT ref hali yo'q bo'lsa yozadi (atomik): natijani qo'llash
    (narx farqi, holat) faqat bitta jarayonda — handlerda YOKI fon jarayonida — bajarilishi uchun."""
    if _PG:
        async with _pool.acquire() as conn:
            res = await conn.execute(
                "UPDATE orders SET ref = $1 WHERE id = $2 AND ref IS NULL AND status = 'processing'", ref, order_pk)
            return res.split()[-1] != "0"
    async with _db_sqlite() as db:
        cur = await db.execute(
            "UPDATE orders SET ref = ? WHERE id = ? AND ref IS NULL AND status = 'processing'", (ref, order_pk))
        await db.commit()
        return cur.rowcount > 0


async def _uc_write_final(order_pk: int, price: int, qty: Optional[int], details: dict, status: str) -> bool:
    """Yakuniy narx/miqdor/tafsilot/holatni yozadi — faqat buyurtma hali 'processing' bo'lsa
    (agar shu orada refund qilingan bo'lsa, ustidan yozib yubormaydi)."""
    details_json = json.dumps(details, ensure_ascii=False)
    if _PG:
        async with _pool.acquire() as conn:
            res = await conn.execute(
                "UPDATE orders SET price = $1, qty = $2, details = $3, status = $4 "
                "WHERE id = $5 AND status = 'processing'", price, qty, details_json, status, order_pk)
            return res.split()[-1] != "0"
    async with _db_sqlite() as db:
        cur = await db.execute(
            "UPDATE orders SET price = ?, qty = ?, details = ?, status = ? WHERE id = ? AND status = 'processing'",
            (price, qty, details_json, status, order_pk))
        await db.commit()
        return cur.rowcount > 0


async def _set_order_status_if(order_pk: int, expected: str, new_status: str, details: Optional[dict] = None) -> bool:
    """Holatni FAQAT hozirgi holat `expected` bo'lsa o'zgartiradi (atomik) — parallel tekshiruvlarda
    (bir necha nusxa / qo'lda tekshirish + fon tekshiruvi) xabar va pul ikki marta ishlamasligi uchun."""
    details_json = json.dumps(details, ensure_ascii=False) if details is not None else None
    if _PG:
        async with _pool.acquire() as conn:
            if details_json is not None:
                res = await conn.execute(
                    "UPDATE orders SET status = $1, details = $2 WHERE id = $3 AND status = $4",
                    new_status, details_json, order_pk, expected)
            else:
                res = await conn.execute(
                    "UPDATE orders SET status = $1 WHERE id = $2 AND status = $3", new_status, order_pk, expected)
            return res.split()[-1] != "0"
    async with _db_sqlite() as db:
        if details_json is not None:
            cur = await db.execute(
                "UPDATE orders SET status = ?, details = ? WHERE id = ? AND status = ?",
                (new_status, details_json, order_pk, expected))
        else:
            cur = await db.execute(
                "UPDATE orders SET status = ? WHERE id = ? AND status = ?", (new_status, order_pk, expected))
        await db.commit()
        return cur.rowcount > 0


async def _list_pending_uc_orders(limit: int, after_id: int = 0) -> list:
    """Kuzatiladigan (SmmUpper order_id si bor) buyurtmalar, ID > after_id: [(id, created_at), ...]"""
    sql = ("SELECT id, created_at FROM orders WHERE order_type = 'uc' AND status = 'processing' "
           "AND ref IS NOT NULL AND id > ")
    if _PG:
        async with _pool.acquire() as conn:
            rows = await conn.fetch(sql + "$1 ORDER BY id LIMIT $2", after_id, limit)
            return [(r["id"], r["created_at"]) for r in rows]
    async with _db_sqlite() as db:
        cur = await db.execute(sql + "? ORDER BY id LIMIT ?", (after_id, limit))
        rows = await cur.fetchall()
        return [(r[0], r[1]) for r in rows]


async def _list_unsent_uc_orders(created_before: int, limit: int) -> list:
    """Yuborilishi noma'lum buyurtmalar (ref yo'q): pul yechilgan, buyUC natijasi olinmagan."""
    sql = ("SELECT id FROM orders WHERE order_type = 'uc' AND status = 'processing' AND ref IS NULL "
           "AND created_at < ")
    if _PG:
        async with _pool.acquire() as conn:
            rows = await conn.fetch(sql + "$1 ORDER BY id LIMIT $2", created_before, limit)
            return [r["id"] for r in rows]
    async with _db_sqlite() as db:
        cur = await db.execute(sql + "? ORDER BY id LIMIT ?", (created_before, limit))
        rows = await cur.fetchall()
        return [r[0] for r in rows]


async def _uc_buyer_name(bot, user_id: int) -> str:
    try:
        chat = await bot.get_chat(user_id)
        name = chat.full_name or str(user_id)
        return f"{name} (@{chat.username})" if chat.username else name
    except Exception:
        return str(user_id)


async def _uc_notify_refund(bot, order_pk: int, user_id: int, price: int, reason: str):
    balance = await get_balance(user_id)
    try:
        await bot.send_message(
            user_id,
            f"\u274C UC buyurtmangiz (#{order_pk}) bajarilmadi ({reason}).\n"
            f"\U0001F4B0 {fmt_money(price)} so'm balansingizga qaytarildi.\n"
            f"Yangi balans: {fmt_money(balance)} so'm")
    except Exception:
        pass


async def _uc_apply_buy_result(bot, order_pk: int, result: dict) -> Optional[dict]:
    """buyUC muvaffaqiyatli javobini buyurtmaga qo'llaydi: order_id (ref), narx farqi, miqdor, holat.
    BIR MARTA (atomik "claim") — handler va fon jarayoni bir vaqtda urinsa ham faqat bittasi bajaradi.
    Qaytadi: {order_id, price, total_uc, status, account_id} yoki None (allaqachon qo'llangan / order_id yo'q)."""
    order_id = result.get("order_id")
    if not order_id:
        return None
    row = await get_order_row(order_pk)
    if not row or row["order_type"] != "uc" or row["ref"] or row["status"] != "processing":
        return None
    if not await _uc_claim_ref(order_pk, str(order_id)):
        return None

    details = _uc_details_from_row(row)
    user_id = row["user_id"]
    est_price = row["price"]
    charged_price = await final_price(user_id, result, est_price, "uc")

    total_uc = int(result.get("total_uc") or details.get("total_uc") or row["qty"] or 0)
    account_id = str(result.get("account_id") or details.get("account_id") or "")
    status = "done" if str(result.get("status") or "").lower() == "done" else "processing"
    new_details = {**details, **result, "account_id": account_id, "unsent": False}
    await _uc_write_final(order_pk, charged_price, total_uc or None, new_details, status)
    if status == "done":
        buyer = await _uc_buyer_name(bot, user_id)
        await notify_channel(bot, channel_uc_notice(buyer, account_id, total_uc, charged_price), order_type="uc")
    return {"order_id": order_id, "price": charged_price, "total_uc": total_uc, "status": status,
            "account_id": account_id}


def _uc_success_text(info: dict, order_pk: int) -> str:
    if info["status"] == "done":
        tail = "\u2705 UC yuborildi!"
    else:
        tail = (
            "\u23F3 UC administrator tasdiqlagandan so'ng yuboriladi. Tayyor bo'lganda o'zimiz xabar beramiz; "
            f"holatini \u00ab{BTN_ORDERS}\u00bb bo'limidan ham ko'rishingiz mumkin."
        )
    return (
        f"\u2705 Buyurtma qabul qilindi!\n"
        f"\U0001F3AE PUBG ID: {info['account_id']}\n"
        f"\U0001F48E {info['total_uc']} UC\n"
        f"\U0001F4B5 {fmt_money(info['price'])} so'm\n"
        f"\U0001F522 Buyurtma raqami: {info['order_id']} (#{order_pk})\n\n"
        f"{tail}"
    )


@router_uc.callback_query(BuyUC.confirming, F.data == "buyuc:confirm")
@single_flight_purchase
async def confirm_uc(callback: CallbackQuery, state: FSMContext, bot):
    data = await state.get_data()
    tariff_id = data.get("tariff_id")
    account_id = data.get("account_id")
    est_price = data.get("price")
    est_total = data.get("total_uc")
    user_id = callback.from_user.id
    if not (tariff_id and account_id and est_price):
        await state.clear()
        await _safe_answer(callback, "Sessiya tugagan. Qaytadan boshlang.", show_alert=True)
        return

    # Callback'ga BIRINCHI javob beramiz (xato bersa ham e'tiborsiz): pul yechilgandan KEYIN xato chiqib,
    # xaridni to'xtatib qo'ymasligi uchun.
    await _safe_answer(callback, "Amalga oshirilmoqda...")
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

    if not await try_deduct_balance(user_id, est_price):
        balance = await get_balance(user_id)
        sent = await callback.message.answer(insufficient_balance(est_price, balance), reply_markup=balance_menu())
        await track_flow_msg(sent)
        await state.clear()
        return

    request_id = new_request_id()
    # WRITE-AHEAD: buyurtma buyUC'dan OLDIN yoziladi — jarayon shu orada to'xtasa ham pul va so'rov yo'qolmaydi
    # (fon jarayoni shu request_id bilan qayta yuboradi).
    try:
        order_pk = await create_order(
            user_id=user_id, order_type="uc", ref=None, server=None, price=est_price,
            details={"request_id": request_id, "tariff_id": tariff_id, "account_id": str(account_id),
                     "total_uc": est_total, "unsent": True},
            status="processing", qty=est_total or None,
        )
    except Exception:
        logging.exception("UC: buyurtmani yozib bo'lmadi — pul qaytarildi")
        await change_balance(user_id, est_price)              # SmmUpper'ga hali hech narsa yuborilmagan
        await callback.message.answer(
            "\u274C Vaqtinchalik xatolik. \U0001F4B0 Pulingiz balansga qaytarildi, qayta urinib ko'ring.",
            reply_markup=main_menu())
        await state.clear()
        return

    try:
        result = await _call_idempotent(client.buy_uc, tariff_id, str(account_id), request_id=request_id)
        if not result.get("order_id"):
            raise PurchaseUnknownError(request_id, "javobda order_id yo'q")
    except SmmUpperError as e:
        # Aniq rad etildi (balans/tarif/ID): buyurtma 'refunded' bo'ladi va pul qaytadi (atomik, bir marta)
        await refund_order(order_pk)
        await callback.message.answer(
            f"\u274C Xatolik: {e.message}\n\U0001F4B0 Pulingiz balansga qaytarildi.",
            reply_markup=main_menu(),
        )
        await state.clear()
        return
    except PurchaseUnknownError as e:
        # Natija noma'lum: buyurtma 'processing' (ref yo'q) holida qoladi — fon jarayoni xuddi shu request_id bilan
        # qayta uradi. Pul YO'QOLMAYDI: natija aniqlangach yoki 2 soatdan keyin qaytariladi.
        logging.error(f"UC #{order_pk}: buyUC natijasi noma'lum user={user_id} request_id={request_id}: {e}")
        await _uc_alert_admins(
            bot,
            f"\u26A0\uFE0F UC #{order_pk}: SmmUpper javobi olinmadi (tarmoq). request_id: {request_id}\n"
            f"Foydalanuvchi ID: {user_id}, PUBG ID: {account_id}, summa: {fmt_money(est_price)} so'm.\n"
            f"Bot shu request_id bilan avtomatik qayta uradi (takroriy xarid bo'lmaydi).")
        try:
            await callback.message.answer(
                f"\u23F3 Buyurtmangiz (#{order_pk}) qabul qilindi, lekin SmmUpper bilan aloqa sekin.\n"
                "Natijani tez orada o'zimiz xabar qilamiz \u2014 pulingiz yo'qolmaydi.",
                reply_markup=main_menu())
        except Exception:
            pass
        await state.clear()
        return

    info = await _uc_apply_buy_result(bot, order_pk, result)
    if info is None:                       # kamdan-kam: shu orada fon jarayoni qo'llab bo'lgan
        info = {"order_id": result.get("order_id"), "price": est_price, "total_uc": est_total or 0,
                "status": "processing", "account_id": str(account_id)}
    try:
        await callback.message.answer(_uc_success_text(info, order_pk), reply_markup=main_menu())
    except Exception:
        logging.exception("UC: tasdiq xabarini yuborib bo'lmadi")
    await state.clear()


# ---------------- Yuborilmay qolgan buyurtmalarni tiklash ----------------
async def uc_recover_order(bot, order_pk: int) -> Optional[str]:
    """Pul yechilgan, lekin buyUC natijasi olinmagan (ref yo'q) buyurtmani XUDDI SHU request_id bilan qayta yuboradi.
    SmmUpper takroriy so'rovni ikkinchi marta bajarmaydi: agar birinchisi o'tgan bo'lsa — saqlangan natijani qaytaradi.
    Qaytadi: "sent" | "refunded" | "pending" | None."""
    row = await get_order_row(order_pk)
    if not row or row["order_type"] != "uc" or row["status"] != "processing" or row["ref"]:
        return None
    user_id = row["user_id"]
    details = _uc_details_from_row(row)
    request_id = details.get("request_id")
    tariff_id, account_id = details.get("tariff_id"), details.get("account_id")
    age = int(time.time()) - int(row["created_at"] or 0)
    have_data = bool(request_id and tariff_id and account_id)

    result, definite_error = None, None
    if have_data:
        try:
            result = await _call_idempotent(client.buy_uc, tariff_id, str(account_id), request_id=request_id, attempts=1)
        except SmmUpperError as e:
            definite_error = e
        except PurchaseUnknownError:
            result = None

    if result and result.get("order_id"):
        info = await _uc_apply_buy_result(bot, order_pk, result)
        if info:
            try:
                await bot.send_message(user_id, _uc_success_text(info, order_pk))
            except Exception:
                pass
            await _uc_alert_admins(bot, f"\u2705 UC #{order_pk}: buyurtma qayta yuborildi va tasdiqlandi ({info['order_id']}).")
        return "sent"

    if definite_error is not None or not have_data or age > UC_RECOVER_GIVE_UP:
        refund = await refund_order(order_pk)
        if refund:
            reason = definite_error.message if definite_error is not None else "SmmUpper javob bermadi"
            await _uc_notify_refund(bot, order_pk, user_id, refund["price"], reason)
            if definite_error is None:
                await _uc_alert_admins(
                    bot,
                    f"\u26A0\uFE0F UC #{order_pk}: natija aniqlanmadi, pul foydalanuvchiga qaytarildi "
                    f"(request_id: {request_id}). Xarid SmmUpper'da bo'lgan-bo'lmaganini tekshiring.")
        return "refunded"
    return "pending"


# ---------------- Holatni kuzatish: done -> xabar, failed/error -> pulni qaytarish ----------------
async def uc_sync_order(bot, order_pk: int) -> Optional[str]:
    """Bitta UC buyurtmaning holatini SmmUpper'dan olib, bazaga yozadi va kerak bo'lsa xabar beradi.
    Qaytadi: "done" | "refunded" | "processing" | None (SmmUpper javob bermadi / buyurtma UC emas)."""
    row = await get_order_row(order_pk)
    if not row or row["order_type"] != "uc":
        return None
    if row["status"] in FINAL_STATUSES:
        return row["status"]
    if not row["ref"]:
        return row["status"]
    try:
        data = await client.get_order(row["ref"])
    except (SmmUpperError,) + _NET_ERRORS:
        return None

    result = data.get("result") or {}
    api_status = str(result.get("status") or "").lower()
    user_id = row["user_id"]
    old_details = _uc_details_from_row(row)

    if api_status == "done":
        if await _set_order_status_if(order_pk, "processing", "done", {**old_details, **result}):
            account_id = old_details.get("account_id") or result.get("account_id") or ""
            total_uc = row["qty"] or result.get("total_uc") or 0
            try:
                await bot.send_message(
                    user_id,
                    f"\u2705 UC yuborildi!\n\U0001F3AE PUBG ID: {account_id}\n\U0001F48E {total_uc} UC\n"
                    f"\U0001F522 Buyurtma #{order_pk}\n\nO'yinda tekshirib ko'ring. Rahmat! \U0001F64C")
            except Exception:
                pass
            buyer = await _uc_buyer_name(bot, user_id)
            await notify_channel(bot, channel_uc_notice(buyer, account_id, total_uc, row["price"]), order_type="uc")
        return "done"

    if api_status in _UC_FAILED_STATUSES or (result.get("refunded") is True and api_status != "done"):
        refund = await refund_order(order_pk)          # atomik: faqat 'processing' bo'lsa, bir marta
        if refund:
            await _uc_notify_refund(bot, order_pk, user_id, refund["price"], "administrator rad etdi")
        return "refunded"

    return "processing"                                # processing / pending / waiting / review — kutamiz


async def uc_sweep_once(bot) -> int:
    """Bir aylanish: (1) yuborilmay qolgan buyurtmalarni qayta yuboradi, (2) kutilayotganlar holatini tekshiradi.
    Ko'p buyurtma bo'lsa, har aylanishda navbatdagi qism tekshiriladi (eskilari yangilarini to'smasin)."""
    handled = 0
    now = int(time.time())
    for order_pk in await _list_unsent_uc_orders(now - UC_RECOVER_AFTER, 20):
        try:
            await uc_recover_order(bot, order_pk)
        except Exception:
            logging.exception(f"UC buyurtma #{order_pk} ni qayta yuborishda xato")
        handled += 1
        await asyncio.sleep(0.3)

    # Navbat kursori ID bo'yicha: buyurtmalar tugab ketsa ham hech biri o'tkazib yuborilmaydi va eskilari
    # yangilarini to'smaydi (har buyurtma ko'pi bilan ceil(n / UC_POLL_BATCH) aylanishda tekshiriladi).
    cursor = _uc_sweep_cursor["last_id"]
    window = await _list_pending_uc_orders(UC_POLL_BATCH, cursor)
    if len(window) < UC_POLL_BATCH and cursor:
        taken = {r[0] for r in window}
        window += [r for r in await _list_pending_uc_orders(UC_POLL_BATCH - len(window), 0)
                   if r[0] not in taken and r[0] <= cursor]
    _uc_sweep_cursor["last_id"] = window[-1][0] if window else 0
    if window:
        for order_pk, created_at in window:
            status = None
            try:
                status = await uc_sync_order(bot, order_pk)
            except Exception:
                logging.exception(f"UC buyurtma #{order_pk} holatini tekshirishda xato")
            handled += 1
            if status == "processing" and now - int(created_at or now) > UC_STALE_AFTER \
                    and order_pk not in _uc_stale_alerted:
                _uc_stale_alerted.add(order_pk)
                hours = int((now - int(created_at)) // 3600)
                await _uc_alert_admins(
                    bot, f"\u23F0 UC buyurtma #{order_pk} {hours} soatdan beri 'processing' holatida "
                         f"\u2014 SmmUpper panelida tekshiring.")
            await asyncio.sleep(0.3)                   # SmmUpper rate limitini hurmat qilamiz
    return handled


async def uc_sweeper(bot):
    """Fon jarayoni: UC buyurtmalar administrator tomonidan tasdiqlanishi/rad etilishini kuzatadi."""
    while True:
        try:
            await asyncio.sleep(UC_POLL_SECONDS)
            await uc_sweep_once(bot)
        except asyncio.CancelledError:
            raise
        except Exception:
            logging.exception("UC kuzatuvchisida xato")


async def _check_uc_order_callback(callback: CallbackQuery, bot, order_pk: int):
    """Buyurtmalarim -> "holatini tekshirish" tugmasi (UC uchun)."""
    status = await uc_sync_order(bot, order_pk)
    if status is None:
        text = "Xatolik: SmmUpper javob bermadi. Birozdan so'ng qayta urinib ko'ring."
    elif status == "done":
        text = "\u2705 UC yuborilgan."
    elif status == "refunded":
        text = "\u274C Buyurtma rad etilgan \u2014 pul balansingizga qaytarilgan."
    else:
        text = "\u23F3 Buyurtma administrator tasdig'ini kutmoqda. Tayyor bo'lganda o'zimiz xabar beramiz."
    await callback.answer(text, show_alert=True)


# ==============================================================
# BUYURTMALAR HANDLER (handler_orders)
# ==============================================================
router_orders = Router(name="orders")

FINAL_STATUSES = {"done", "failed", "error", "refunded"}

_STATUS_EMOJI = {
    "done": "\u2705", "processing": "\u23F3", "pending": "\u23F3", "waiting": "\u23F3",
    "failed": "\u274C", "error": "\u274C", "review": "\U0001F575", "refunded": "\U0001F4B8",
}
_TYPE_LABEL = {
    "number": "\U0001F4F1 Raqam", "stars": "\u2B50 Stars", "premium": "\U0001F48E Premium",
    "uc": "\U0001F3AE UC",
}


@router_orders.message(F.text.in_(MENU_ORDERS))
async def show_my_orders(message: Message, state: FSMContext):
    await state.clear()
    rows = await list_orders(message.from_user.id, limit=10)
    if not rows:
        await message.answer(NO_ORDERS_YET)
        return

    lines = ["\U0001F4CB Oxirgi buyurtmalaringiz:"]
    buttons = []
    for row in rows:
        status = row["status"]
        lines.append(
            f"\n{_STATUS_EMOJI.get(status, '\u2754')} {_TYPE_LABEL.get(row['order_type'], row['order_type'])} "
            f"— {fmt_money(row['price'])} so'm — #{row['id']} ({status})"
        )
        if status not in FINAL_STATUSES and row["ref"]:
            buttons.append([InlineKeyboardButton(
                text=f"\U0001F504 #{row['id']} holatini tekshirish",
                callback_data=f"ordercheck:{row['id']}",
            )])

    markup = InlineKeyboardMarkup(inline_keyboard=buttons) if buttons else None
    await message.answer("\n".join(lines), reply_markup=markup)


@router_orders.callback_query(F.data.startswith("ordercheck:"))
async def check_order(callback: CallbackQuery, bot):
    order_pk = int(callback.data.split(":")[1])
    row = await get_order_row(order_pk)
    if not row or row["user_id"] != callback.from_user.id:
        await callback.answer("Topilmadi.", show_alert=True)
        return

    # Muhim: agar buyurtma bazada allaqachon yakuniy holatda bo'lsa (masalan
    # "refunded" — pul qaytarilgan), SmmUpper'dan qayta so'ramaymiz. Aks holda
    # SmmUpper hali "processing" deb javob qaytarsa, bu bizning "refunded"
    # statusimizni ustidan yozib, xuddi shu buyurtma uchun pulni qayta
    # qaytarib olish yo'lini ochib qo'yishi mumkin edi.
    if row["status"] in FINAL_STATUSES:
        await callback.answer(f"Holat: {row['status']}", show_alert=True)
        return

    # UC: holat SmmUpper'dan olinadi; rad etilgan bo'lsa pul BIR MARTA (atomik) balansga qaytariladi.
    if row["order_type"] == "uc":
        await _check_uc_order_callback(callback, bot, order_pk)
        return

    # "Raqam" turidagi buyurtmalarda "ref" — hash_code/number/id, Stars/
    # Premium'dagi kabi haqiqiy order_id emas. SmmUpper'ning umumiy
    # getOrder'i bunday ID bilan noto'g'ri ishlashi mumkin, shuning uchun
    # bu turdagi buyurtmalar uchun har doim getCode orqali (xuddi
    # manual_check_code'dagi kabi) tekshiramiz.
    if row["order_type"] == "number":
        await _check_and_report_number_code(callback, order_pk, row, clear_markup=False)
        return

    try:
        data = await client.get_order(row["ref"])
    except SmmUpperError as e:
        await callback.answer(f"Xatolik: {e.message}", show_alert=True)
        return

    result = data.get("result", {})
    status = result.get("status", row["status"])
    if str(status).lower() in ("failed", "error"):
        # Bajarilmagan Stars/Premium: hujjatga ko'ra pul SIZNING SmmUpper balansingizga qaytariladi, shuning uchun
        # foydalanuvchi puli ham qaytariladi (atomik, faqat bir marta) — aks holda foydalanuvchi puldan ayrilib qolardi.
        refund = await refund_order(order_pk)
        await callback.answer(
            "\u274C Buyurtma bajarilmadi \u2014 pul balansingizga qaytarildi." if refund else "Holat: refunded",
            show_alert=True)
        return
    await update_order_status(order_pk, status, result)
    await callback.answer(f"Holat: {status}", show_alert=True)


def _format_order_detail(row, include_buyer: bool = False) -> str:
    """Bitta buyurtmani (#ID bilan) batafsil ko'rinishda matnga aylantiradi —
    '📋 Buyurtmalarim' ro'yxatidagi bitta qatorga o'xshash, lekin turi bo'yicha
    qo'shimcha maydonlar (davlat / kimga / miqdor) bilan boyitilgan.
    include_buyer=True bo'lsa (admin panelida), buyurtma kimniki ekanini
    ham (user_id) qo'shadi."""
    status = row["status"]
    lines = [
        f"{_STATUS_EMOJI.get(status, '\u2754')} {_TYPE_LABEL.get(row['order_type'], row['order_type'])} \u2014 #{row['id']}",
    ]
    if include_buyer:
        lines.append(f"\U0001F464 Foydalanuvchi ID: {row['user_id']}")
    lines.append(f"Holat: {status}")
    lines.append(f"\U0001F4B0 Narx: {fmt_money(row['price'])} so'm")
    if row["order_type"] == "number" and row["country"]:
        lines.append(f"\U0001F30E Davlat: {country_flag(row['country'])} {country_display_name(row['country'])}")
    target_username = row["target_username"] if "target_username" in row.keys() else None
    if target_username:
        lines.append(f"\U0001F3AF Kimga: @{target_username}")
    qty = row["qty"] if "qty" in row.keys() else None
    if qty:
        if row["order_type"] == "stars":
            lines.append(f"\u2B50 Miqdor: {qty}")
        elif row["order_type"] == "premium":
            lines.append(f"\U0001F4C5 Muddat: {qty} oy")
        elif row["order_type"] == "uc":
            lines.append(f"\U0001F48E UC: {qty}")
    if row["order_type"] == "uc":
        account_id = _uc_account_from_row(row)
        if account_id:
            lines.append(f"\U0001F3AE PUBG ID: {account_id}")
    return "\n".join(lines)


@router_orders.message(F.text.in_(MENU_CHECK_ORDER))
async def start_check_order(message: Message, state: FSMContext):
    await state.clear()
    await state.set_state(CheckOrder.order_id)
    await message.answer(
        "Buyurtma ID raqamini kiriting (masalan: 42) \u2014 bu \u00ab\U0001F4CB Buyurtmalarim\u00bb "
        "ro'yxatida har bir buyurtma oldida \u00ab#\u00bb belgisi bilan ko'rsatilgan.",
        reply_markup=cancel_inline(),
    )


@router_orders.message(CheckOrder.order_id, is_free_text)
async def check_order_by_id(message: Message, state: FSMContext):
    text = message.text.strip().lstrip("#")
    if not text.isdigit():
        await message.answer("Iltimos, faqat buyurtma raqamini (son) yuboring, masalan: 42")
        return

    order_pk = int(text)
    row = await get_order_row(order_pk)
    await state.clear()

    if not row or row["user_id"] != message.from_user.id:
        await message.answer(
            f"\u274C #{order_pk} raqamli buyurtma topilmadi (yoki sizga tegishli emas).",
            reply_markup=main_menu(),
        )
        return

    markup = None
    if row["status"] not in FINAL_STATUSES and row["ref"]:
        markup = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(
            text="\U0001F504 Holatini tekshirish", callback_data=f"ordercheck:{row['id']}",
        )]])
    await message.answer(_format_order_detail(row), reply_markup=markup or main_menu())


@router_orders.message(F.text.in_(MENU_REPEAT_ORDER))
async def repeat_last_order(message: Message, state: FSMContext, bot):
    """Foydalanuvchining eng oxirgi buyurtmasidagi davlat/mahsulotni
    qayta tanlab, to'g'ridan-to'g'ri tasdiqlash bosqichiga olib boradi —
    ogohlantirish/tur tanlash/ro'yxat bosqichlarini qayta bosish shart
    emas. Narx har doim JORIY (joriy ustama bilan) qayta hisoblanadi —
    eski buyurtmadagi narx emas, chunki narxlar/ustama o'shandan beri
    o'zgargan bo'lishi mumkin."""
    await state.clear()
    await clear_stale_flow_message(bot, message.from_user.id)
    row = await get_last_order(message.from_user.id)
    if not row:
        await message.answer("Sizda hali buyurtmalar tarixi yo'q. Avval biror narsa sotib oling.")
        return

    order_type = row["order_type"]
    user_id = message.from_user.id

    if order_type == "number":
        if not row["server"] or not row["country"]:
            await message.answer("Oxirgi buyurtma haqida yetarli ma'lumot yo'q, qaytadan \u00abRaqam sotib olish\u00bb orqali tanlang.")
            return
        try:
            data = await client.available_countries(row["server"])
            countries = data.get("countries") or {}
        except _API_ERRORS as e:
            await message.answer(f"\u274C Narxlarni olib bo'lmadi: {_err_text(e)}")
            return
        info = countries.get(row["country"])
        server = row["server"]
        price = await with_markup((info or {}).get("price"), "number")
        # Himoya: davlat endi mavjud emas yoki provayder narx bermagan bo'lsa — 0 so'mlik xaridga yo'l qo'yilmaydi
        # (choose_country'dagi bilan bir xil himoya; xabari ham davlat topilmagan holat bilan bir xil).
        if not info or price <= 0:
            await message.answer(
                f"\u274C {country_display_name(row['country'])} uchun hozircha raqam yo'q. "
                f"\u00abRaqam sotib olish\u00bb orqali boshqa davlat tanlang."
            )
            return

        balance = await get_balance(user_id)
        await state.update_data(server=server, country=row["country"], countries=countries, price=price)
        await state.set_state(BuyNumber.confirming)

        text = _number_confirm_text(server, row["country"], price, balance)
        if balance < price:
            sent = await message.answer(text + "\n\n" + insufficient_balance(price, balance), reply_markup=balance_menu())
            await track_flow_msg(sent)
            await state.clear()
            return
        await message.answer(text + "\n\n" + _number_confirm_question(server), reply_markup=confirm_menu("buynum:confirm"))

    elif order_type == "stars":
        if not row["target_username"] or not row["qty"]:
            await message.answer("Oxirgi buyurtma haqida yetarli ma'lumot yo'q, qaytadan \u00abStars sotib olish\u00bb orqali tanlang.")
            return
        await state.update_data(username=row["target_username"])
        await _process_stars_amount(message, state, row["qty"], user_id)

    elif order_type == "premium":
        if not row["target_username"] or not row["qty"]:
            await message.answer("Oxirgi buyurtma haqida yetarli ma'lumot yo'q, qaytadan \u00abPremium sotib olish\u00bb orqali tanlang.")
            return
        await _process_premium_choice(message, state, row["target_username"], row["qty"], user_id)

    elif order_type == "uc":
        details = _uc_details_from_row(row)
        tariff_id, account_id = details.get("tariff_id"), details.get("account_id")
        if not tariff_id or not account_id:
            await message.answer("Oxirgi buyurtma haqida yetarli ma'lumot yo'q, qaytadan \u00abUC sotib olish\u00bb orqali tanlang.")
            return
        await _process_uc_choice(message, state, tariff_id, str(account_id), user_id)


# ==============================================================
# ADMIN PANEL HANDLER (handler_admin)
# ==============================================================
# (router_admin quyida emas, balki yuqorida, router_common bilan birga
# e'lon qilingan — sababi shu bo'limning boshida izohlangan.)


async def _is_admin(user_id: int) -> bool:
    """Bootstrap (.env ADMIN_IDS) yoki admin panel orqali qo'shilgan
    qo'shimcha adminlardan biri bo'lsa True."""
    if user_id in ADMIN_IDS:
        return True
    return user_id in await get_extra_admin_ids()


def _mask_key(key: str) -> str:
    if not key:
        return "\u2014 (o'rnatilmagan)"
    if len(key) <= 8:
        return key[0] + "***"
    return key[:4] + "..." + key[-4:]


async def _ask(callback: CallbackQuery, state: FSMContext, new_state, prompt: str, keyboard: InlineKeyboardMarkup):
    """Panel xabarini (edit_text bilan) so'rov matniga aylantiradi va xabar
    manzilini state'ga saqlaydi — keyingi qadamda (erkin matn kelganda)
    aynan shu xabarni tahrirlab davom ettirish uchun. Shu tarzda butun
    sozlash "suhbat"day bitta xabar ichida davom etadi, chatga ortiqcha
    xabarlar to'planib qolmaydi."""
    await state.update_data(panel_chat_id=callback.message.chat.id, panel_message_id=callback.message.message_id)
    await state.set_state(new_state)
    await callback.message.edit_text(prompt, reply_markup=keyboard)
    await callback.answer()


async def _panel_edit(bot, state: FSMContext, message: Message, text: str, keyboard: InlineKeyboardMarkup):
    """Admin yuborgan erkin-matn xabarini o'chiradi va sozlash boshlangandagi
    ASL panel xabarini shu natija bilan tahrirlaydi."""
    data = await state.get_data()
    chat_id = data.get("panel_chat_id")
    message_id = data.get("panel_message_id")
    try:
        await message.delete()
    except Exception:
        pass
    if chat_id and message_id:
        try:
            await bot.edit_message_text(text, chat_id=chat_id, message_id=message_id, reply_markup=keyboard)
            return
        except Exception:
            pass
    await bot.send_message(message.chat.id, text, reply_markup=keyboard)


async def _panel_text() -> str:
    try:
        data = await client.get_balance()
        balance = data["result"]["balance"]
        balance_line = f"\U0001F4B0 SmmUpper balans: {fmt_money(balance)} so'm"
    except (SmmUpperError, KeyError) as e:
        balance_line = f"\u26A0\uFE0F SmmUpper balansini olishda xatolik: {e}"
    return f"{ADMIN_TITLE}\n\n{balance_line}\n\nKerakli bo'limni tanlang \U0001F447"


async def _settings_text() -> str:
    current_key = await get_setting(SETTINGS_KEY_API_KEY, default=SMMUPPER_API_KEY)
    markup_overview = await get_markup_overview()
    current_channel = await get_channel_id()
    card_number, card_holder = await get_card_info()
    refund_seconds = await get_refund_eligible_seconds()
    return (
        f"\u2699\uFE0F Sozlamalar\n\n"
        f"{current_api_key_line(_mask_key(current_key))}\n"
        f"{markup_short_line(markup_overview)}\n"
        f"{current_channel_line(current_channel)}\n"
        f"{current_card_line(card_number, card_holder)}\n"
        f"{current_refund_seconds_line(refund_seconds)}"
    )


def _forcesub_text(channels: list) -> str:
    if not channels:
        return "\U0001F510 Majburiy obuna\n\n\u2014 Hozircha kanal qo'shilmagan (majburiy obuna o'chiq)."
    lines = "\n".join(f"\u2022 {c.get('channel')}" for c in channels)
    return (
        f"\U0001F510 Majburiy obuna\n\n"
        f"Joriy kanallar ({len(channels)} ta):\n{lines}\n\n"
        f"O'chirish uchun kanalni bosing."
    )


def _admins_text(extra_ids: list) -> str:
    bootstrap_line = ", ".join(str(x) for x in ADMIN_IDS) if ADMIN_IDS else "\u2014"
    extra_line = ", ".join(str(x) for x in extra_ids) if extra_ids else "\u2014 (yo'q)"
    return (
        f"\U0001F464 Adminlar\n\n"
        f"\U0001F512 Asosiy (.env, o'chirib bo'lmaydi):\n{bootstrap_line}\n\n"
        f"\u2795 Qo'shimcha adminlar:\n{extra_line}\n\n"
        f"O'chirish uchun ID'ni bosing."
    )


async def _show_panel(message: Message):
    await hide_main_menu(message)
    await message.answer(await _panel_text(), reply_markup=admin_menu(owner=_is_owner(message.from_user.id)))


@router_admin.message(Command("admin"))
async def admin_panel(message: Message, state: FSMContext):
    if not await _is_admin(message.from_user.id):
        return
    await state.clear()
    await _show_panel(message)


@router_admin.callback_query(F.data == "adm:refresh")
async def admin_refresh(callback: CallbackQuery, state: FSMContext):
    if not await _is_admin(callback.from_user.id):
        await callback.answer("Ruxsat yo'q.", show_alert=True)
        return
    await state.clear()
    await callback.message.edit_text(await _panel_text(), reply_markup=admin_menu(owner=_is_owner(callback.from_user.id)))
    await callback.answer()


@router_admin.callback_query(F.data == "adm:menu:users")
async def admin_menu_users(callback: CallbackQuery, state: FSMContext):
    if not await _is_admin(callback.from_user.id):
        await callback.answer("Ruxsat yo'q.", show_alert=True)
        return
    await state.clear()
    await callback.message.edit_text(USERS_MENU_TEXT, reply_markup=users_admin_menu())
    await callback.answer()


@router_admin.callback_query(F.data == "adm:menu:settings")
async def admin_menu_settings(callback: CallbackQuery, state: FSMContext):
    if not await _is_admin(callback.from_user.id):
        await callback.answer("Ruxsat yo'q.", show_alert=True)
        return
    await state.clear()
    await callback.message.edit_text(await _settings_text(), reply_markup=settings_admin_menu())
    await callback.answer()


# ---------- Foydalanuvchi qidirish ----------

@router_admin.callback_query(F.data == "adm:find")
async def admin_find_start(callback: CallbackQuery, state: FSMContext):
    if not await _is_admin(callback.from_user.id):
        await callback.answer("Ruxsat yo'q.", show_alert=True)
        return
    await _ask(callback, state, AdminPanel.find_user, ASK_FIND_USER, users_cancel_menu())


@router_admin.message(AdminPanel.find_user, is_free_text)
async def admin_find_receive(message: Message, state: FSMContext, bot):
    if not await _is_admin(message.from_user.id):
        return
    user = await find_user(message.text.strip())
    if not user:
        await _panel_edit(bot, state, message, "Foydalanuvchi topilmadi.", users_cancel_menu())
    else:
        referral = await get_referral_stats(user["user_id"])
        await _panel_edit(
            bot, state, message,
            admin_user_card(user, referral),
            admin_user_actions_menu(user["user_id"], bool(user.get("banned"))),
        )
    await state.clear()


# ---------- Balans qo'shish/ayirish ----------

@router_admin.callback_query(F.data == "adm:balance")
async def admin_balance_start(callback: CallbackQuery, state: FSMContext):
    if not await _is_admin(callback.from_user.id):
        await callback.answer("Ruxsat yo'q.", show_alert=True)
        return
    await _ask(callback, state, AdminPanel.balance_id, ASK_BALANCE_ID, users_cancel_menu())


@router_admin.callback_query(F.data.startswith("adm:quickbalance:"))
async def admin_quick_balance(callback: CallbackQuery, state: FSMContext):
    """Profil kartasidan to'g'ridan-to'g'ri — ID qayta kiritilmaydi,
    darhol summa so'raladi."""
    if not await _is_admin(callback.from_user.id):
        await callback.answer("Ruxsat yo'q.", show_alert=True)
        return
    target_id = int(callback.data.split(":")[2])
    await state.update_data(target_id=target_id)
    await _ask(callback, state, AdminPanel.balance_amount, ASK_BALANCE_AMOUNT, users_cancel_menu())


@router_admin.callback_query(F.data.startswith("adm:quickban:"))
async def admin_quick_ban(callback: CallbackQuery):
    if not await _is_admin(callback.from_user.id):
        await callback.answer("Ruxsat yo'q.", show_alert=True)
        return
    target_id = int(callback.data.split(":")[2])
    await set_banned(target_id, True)
    await callback.answer(f"\U0001F6AB {target_id} bloklandi.", show_alert=True)
    user = await find_user(str(target_id))
    if user:
        referral = await get_referral_stats(target_id)
        await callback.message.edit_text(admin_user_card(user, referral),
                                          reply_markup=admin_user_actions_menu(target_id, True))


@router_admin.callback_query(F.data.startswith("adm:quickunban:"))
async def admin_quick_unban(callback: CallbackQuery):
    if not await _is_admin(callback.from_user.id):
        await callback.answer("Ruxsat yo'q.", show_alert=True)
        return
    target_id = int(callback.data.split(":")[2])
    await set_banned(target_id, False)
    await callback.answer(f"\u2705 {target_id} blokdan chiqarildi.", show_alert=True)
    user = await find_user(str(target_id))
    if user:
        referral = await get_referral_stats(target_id)
        await callback.message.edit_text(admin_user_card(user, referral),
                                          reply_markup=admin_user_actions_menu(target_id, False))


@router_admin.message(AdminPanel.balance_id, is_free_text)
async def admin_balance_id_receive(message: Message, state: FSMContext, bot):
    if not await _is_admin(message.from_user.id):
        return
    text = message.text.strip()
    if not text.isdigit():
        await _panel_edit(bot, state, message, NOT_A_VALID_ID, users_cancel_menu())
        return
    # Muhim: ID raqam ekanini tekshirish YETARLI EMAS — bunday ID umuman
    # mavjud bo'lmasligi mumkin (masalan xato yozilgan). Foydalanuvchi
    # topilmasa, change_balance() baribir "muvaffaqiyatli" ishlab, aslida
    # HECH NARSANI o'zgartirmasdan jim qoladi (0 qator yangilanadi) — admin
    # esa buni "balans 0 so'm bo'ldi" deb noto'g'ri tushunishi mumkin edi.
    target = await find_user(text)
    if not target:
        await _panel_edit(bot, state, message,
                           f"\u274C {text} ID'li foydalanuvchi topilmadi. Qayta kiriting.",
                           users_cancel_menu())
        return
    await state.update_data(target_id=int(text))
    await state.set_state(AdminPanel.balance_amount)
    await _panel_edit(bot, state, message, ASK_BALANCE_AMOUNT, users_cancel_menu())


@router_admin.message(AdminPanel.balance_amount, is_free_text)
async def admin_balance_amount_receive(message: Message, state: FSMContext, bot):
    if not await _is_admin(message.from_user.id):
        return
    text = message.text.strip()
    if not text.lstrip("-").isdigit():
        await _panel_edit(bot, state, message, NOT_A_VALID_AMOUNT, users_cancel_menu())
        return

    data = await state.get_data()
    user_id = data["target_id"]
    amount = int(text)

    await change_balance(user_id, amount)
    new_balance = await get_balance(user_id)
    await _panel_edit(
        bot, state, message,
        f"\u2705 Bajarildi. {user_id} balansi endi: {fmt_money(new_balance)} so'm",
        users_cancel_menu(),
    )
    await state.clear()
    try:
        await bot.send_message(user_id, balance_adjusted_by_admin(amount, new_balance))
    except Exception:
        pass


# ---------- Bloklash / blokdan chiqarish ----------

@router_admin.callback_query(F.data == "adm:ban")
async def admin_ban_start(callback: CallbackQuery, state: FSMContext):
    if not await _is_admin(callback.from_user.id):
        await callback.answer("Ruxsat yo'q.", show_alert=True)
        return
    await _ask(callback, state, AdminPanel.ban_id, ASK_BAN_ID, users_cancel_menu())


@router_admin.message(AdminPanel.ban_id, is_free_text)
async def admin_ban_receive(message: Message, state: FSMContext, bot):
    if not await _is_admin(message.from_user.id):
        return
    text = message.text.strip()
    if not text.isdigit():
        await _panel_edit(bot, state, message, NOT_A_VALID_ID, users_cancel_menu())
        return
    if not await find_user(text):
        await _panel_edit(bot, state, message,
                           f"\u274C {text} ID'li foydalanuvchi topilmadi. Qayta kiriting.",
                           users_cancel_menu())
        return
    user_id = int(text)
    await set_banned(user_id, True)
    await _panel_edit(bot, state, message, f"\U0001F6AB {user_id} bloklandi.", users_cancel_menu())
    await state.clear()


@router_admin.callback_query(F.data == "adm:unban")
async def admin_unban_start(callback: CallbackQuery, state: FSMContext):
    if not await _is_admin(callback.from_user.id):
        await callback.answer("Ruxsat yo'q.", show_alert=True)
        return
    await _ask(callback, state, AdminPanel.unban_id, ASK_UNBAN_ID, users_cancel_menu())


@router_admin.message(AdminPanel.unban_id, is_free_text)
async def admin_unban_receive(message: Message, state: FSMContext, bot):
    if not await _is_admin(message.from_user.id):
        return
    text = message.text.strip()
    if not text.isdigit():
        await _panel_edit(bot, state, message, NOT_A_VALID_ID, users_cancel_menu())
        return
    if not await find_user(text):
        await _panel_edit(bot, state, message,
                           f"\u274C {text} ID'li foydalanuvchi topilmadi. Qayta kiriting.",
                           users_cancel_menu())
        return
    user_id = int(text)
    await set_banned(user_id, False)
    await _panel_edit(bot, state, message, f"\u2705 {user_id} blokdan chiqarildi.", users_cancel_menu())
    await state.clear()


# ---------- Broadcast ----------

@router_admin.callback_query(F.data == "adm:broadcast")
async def admin_broadcast_start(callback: CallbackQuery, state: FSMContext):
    if not await _is_admin(callback.from_user.id):
        await callback.answer("Ruxsat yo'q.", show_alert=True)
        return
    await _ask(callback, state, AdminPanel.broadcast_text, ASK_BROADCAST_TEXT, admin_cancel_menu())


@router_admin.message(AdminPanel.broadcast_text, is_free_text)
async def admin_broadcast_receive(message: Message, state: FSMContext, bot):
    if not await _is_admin(message.from_user.id):
        return
    text = message.text
    data = await state.get_data()
    chat_id = data.get("panel_chat_id", message.chat.id)
    message_id = data.get("panel_message_id")
    await state.clear()

    try:
        await message.delete()
    except Exception:
        pass

    # Fonda (background task) ishga tushiriladi — shunda 1000+ foydalanuvchiga
    # yuborish davomida admin panel "qotib" qolmaydi va bot boshqa
    # foydalanuvchilarga bemalol javob berishda davom etadi.
    task = asyncio.create_task(_run_broadcast(bot, chat_id, message_id, text))
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)


async def _run_broadcast(bot, chat_id: int, message_id: Optional[int], text: str):
    """Xabarni barcha foydalanuvchiga fonda yuboradi. Telegram flood-limit
    bersa (TelegramRetryAfter), ko'rsatilgan vaqtcha kutib O'SHA
    foydalanuvchiga qayta uriniladi (o'tkazib yuborilmaydi); botni
    bloklagan foydalanuvchilar (TelegramForbiddenError) alohida hisoblanadi."""
    user_ids = await get_all_user_ids()
    progress_text = f"\u23F3 {len(user_ids)} foydalanuvchiga yuborilmoqda..."
    if message_id:
        try:
            await bot.edit_message_text(progress_text, chat_id=chat_id, message_id=message_id)
        except Exception:
            message_id = None
    if not message_id:
        sent_msg = await bot.send_message(chat_id, progress_text)
        message_id = sent_msg.message_id

    sent = 0
    blocked = 0
    failed = 0
    for user_id in user_ids:
        while True:
            try:
                await bot.send_message(user_id, text)
                sent += 1
                break
            except TelegramRetryAfter as e:
                await asyncio.sleep(e.retry_after + 0.5)
                continue
            except TelegramForbiddenError:
                blocked += 1
                break
            except Exception:
                failed += 1
                break
        await asyncio.sleep(0.05)

    result_text = (
        f"\u2705 Yuborildi: {sent}\n"
        f"\U0001F6AB Botni bloklagan: {blocked}\n"
        f"\u274C Boshqa xatolik: {failed}"
    )
    try:
        await bot.edit_message_text(result_text, chat_id=chat_id, message_id=message_id, reply_markup=admin_cancel_menu())
    except Exception:
        try:
            await bot.send_message(chat_id, result_text, reply_markup=admin_cancel_menu())
        except Exception:
            pass


# ---------- API kalitni sozlash ----------

@router_admin.callback_query(F.data == "adm:apikey")
async def admin_apikey_start(callback: CallbackQuery, state: FSMContext):
    if not await _is_admin(callback.from_user.id):
        await callback.answer("Ruxsat yo'q.", show_alert=True)
        return
    current_key = await get_setting(SETTINGS_KEY_API_KEY, default=SMMUPPER_API_KEY)
    await _ask(
        callback, state, AdminPanel.api_key,
        f"{current_api_key_line(_mask_key(current_key))}\n\n{ASK_API_KEY}",
        settings_cancel_menu(),
    )


@router_admin.message(AdminPanel.api_key, is_free_text)
async def admin_apikey_receive(message: Message, state: FSMContext, bot):
    if not await _is_admin(message.from_user.id):
        return
    new_key = message.text.strip()

    if not new_key or len(new_key) < 6:
        await _panel_edit(bot, state, message,
                           "\u274C Kalit juda qisqa ko'rinyapti, qaytadan tekshiring.",
                           settings_cancel_menu())
        return

    await set_setting(SETTINGS_KEY_API_KEY, new_key)

    # Yangi kalit ishlayotganini darhol tekshirib ko'ramiz
    try:
        data = await client.get_balance()
        balance = data["result"]["balance"]
        check_line = f"\u2705 Kalit tekshirildi \u2014 SmmUpper balansi: {fmt_money(balance)} so'm"
    except (SmmUpperError, KeyError) as e:
        check_line = f"\u26A0\uFE0F Kalit saqlandi, lekin tekshirishda xatolik: {e}"

    await _panel_edit(bot, state, message,
                       api_key_saved(_mask_key(new_key)) + "\n\n" + check_line,
                       settings_cancel_menu())
    await state.clear()


# ---------- Narx ustamasini sozlash (bo'lim bo'yicha alohida) ----------

@router_admin.callback_query(F.data == "adm:markup")
async def admin_markup_start(callback: CallbackQuery, state: FSMContext):
    if not await _is_admin(callback.from_user.id):
        await callback.answer("Ruxsat yo'q.", show_alert=True)
        return
    await state.clear()
    overview = await get_markup_overview()
    await callback.message.edit_text(markup_panel_text(overview), reply_markup=markup_admin_menu(overview))
    await callback.answer()


@router_admin.callback_query(F.data.startswith("adm:mk:"))
async def admin_markup_pick(callback: CallbackQuery, state: FSMContext):
    if not await _is_admin(callback.from_user.id):
        await callback.answer("Ruxsat yo'q.", show_alert=True)
        return
    target = callback.data.split(":", 2)[2]
    if target != "all" and target not in MARKUP_CATEGORIES:
        await callback.answer()
        return
    await state.update_data(markup_target=target)
    await _ask(callback, state, AdminPanel.markup_percent, ask_markup_percent(target), markup_cancel_menu())


@router_admin.message(AdminPanel.markup_percent, is_free_text)
async def admin_markup_receive(message: Message, state: FSMContext, bot):
    if not await _is_admin(message.from_user.id):
        return
    data = await state.get_data()
    target = data.get("markup_target", "all")
    text = message.text.strip()

    if target != "all" and text == "-":
        # Bo'sh qiymat = "bu bo'lim uchun alohida ustama yo'q" (get_markup_percent avtomatik umumiyga tushadi) —
        # alohida delete_setting funksiyasiz, xotira sxemasini o'zgartirmasdan.
        await set_setting(_markup_key(target), "")
        overview = await get_markup_overview()
        await _panel_edit(bot, state, message, markup_reset_text(target, overview["items"][target][0]), markup_cancel_menu())
        await state.clear()
        return

    try:
        percent = float(text.replace(",", "."))
    except ValueError:
        await _panel_edit(bot, state, message, NOT_A_VALID_PERCENT, markup_cancel_menu())
        return
    if not math.isfinite(percent) or percent <= -100:
        await _panel_edit(bot, state, message, INVALID_MARKUP_RANGE, markup_cancel_menu())
        return
    if percent > MAX_MARKUP_PERCENT:
        await _panel_edit(bot, state, message, INVALID_MARKUP_MAX, markup_cancel_menu())
        return

    await set_setting(_markup_key(None if target == "all" else target), str(percent))
    await _panel_edit(bot, state, message, markup_saved(percent, target), markup_cancel_menu())
    await state.clear()

# ---------- Xarid kanalini sozlash ----------

@router_admin.callback_query(F.data == "adm:channel")
async def admin_channel_start(callback: CallbackQuery, state: FSMContext):
    if not await _is_admin(callback.from_user.id):
        await callback.answer("Ruxsat yo'q.", show_alert=True)
        return
    current = await get_channel_id()
    await _ask(
        callback, state, AdminPanel.channel_id,
        f"{current_channel_line(current)}\n\n{ASK_CHANNEL_ID}",
        settings_cancel_menu(),
    )


@router_admin.message(AdminPanel.channel_id, is_free_text)
async def admin_channel_receive(message: Message, state: FSMContext, bot):
    if not await _is_admin(message.from_user.id):
        return
    text = message.text.strip()

    if text == "0":
        await set_setting(SETTINGS_KEY_CHANNEL_ID, "")
        await _panel_edit(bot, state, message, CHANNEL_DISABLED, settings_cancel_menu())
        await state.clear()
        return

    if not (text.startswith("@") or text.lstrip("-").isdigit()):
        await _panel_edit(bot, state, message, NOT_A_VALID_CHANNEL, settings_cancel_menu())
        return

    await set_setting(SETTINGS_KEY_CHANNEL_ID, text)

    # Kanal to'g'ri sozlanganini va bot xabar yubora olishini darhol tekshiramiz
    try:
        target = int(text) if text.lstrip("-").isdigit() else text
        await bot.send_message(target, "\u2705 Bot shu kanalga xarid xabarlarini yuboradi.")
        await _panel_edit(bot, state, message, channel_saved_ok(text), settings_cancel_menu())
    except Exception as e:
        await _panel_edit(bot, state, message, channel_saved_warning(text, str(e)), settings_cancel_menu())
    await state.clear()


# ---------- To'lov kartasini sozlash ----------

@router_admin.callback_query(F.data == "adm:card")
async def admin_card_start(callback: CallbackQuery, state: FSMContext):
    if not await _is_admin(callback.from_user.id):
        await callback.answer("Ruxsat yo'q.", show_alert=True)
        return
    card_number, card_holder = await get_card_info()
    await _ask(
        callback, state, AdminPanel.card_number,
        f"{current_card_line(card_number, card_holder)}\n\n{ASK_CARD_NUMBER}",
        settings_cancel_menu(),
    )


@router_admin.message(AdminPanel.card_number, is_free_text)
async def admin_card_number_receive(message: Message, state: FSMContext, bot):
    if not await _is_admin(message.from_user.id):
        return
    card_number = message.text.strip()
    if not card_number:
        await _panel_edit(bot, state, message, "\u274C Karta raqami bo'sh bo'lishi mumkin emas. Qayta kiriting.", settings_cancel_menu())
        return
    await state.update_data(card_number=card_number)
    await state.set_state(AdminPanel.card_holder)
    await _panel_edit(bot, state, message, ASK_CARD_HOLDER, settings_cancel_menu())


@router_admin.message(AdminPanel.card_holder, is_free_text)
async def admin_card_holder_receive(message: Message, state: FSMContext, bot):
    if not await _is_admin(message.from_user.id):
        return
    card_holder = message.text.strip()
    data = await state.get_data()
    card_number = data["card_number"]

    await set_setting(SETTINGS_KEY_CARD_NUMBER, card_number)
    await set_setting(SETTINGS_KEY_CARD_HOLDER, card_holder)
    await _panel_edit(bot, state, message, card_saved(card_number, card_holder), settings_cancel_menu())
    await state.clear()


# ---------- Pul qaytarish vaqtini sozlash ----------

@router_admin.callback_query(F.data == "adm:refundtime")
async def admin_refundtime_start(callback: CallbackQuery, state: FSMContext):
    if not await _is_admin(callback.from_user.id):
        await callback.answer("Ruxsat yo'q.", show_alert=True)
        return
    current = await get_refund_eligible_seconds()
    await _ask(
        callback, state, AdminPanel.refund_seconds,
        f"{current_refund_seconds_line(current)}\n\n{ASK_REFUND_SECONDS}",
        settings_cancel_menu(),
    )


@router_admin.message(AdminPanel.refund_seconds, is_free_text)
async def admin_refundtime_receive(message: Message, state: FSMContext, bot):
    if not await _is_admin(message.from_user.id):
        return
    text = message.text.strip()
    if not text.isdigit() or int(text) <= 0:
        await _panel_edit(bot, state, message, NOT_A_VALID_SECONDS, settings_cancel_menu())
        return

    seconds = int(text)
    await set_setting(SETTINGS_KEY_REFUND_SECONDS, str(seconds))
    await _panel_edit(bot, state, message, refund_seconds_saved(seconds), settings_cancel_menu())
    await state.clear()


# ---------- Majburiy obuna kanallari (ro'yxat: qo'shish/o'chirish) ----------

@router_admin.callback_query(F.data == "adm:forcesub")
async def admin_forcesub_list(callback: CallbackQuery, state: FSMContext):
    if not await _is_admin(callback.from_user.id):
        await callback.answer("Ruxsat yo'q.", show_alert=True)
        return
    await state.clear()
    channels = await get_force_sub_channels()
    await callback.message.edit_text(_forcesub_text(channels), reply_markup=forcesub_admin_menu(channels))
    await callback.answer()


@router_admin.callback_query(F.data == "adm:fs:add")
async def admin_forcesub_add_start(callback: CallbackQuery, state: FSMContext):
    if not await _is_admin(callback.from_user.id):
        await callback.answer("Ruxsat yo'q.", show_alert=True)
        return
    await _ask(callback, state, AdminPanel.force_sub_channel, ASK_FORCE_SUB_CHANNEL, fs_cancel_menu())


@router_admin.message(AdminPanel.force_sub_channel, is_free_text)
async def admin_forcesub_channel_receive(message: Message, state: FSMContext, bot):
    if not await _is_admin(message.from_user.id):
        return
    text = message.text.strip()

    if not (text.startswith("@") or text.lstrip("-").isdigit()):
        await _panel_edit(bot, state, message, NOT_A_VALID_FORCE_SUB_CHANNEL, fs_cancel_menu())
        return

    await state.update_data(fs_channel=text)
    await state.set_state(AdminPanel.force_sub_url)
    await _panel_edit(bot, state, message, ASK_FORCE_SUB_URL, fs_cancel_menu())


@router_admin.message(AdminPanel.force_sub_url, is_free_text)
async def admin_forcesub_url_receive(message: Message, state: FSMContext, bot):
    if not await _is_admin(message.from_user.id):
        return
    data = await state.get_data()
    channel = data["fs_channel"]
    url_text = message.text.strip()
    url = "" if url_text == "-" else url_text
    if not url and channel.startswith("@"):
        url = f"https://t.me/{channel.lstrip('@')}"

    await add_force_sub_channel(channel, url)
    channels = await get_force_sub_channels()
    await _panel_edit(bot, state, message, _forcesub_text(channels), forcesub_admin_menu(channels))
    await state.clear()


@router_admin.callback_query(F.data.startswith("adm:fs:del:"))
async def admin_forcesub_delete(callback: CallbackQuery, state: FSMContext):
    if not await _is_admin(callback.from_user.id):
        await callback.answer("Ruxsat yo'q.", show_alert=True)
        return
    try:
        index = int(callback.data.split(":")[3])
    except (IndexError, ValueError):
        await callback.answer()
        return
    await remove_force_sub_channel(index)
    channels = await get_force_sub_channels()
    await callback.message.edit_text(_forcesub_text(channels), reply_markup=forcesub_admin_menu(channels))
    await callback.answer("O'chirildi.")


# ---------- Adminlar (ro'yxat: qo'shish/o'chirish) ----------

@router_admin.callback_query(F.data == "adm:admins")
async def admin_admins_list(callback: CallbackQuery, state: FSMContext):
    if not await _is_admin(callback.from_user.id):
        await callback.answer("Ruxsat yo'q.", show_alert=True)
        return
    await state.clear()
    ids = await get_extra_admin_ids()
    await callback.message.edit_text(_admins_text(ids), reply_markup=admins_admin_menu(ids))
    await callback.answer()


@router_admin.callback_query(F.data == "adm:adm:add")
async def admin_add_admin_start(callback: CallbackQuery, state: FSMContext):
    if not await _is_admin(callback.from_user.id):
        await callback.answer("Ruxsat yo'q.", show_alert=True)
        return
    await _ask(callback, state, AdminPanel.add_admin_id, ASK_ADD_ADMIN_ID, admins_cancel_menu())


@router_admin.message(AdminPanel.add_admin_id, is_free_text)
async def admin_add_admin_receive(message: Message, state: FSMContext, bot):
    if not await _is_admin(message.from_user.id):
        return
    text = message.text.strip()
    if not text.isdigit():
        await _panel_edit(bot, state, message, NOT_A_VALID_ID, admins_cancel_menu())
        return

    user_id = int(text)
    if user_id in ADMIN_IDS:
        await _panel_edit(bot, state, message, ALREADY_ADMIN, admins_cancel_menu())
        await state.clear()
        return

    added = await add_extra_admin(user_id)
    ids = await get_extra_admin_ids()
    if not added:
        await _panel_edit(bot, state, message, ALREADY_ADMIN, admins_cancel_menu())
        await state.clear()
        return
    await _panel_edit(bot, state, message, _admins_text(ids), admins_admin_menu(ids))
    await state.clear()


@router_admin.callback_query(F.data.startswith("adm:adm:del:"))
async def admin_remove_admin(callback: CallbackQuery, state: FSMContext):
    if not await _is_admin(callback.from_user.id):
        await callback.answer("Ruxsat yo'q.", show_alert=True)
        return
    try:
        user_id = int(callback.data.split(":")[3])
    except (IndexError, ValueError):
        await callback.answer()
        return
    await remove_extra_admin(user_id)
    ids = await get_extra_admin_ids()
    await callback.message.edit_text(_admins_text(ids), reply_markup=admins_admin_menu(ids))
    await callback.answer("O'chirildi.")


# ---------- Statistika ----------

@router_admin.callback_query(F.data == "adm:stats")
async def admin_stats(callback: CallbackQuery, state: FSMContext):
    if not await _is_admin(callback.from_user.id):
        await callback.answer("Ruxsat yo'q.", show_alert=True)
        return
    await state.clear()
    stats = await get_stats()
    await callback.message.edit_text(stats_text(stats), reply_markup=stats_menu())
    await callback.answer()


@router_admin.callback_query(F.data == "adm:topusers")
async def admin_top_users(callback: CallbackQuery):
    if not await _is_admin(callback.from_user.id):
        await callback.answer("Ruxsat yo'q.", show_alert=True)
        return
    rows = await top_spenders(limit=10)
    if not rows:
        await callback.message.edit_text("\U0001F3C6 Hali buyurtmalar yo'q.", reply_markup=stats_menu())
        await callback.answer()
        return

    lines = ["\U0001F3C6 Eng ko'p xarid qilgan userlar (jami to'lov bo'yicha)\n"]
    for i, row in enumerate(rows, start=1):
        name = row["full_name"] or "?"
        if row["username"]:
            name += f" (@{row['username']})"
        lines.append(f"{i}. {name} \u2014 {fmt_money(row['total'])} so'm ({row['cnt']} ta buyurtma)")
    await callback.message.edit_text("\n".join(lines), reply_markup=stats_menu())
    await callback.answer()


@router_admin.callback_query(F.data == "adm:topcountries")
async def admin_top_countries(callback: CallbackQuery):
    if not await _is_admin(callback.from_user.id):
        await callback.answer("Ruxsat yo'q.", show_alert=True)
        return
    rows = await top_countries(limit=10)
    if not rows:
        await callback.message.edit_text("\U0001F30D Hali raqam buyurtmalari yo'q.", reply_markup=stats_menu())
        await callback.answer()
        return

    lines = ["\U0001F30D Eng ko'p sotilgan davlatlar (buyurtmalar soni bo'yicha)\n"]
    for i, (code, cnt) in enumerate(rows, start=1):
        lines.append(f"{i}. {country_flag(code)} {country_display_name(code)} \u2014 {cnt} ta")
    await callback.message.edit_text("\n".join(lines), reply_markup=stats_menu())
    await callback.answer()


@router_admin.callback_query(F.data == "adm:graph")
async def admin_graph(callback: CallbackQuery):
    if not await _is_admin(callback.from_user.id):
        await callback.answer("Ruxsat yo'q.", show_alert=True)
        return
    daily = await daily_revenue(days=7)
    await callback.message.edit_text(revenue_graph_text(daily), reply_markup=stats_menu())
    await callback.answer()


# ---------- To'lovlar paneli (barcha kutilayotgan so'rovlar ro'yxati) ----------

@router_admin.callback_query(F.data == "adm:payments")
async def admin_payments_panel(callback: CallbackQuery, state: FSMContext):
    if not await _is_admin(callback.from_user.id):
        await callback.answer("Ruxsat yo'q.", show_alert=True)
        return
    await state.clear()
    rows = await get_pending_topups(limit=15)
    if not rows:
        await callback.message.edit_text("\U0001F4B3 Kutilayotgan to'lov so'rovlari yo'q.", reply_markup=admin_cancel_menu())
        await callback.answer()
        return

    lines = [f"\U0001F4B3 Kutilayotgan to'lovlar: {len(rows)}\n"]
    keyboard = []
    for row in rows:
        name = row["full_name"] or "\u2014"
        if row["username"]:
            name += f" (@{row['username']})"
        lines.append(f"\U0001F464 {name} \u2014 {fmt_money(row['amount'])} so'm (#{row['id']})")
        keyboard.append([
            InlineKeyboardButton(text=f"\u2705 #{row['id']}", callback_data=f"topup:approve:{row['id']}", style=STYLE_SUCCESS),
            InlineKeyboardButton(text=f"\u274C #{row['id']}", callback_data=f"topup:reject:{row['id']}", style=STYLE_DANGER),
        ])
    keyboard.append([InlineKeyboardButton(text="\U0001F504 Yangilash", callback_data="adm:payments", style=STYLE_PRIMARY)])
    keyboard.append([InlineKeyboardButton(text="\u2B05\uFE0F Admin panelga qaytish", callback_data="adm:refresh", style=STYLE_DANGER)])

    await callback.message.edit_text("\n".join(lines), reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard))
    await callback.answer()


# ---------- Buyurtmalarni boshqarish paneli ----------

def orders_admin_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="\U0001F50D ID orqali qidirish", callback_data="adm:orders:findid", style=STYLE_PRIMARY)],
        [InlineKeyboardButton(text="\U0001F464 User ID orqali", callback_data="adm:orders:finduser", style=STYLE_PRIMARY)],
        [InlineKeyboardButton(text="\U0001F4CB Oxirgi buyurtmalar", callback_data="adm:orders:recent", style=STYLE_PRIMARY)],
        [InlineKeyboardButton(text="\u2B05\uFE0F Admin panelga qaytish", callback_data="adm:refresh", style=STYLE_DANGER)],
    ])


def orders_cancel_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="\u2B05\uFE0F Buyurtmalar bo'limiga qaytish", callback_data="adm:orders:menu", style=STYLE_DANGER)],
    ])


def _orders_compact_list(rows, header: str) -> str:
    if not rows:
        return header + "\n\nHech narsa topilmadi."
    lines = [header + "\n"]
    for row in rows:
        emoji = _STATUS_EMOJI.get(row["status"], "\u2754")
        type_label = _TYPE_LABEL.get(row["order_type"], row["order_type"])
        lines.append(f"{emoji} #{row['id']} \u2014 {type_label} \u2014 {fmt_money(row['price'])} so'm \u2014 {row['status']}")
    return "\n".join(lines)


@router_admin.callback_query(F.data == "adm:orders:menu")
async def admin_orders_menu(callback: CallbackQuery, state: FSMContext):
    if not await _is_admin(callback.from_user.id):
        await callback.answer("Ruxsat yo'q.", show_alert=True)
        return
    await state.clear()
    await callback.message.edit_text("\U0001F6D2 Buyurtmalarni boshqarish", reply_markup=orders_admin_menu())
    await callback.answer()


@router_admin.callback_query(F.data == "adm:orders:recent")
async def admin_orders_recent(callback: CallbackQuery):
    if not await _is_admin(callback.from_user.id):
        await callback.answer("Ruxsat yo'q.", show_alert=True)
        return
    rows = await get_recent_orders(limit=15)
    text = _orders_compact_list(rows, "\U0001F4CB Oxirgi 15 ta buyurtma (barcha userlar)")
    await callback.message.edit_text(text, reply_markup=orders_cancel_menu())
    await callback.answer()


@router_admin.callback_query(F.data == "adm:orders:findid")
async def admin_orders_findid_start(callback: CallbackQuery, state: FSMContext):
    if not await _is_admin(callback.from_user.id):
        await callback.answer("Ruxsat yo'q.", show_alert=True)
        return
    await _ask(callback, state, AdminPanel.order_search_id,
               "Buyurtma ID raqamini kiriting (masalan: 42):", orders_cancel_menu())


@router_admin.message(AdminPanel.order_search_id, is_free_text)
async def admin_orders_findid_receive(message: Message, state: FSMContext, bot):
    if not await _is_admin(message.from_user.id):
        return
    text = message.text.strip().lstrip("#")
    if not text.isdigit():
        await _panel_edit(bot, state, message, "Iltimos, faqat raqam yuboring.", orders_cancel_menu())
        return
    row = await get_order_row(int(text))
    await state.clear()
    if not row:
        await _panel_edit(bot, state, message, f"\u274C #{text} raqamli buyurtma topilmadi.", orders_cancel_menu())
        return
    await _panel_edit(bot, state, message, _format_order_detail(row, include_buyer=True), orders_cancel_menu())


@router_admin.callback_query(F.data == "adm:orders:finduser")
async def admin_orders_finduser_start(callback: CallbackQuery, state: FSMContext):
    if not await _is_admin(callback.from_user.id):
        await callback.answer("Ruxsat yo'q.", show_alert=True)
        return
    await _ask(callback, state, AdminPanel.order_search_user,
               "Foydalanuvchi ID raqamini kiriting:", orders_cancel_menu())


@router_admin.message(AdminPanel.order_search_user, is_free_text)
async def admin_orders_finduser_receive(message: Message, state: FSMContext, bot):
    if not await _is_admin(message.from_user.id):
        return
    text = message.text.strip()
    if not text.isdigit():
        await _panel_edit(bot, state, message, "Iltimos, faqat ID (raqam) yuboring.", orders_cancel_menu())
        return
    await state.clear()
    rows = await list_orders(int(text), limit=15)
    text_out = _orders_compact_list(rows, f"\U0001F6D2 {text} ID'li foydalanuvchining buyurtmalari")
    await _panel_edit(bot, state, message, text_out, orders_cancel_menu())


@router_admin.callback_query(F.data.startswith("adm:orders:user:"))
async def admin_orders_by_user_direct(callback: CallbackQuery):
    """Foydalanuvchi profili kartasidagi '🛒 Buyurtmalari' tugmasidan —
    ID qayta kiritilmaydi."""
    if not await _is_admin(callback.from_user.id):
        await callback.answer("Ruxsat yo'q.", show_alert=True)
        return
    target_id = int(callback.data.split(":")[3])
    rows = await list_orders(target_id, limit=15)
    text_out = _orders_compact_list(rows, f"\U0001F6D2 {target_id} ID'li foydalanuvchining buyurtmalari")
    await callback.message.edit_text(text_out, reply_markup=orders_cancel_menu())
    await callback.answer()


# ---------- Balansni to'ldirish so'rovlarini tasdiqlash / rad etish ----------

async def _finalize_topup_message(callback: CallbackQuery, suffix: str):
    """Ko'rib chiqilgan to'lov so'rovi xabarini yangilaydi. Xabar RASM
    (skrinshot) bilan kelishi mumkin (individual bildirishnoma — caption
    tahrirlanadi) yoki oddiy matn bo'lishi mumkin ("To'lovlar" ro'yxat
    panelidan — matn tahrirlanadi). Xatolik chiqsa (masalan xabar juda
    eski), adminga ta'sir qilmasligi uchun jimgina o'tkazib yuboriladi —
    callback.answer() baribir tasdiqni ko'rsatadi."""
    try:
        if callback.message.photo:
            old = callback.message.caption or ""
            await callback.message.edit_caption(caption=old + suffix)
        else:
            old = callback.message.text or ""
            await callback.message.edit_text(old + suffix)
    except Exception:
        pass


@router_admin.callback_query(F.data.startswith("topup:edit:"))
async def topup_edit_amount_start(callback: CallbackQuery, state: FSMContext, bot):
    if not await _is_admin(callback.from_user.id):
        await callback.answer("Ruxsat yo'q.", show_alert=True)
        return

    topup_id = int(callback.data.split(":")[2])
    topup = await get_topup(topup_id)
    if not topup:
        await callback.answer("So'rov topilmadi.", show_alert=True)
        return
    if topup["status"] != "pending":
        await callback.answer("Bu so'rov allaqachon ko'rib chiqilgan.", show_alert=True)
        return

    # Xabarning o'zini (rasm captioni yoki oddiy matn) va uning chat/message
    # ID'sini saqlab qo'yamiz — yangi summa kiritilgach, aynan shu xabarni
    # (Tasdiqlash/Rad etish tugmalari turgan joyni) yangilaymiz.
    await state.update_data(
        edit_topup_id=topup_id,
        edit_topup_chat_id=callback.message.chat.id,
        edit_topup_message_id=callback.message.message_id,
        edit_topup_old_text=callback.message.caption or callback.message.text or "",
        edit_topup_is_photo=bool(callback.message.photo),
    )
    await state.set_state(AdminPanel.topup_amount)
    await callback.answer()
    await callback.message.answer(
        f"\u270F\uFE0F #{topup_id} so'rovi uchun YANGI summani kiriting (so'mda).\n"
        f"Foydalanuvchi ko'rsatgan summa: {fmt_money(topup['amount'])} so'm.",
        reply_markup=admin_cancel_menu(),
    )


@router_admin.message(AdminPanel.topup_amount, is_free_text)
async def topup_edit_amount_receive(message: Message, state: FSMContext, bot):
    if not await _is_admin(message.from_user.id):
        return
    text = message.text.strip().replace(" ", "")
    if not text.isdigit() or int(text) <= 0:
        await message.answer("\u274C Noto'g'ri summa. Faqat musbat butun son kiriting.")
        return
    new_amount = int(text)

    data = await state.get_data()
    topup_id = data.get("edit_topup_id")
    chat_id = data.get("edit_topup_chat_id")
    message_id = data.get("edit_topup_message_id")
    old_text = data.get("edit_topup_old_text", "")
    is_photo = data.get("edit_topup_is_photo", False)
    await state.clear()

    if not topup_id:
        await message.answer("\u274C Xatolik: qaysi so'rov ekanligi topilmadi. Qaytadan urinib ko'ring.")
        return

    topup = await get_topup(topup_id)
    if not topup or topup["status"] != "pending":
        await message.answer("Bu so'rov endi mavjud emas yoki allaqachon ko'rib chiqilgan.")
        return

    old_amount = topup["amount"]
    updated = await set_topup_amount(topup_id, new_amount)
    if not updated:
        await message.answer(
            "\u26A0\uFE0F Bu so'rovni shu payt ichida boshqa admin allaqachon ko'rib "
            "chiqib ulgurdi — summa o'zgartirilmadi."
        )
        return

    # Bildirishnoma xabaridagi "Summa:" qatorini yangi summaga almashtiramiz,
    # avvalgi (foydalanuvchi ko'rsatgan) summani ham qavs ichida saqlab
    # qo'yamiz — shaffoflik/audit uchun.
    new_lines = []
    replaced = False
    for line in old_text.split("\n"):
        if line.startswith("\U0001F4B0 Summa:"):
            new_lines.append(
                f"\U0001F4B0 Summa: {fmt_money(new_amount)} so'm "
                f"(\u270F\uFE0F tahrirlandi, avvalgi: {fmt_money(old_amount)} so'm)"
            )
            replaced = True
        else:
            new_lines.append(line)
    new_text = "\n".join(new_lines) if replaced else old_text

    if chat_id and message_id:
        try:
            if is_photo:
                await bot.edit_message_caption(
                    chat_id=chat_id, message_id=message_id,
                    caption=new_text, reply_markup=topup_review_menu(topup_id),
                )
            else:
                await bot.edit_message_text(
                    chat_id=chat_id, message_id=message_id,
                    text=new_text, reply_markup=topup_review_menu(topup_id),
                )
        except Exception:
            pass

    await message.answer(
        f"\u2705 #{topup_id} so'rovi uchun summa {fmt_money(new_amount)} so'mga o'zgartirildi.\n"
        f"Endi yuqoridagi xabardan \u2705 Tasdiqlashingiz mumkin."
    )


@router_admin.callback_query(F.data.startswith("topup:approve:"))
async def approve_topup(callback: CallbackQuery, bot):
    if not await _is_admin(callback.from_user.id):
        await callback.answer("Ruxsat yo'q.", show_alert=True)
        return

    topup_id = int(callback.data.split(":")[2])
    topup = await get_topup(topup_id)
    if not topup:
        await callback.answer("So'rov topilmadi.", show_alert=True)
        return
    if topup["status"] != "pending":
        await callback.answer("Bu so'rov allaqachon ko'rib chiqilgan.", show_alert=True)
        return

    # Muhim: yuqoridagi tekshiruv (status != "pending") YOLG'IZ O'ZI yetarli
    # emas — ikki admin xuddi shu so'rovni deyarli bir vaqtda bossa, ikkalasi
    # ham shu tekshiruvdan "pending" holida o'tib ketishi mumkin edi. Shuning
    # uchun status yangilashning O'ZI ham shartli va atomik: faqat topup HOZIR
    # ham "pending" bo'lsagina "approved"ga o'tadi. Kimdir ulgurib bo'lgan
    # bo'lsa (masalan boshqa admin), bu False qaytaradi va balans EKKI MARTA
    # qo'shilmaydi.
    if not await set_topup_status(topup_id, "approved", expected_current="pending"):
        await callback.answer("Bu so'rovni aynan shu daqiqada boshqa admin ko'rib chiqdi.", show_alert=True)
        return

    await change_balance(topup["user_id"], topup["amount"])
    new_balance = await get_balance(topup["user_id"])

    try:
        await bot.send_message(
            topup["user_id"],
            topup_approved_text(topup["amount"], new_balance),
        )
    except Exception:
        pass

    # Referal keshbek: to'ldirgan foydalanuvchini kimdir taklif qilgan bo'lsa,
    # to'ldirilgan summaning REFERRAL_CASHBACK_PERCENT foizi o'sha kishiga
    # avtomatik qo'shiladi.
    referrer_id = await get_referrer(topup["user_id"])
    if referrer_id:
        cashback = topup["amount"] * REFERRAL_CASHBACK_PERCENT // 100
        if cashback > 0:
            await change_balance(referrer_id, cashback)
            await add_referral_earning(referrer_id, cashback)
            try:
                await bot.send_message(
                    referrer_id,
                    f"\U0001F381 Taklif qilgan do'stingiz balansini to'ldirdi!\n"
                    f"Sizga {fmt_money(cashback)} so'm keshbek qo'shildi.",
                )
            except Exception:
                pass

    await _finalize_topup_message(callback, "\n\n\u2705 TASDIQLANDI")
    await callback.answer("Tasdiqlandi \u2705")


@router_admin.callback_query(F.data.startswith("topup:reject:"))
async def reject_topup(callback: CallbackQuery, bot):
    if not await _is_admin(callback.from_user.id):
        await callback.answer("Ruxsat yo'q.", show_alert=True)
        return

    topup_id = int(callback.data.split(":")[2])
    topup = await get_topup(topup_id)
    if not topup:
        await callback.answer("So'rov topilmadi.", show_alert=True)
        return
    if topup["status"] != "pending":
        await callback.answer("Bu so'rov allaqachon ko'rib chiqilgan.", show_alert=True)
        return

    if not await set_topup_status(topup_id, "rejected", expected_current="pending"):
        await callback.answer("Bu so'rovni aynan shu daqiqada boshqa admin ko'rib chiqdi.", show_alert=True)
        return

    try:
        await bot.send_message(topup["user_id"], TOPUP_REJECTED_USER)
    except Exception:
        pass

    await _finalize_topup_message(callback, "\n\n\u274C RAD ETILDI")
    await callback.answer("Rad etildi \u274C")


# ==============================================================
# AVTO-TO'LOV (autopay) — karta orqali AVTOMATIK balans to'ldirish
# ==============================================================
# SHAXSIY (faqat egasi uchun): uchinchi tomon xizmati yo'q, sessiya faqat SIZNING serveringizda.
#
# Qanday ishlaydi:
#   1. Foydalanuvchi summani kiritadi -> unikal summa beriladi (masalan 50 000 -> 50 013).
#   2. U shu ANIQ summani kartangizga o'tkazadi.
#   3. Userbot (SIZNING Telegram akkauntingiz) @HUMOcardbot / @CardXabarBot dagi
#      "kirim" xabarini o'qiydi va summa + vaqt bo'yicha to'lovni topadi.
#   4. Balans (va referal keshbek) BITTA tranzaksiyada qo'shiladi, foydalanuvchiga xabar boradi.
#
# Render -> Environment:  API_ID, API_HASH, PAY_SESSION   (+ requirements.txt ga: kurigram>=2.2,<3.0)
# Boshqarish: /admin -> "Avto-to'lov" (faqat .env dagi ADMIN_IDS uchun; qo'shimcha adminlarga YOPIQ).
# Avto-to'lov tayyor bo'lmasa (yoqilmagan / userbot ulanmagan / karta yo'q) — foydalanuvchiga
# eski usul (chek yuborish) ko'rsatiladi, ya'ni bot hech qachon to'xtab qolmaydi.


# >>> AUTOPAY CORE BEGIN
def _ap_env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, "") or default)
    except ValueError:
        return default


API_ID = _ap_env_int("API_ID", 0)
API_HASH = os.getenv("API_HASH", "").strip()
PAY_SESSION = os.getenv("PAY_SESSION", "").strip()          # Pyrogram session string (FAQAT o'zingiz yarating)
AUTOPAY_SOURCES = [s.strip().lstrip("@") for s in
                   os.getenv("PAY_SOURCES", "HUMOcardbot,CardXabarBot").split(",") if s.strip()]
AUTOPAY_MAX_CARDS = _ap_env_int("AUTOPAY_MAX_CARDS", 10)    # admin panelda ulash mumkin bo'lgan kartalar soni

SETTINGS_KEY_AUTOPAY_ON = "autopay_enabled"
SETTINGS_KEY_AUTOPAY_TTL = "autopay_ttl_min"
AUTOPAY_DEFAULT_TTL_MIN = 20       # to'lov muddati (daqiqa) — admin panelda o'zgartiriladi
AUTOPAY_MAX_OPEN = 3               # bitta foydalanuvchida bir vaqtda ochiq to'lovlar soni
AUTOPAY_HOLD_SECONDS = 3 * 3600    # bir xil summa shuncha vaqt qayta berilmaydi; eski xabarlar e'tiborsiz
AUTOPAY_GRACE_SECONDS = 30 * 60    # muddati o'tgan to'lov shuncha vaqtdan keyin "expired" qilinadi
AUTOPAY_LEASE_BEAT = 15            # "men tirikman" belgisi va tekshiruv qadami (soniya)
AUTOPAY_LEASE_TTL = 60             # shundan keyin qulf bo'shaydi (bir vaqtda faqat BITTA nusxa ishlashi uchun)
AUTOPAY_CATCHUP_EVERY = 3          # har 3 qadamda (~45 s) zaxira tekshiruv: xabar o'tib ketmasligi uchun
AUTOPAY_HISTORY_LIMIT = 15         # har manbadan oxirgi nechta xabar tekshiriladi

class _ApFloodWait(Exception):
    """Haqiqiy FloodWait _connect() ichida (kutubxona muvaffaqiyatli import bo'lgach) shu nomga o'rnatiladi.
    Kutubxonani MODUL YUKLANGANDA import qilmaymiz: u yo'q yoki Python versiyasiga mos kelmasa ham
    bot ishga tushaveradi (faqat avto-to'lov o'chiq qoladi va admin ogohlantiriladi)."""
    value = 5


# ---------------- Xabarnoma parseri (toza funksiyalar) ----------------
_AP_BALANCE_WORDS = ("баланс", "balans", "balance", "qoldiq", "остаток", "доступно", "available")
_AP_IN_WORDS = ("пополнени", "поступлени", "зачислени", "получен", "kirim", "to'ldirish",
                "to'ldirildi", "tushum", "tushdi", "credit", "deposit", "received", "top-up", "topup")
_AP_OUT_WORDS = ("списани", "оплата", "покупк", "снятие", "платеж", "платёж", "chiqim",
                 "yechildi", "yechish", "xarid", "debit", "purchase", "withdraw")
_AP_PLUS, _AP_MINUS = "+\u2795", "-\u2212\u2013\u2796"
_AP_APOS = re.compile(r"[\u2019\u2018\u02bb\u02bc`\u00b4]")
_AP_JUNK = re.compile(r"[\ufe0f\u200b\u200c\u200d\u2060]")      # emoji variation selector, zero-width
_AP_AMOUNT_RE = re.compile(
    r"(?P<sign>[+\-\u2212\u2013\u2795\u2796])?\s*(?<!\d)"
    r"(?P<num>\d{1,3}(?:[ \u00a0\u202f.,]\d{3})+(?:[.,]\d{1,2})?|\d+(?:[.,]\d{1,2})?)"
    r"\s*(?:UZS|so'm|som|сум|сўм|sum)\b",
    re.IGNORECASE,
)
_AP_LAST4_RE = re.compile(r"[*\u2022\u25cf\u00b7]+\s?(\d{4})\b")


class ApNotification(NamedTuple):
    kind: Optional[str]      # "in" (kirim) | "out" (chiqim) | None (noaniq)
    amount: Optional[int]    # butun so'm; tiyinli bo'lsa None
    last4: tuple             # xabardagi karta oxirgi 4 raqamlari (bo'lsa)


def _ap_norm(text: str) -> str:
    return _AP_APOS.sub("'", _AP_JUNK.sub("", text or ""))


def ap_parse_amount(raw: str) -> Decimal:
    """'50 000.00' | '50,000.00' | '50.000' | '1 234,5' -> Decimal"""
    s = re.sub(r"[\s\u00a0\u202f]", "", raw)
    m = re.fullmatch(r"(.*?)(?:[.,](\d{1,2}))?", s)
    return Decimal(f"{re.sub('[.,]', '', m.group(1))}.{m.group(2) or '0'}")


def ap_parse_notification(text: str) -> Optional[ApNotification]:
    """Karta xabarnomasidan summa va yo'nalishni (kirim/chiqim) ajratadi.
    Xabarda summa bo'lmasa None qaytaradi."""
    text = _ap_norm(text)
    found, prev_end = [], 0
    for m in _AP_AMOUNT_RE.finditer(text):
        line_start = text.rfind("\n", 0, m.start()) + 1
        before = text[max(line_start, prev_end):m.start()].lower()
        prev_end = m.end()
        if any(w in before for w in _AP_BALANCE_WORDS):      # "Баланс: 1 000 000 UZS" ni o'tkazib yuboramiz
            continue
        found.append(m)
    if not found:
        return None
    m = next((x for x in found if x.group("sign")), found[0])   # belgili summa (+/-) ustun
    sign = m.group("sign")
    if sign and sign in _AP_PLUS:
        kind = "in"
    elif sign and sign in _AP_MINUS:
        kind = "out"
    else:
        low = text.lower()
        has_in = any(w in low for w in _AP_IN_WORDS)
        has_out = any(w in low for w in _AP_OUT_WORDS)
        kind = "in" if has_in and not has_out else "out" if has_out and not has_in else None
    d = ap_parse_amount(m.group("num"))
    amount = int(d) if d == d.to_integral_value() else None
    return ApNotification(kind, amount, tuple(_AP_LAST4_RE.findall(text)))


def ap_luhn_ok(number: str) -> bool:
    """Karta raqamining tekshiruv raqami (Luhn) — terish xatosini ushlaydi."""
    if not number.isdigit():
        return False
    total = 0
    for i, ch in enumerate(reversed(number)):
        d = int(ch)
        if i % 2 == 1:
            d *= 2
            if d > 9:
                d -= 9
        total += d
    return total % 10 == 0


def ap_fmt_card(number: str, mask: bool = False) -> str:
    n = re.sub(r"\D", "", number or "")
    if len(n) != 16:
        return number or ""
    if mask:
        return f"{n[:4]} **** **** {n[-4:]}"
    return f"{n[:4]} {n[4:8]} {n[8:12]} {n[12:]}"


# ---------------- Baza qatlami (Postgres va SQLite uchun BIR XIL SQL, `?` belgisi bilan) ----------------
def _ap_pg_sql(sql: str) -> str:
    n = 0

    def repl(_m):
        nonlocal n
        n += 1
        return f"${n}"

    return re.sub(r"\?", repl, sql)


class _ApTx:
    """So'rovlar (Postgres yoki SQLite). SQL matnida har doim `?` ishlatiladi."""

    def __init__(self, conn=None, db=None):
        self._conn = conn
        self._db = db

    async def fetchone(self, sql: str, *args) -> Optional[dict]:
        if self._conn is not None:
            row = await self._conn.fetchrow(_ap_pg_sql(sql), *args)
            return dict(row) if row else None
        cur = await self._db.execute(sql, args)
        row = await cur.fetchone()
        await cur.close()                            # ochiq qolgan so'rov qulf ushlab turmasin
        return dict(row) if row else None

    async def fetchall(self, sql: str, *args) -> list:
        if self._conn is not None:
            return [dict(r) for r in await self._conn.fetch(_ap_pg_sql(sql), *args)]
        cur = await self._db.execute(sql, args)
        rows = await cur.fetchall()
        await cur.close()
        return [dict(r) for r in rows]

    async def scalar(self, sql: str, *args):
        if self._conn is not None:
            return await self._conn.fetchval(_ap_pg_sql(sql), *args)
        cur = await self._db.execute(sql, args)
        row = await cur.fetchone()
        await cur.close()
        return row[0] if row else None

    async def execute(self, sql: str, *args) -> int:
        """O'zgargan qatorlar sonini qaytaradi."""
        if self._conn is not None:
            res = await self._conn.execute(_ap_pg_sql(sql), *args)
            try:
                return int(res.split()[-1])
            except (ValueError, IndexError):
                return 0
        cur = await self._db.execute(sql, args)
        n = cur.rowcount
        await cur.close()
        return n

    async def insert(self, sql: str, *args) -> int:
        """INSERT qilib, yangi qator ID'sini qaytaradi."""
        if self._conn is not None:
            return await self._conn.fetchval(_ap_pg_sql(sql) + " RETURNING id", *args)
        cur = await self._db.execute(sql, args)
        new_id = cur.lastrowid
        await cur.close()
        return new_id


@contextlib.asynccontextmanager
async def _ap_tx():
    """Yozish tranzaksiyasi: xato bo'lsa HAMMASI bekor qilinadi."""
    if _PG:
        async with _pool.acquire() as conn:
            async with conn.transaction():
                yield _ApTx(conn=conn)
    else:
        async with _db_sqlite() as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("BEGIN IMMEDIATE")    # yozuvchilarni ketma-ket tartibga soladi
            await cur.close()
            try:
                yield _ApTx(db=db)
            except BaseException:
                await db.rollback()
                raise
            await db.commit()


@contextlib.asynccontextmanager
async def _ap_conn():
    """Faqat o'qish uchun (tranzaksiyasiz)."""
    if _PG:
        async with _pool.acquire() as conn:
            yield _ApTx(conn=conn)
    else:
        async with _db_sqlite() as db:
            db.row_factory = aiosqlite.Row
            yield _ApTx(db=db)


_AP_SCHEMA_SQLITE = """
CREATE TABLE IF NOT EXISTS autopay_cards (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    number TEXT NOT NULL,
    holder TEXT,
    last4 TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 1,
    created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS autopay_payments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    base_amount INTEGER NOT NULL,
    exact_amount INTEGER NOT NULL,
    card_id INTEGER,
    card_last4 TEXT,
    status TEXT NOT NULL DEFAULT 'pending',   -- pending | paid | expired | cancelled
    created_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    paid_at INTEGER,
    event_key TEXT,
    msg_id INTEGER
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_autopay_pending_amount
    ON autopay_payments (exact_amount) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS ix_autopay_user ON autopay_payments (user_id, status);
CREATE TABLE IF NOT EXISTS autopay_events (
    event_key TEXT PRIMARY KEY,
    amount INTEGER,
    result TEXT,
    seen_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS autopay_lease (
    id INTEGER PRIMARY KEY,
    holder TEXT NOT NULL,
    beat INTEGER NOT NULL
);
"""

_AP_SCHEMA_PG = """
CREATE TABLE IF NOT EXISTS autopay_cards (
    id BIGSERIAL PRIMARY KEY,
    number TEXT NOT NULL,
    holder TEXT,
    last4 TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 1,
    created_at BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS autopay_payments (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    base_amount BIGINT NOT NULL,
    exact_amount BIGINT NOT NULL,
    card_id BIGINT,
    card_last4 TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at BIGINT NOT NULL,
    expires_at BIGINT NOT NULL,
    paid_at BIGINT,
    event_key TEXT,
    msg_id BIGINT
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_autopay_pending_amount
    ON autopay_payments (exact_amount) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS ix_autopay_user ON autopay_payments (user_id, status);
CREATE TABLE IF NOT EXISTS autopay_events (
    event_key TEXT PRIMARY KEY,
    amount BIGINT,
    result TEXT,
    seen_at BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS autopay_lease (
    id INTEGER PRIMARY KEY,
    holder TEXT NOT NULL,
    beat BIGINT NOT NULL
);
"""


async def autopay_init_db():
    if _PG:
        async with _pool.acquire() as conn:
            await conn.execute(_AP_SCHEMA_PG)
    else:
        async with _db_sqlite() as db:
            await db.executescript(_AP_SCHEMA_SQLITE)
            await db.commit()


# ---------------- Kartalar ----------------
async def ap_cards_list(active_only: bool = False) -> list:
    sql = "SELECT id, number, holder, last4, active FROM autopay_cards"
    if active_only:
        sql += " WHERE active = 1"
    async with _ap_conn() as tx:
        return await tx.fetchall(sql + " ORDER BY id")


async def ap_card_add(number: str, holder: str) -> int:
    """Karta qo'shadi. ValueError('max') — limitga yetilgan, ValueError('dup') — bunday karta bor."""
    async with _ap_tx() as tx:
        if (await tx.scalar("SELECT COUNT(*) FROM autopay_cards") or 0) >= AUTOPAY_MAX_CARDS:
            raise ValueError("max")
        if await tx.fetchone("SELECT id FROM autopay_cards WHERE number = ?", number):
            raise ValueError("dup")
        return await tx.insert(
            "INSERT INTO autopay_cards (number, holder, last4, active, created_at) VALUES (?, ?, ?, 1, ?)",
            number, holder, number[-4:], int(time.time()))


async def ap_card_toggle(card_id: int):
    async with _ap_tx() as tx:
        await tx.execute("UPDATE autopay_cards SET active = 1 - active WHERE id = ?", card_id)


async def ap_card_delete(card_id: int):
    async with _ap_tx() as tx:
        await tx.execute("DELETE FROM autopay_cards WHERE id = ?", card_id)


class ApPayment(NamedTuple):
    id: int
    amount: int          # ANIQ o'tkaziladigan summa (balansga shu summaning o'zi qo'shiladi)
    base: int            # foydalanuvchi so'ragan summa
    card_number: str
    card_holder: str
    card_last4: str
    expires_at: int      # unix vaqt
    ttl_min: int


class ApTooBusy(Exception):
    """Bo'sh unikal summa qolmadi."""


class Autopay:
    def __init__(self):
        self.bot = None
        self.client = None                 # Pyrogram userbot
        self.state = "off"                 # off | waiting | connecting | connected | error
        self.last_error = ""
        self.last_event_at = 0
        self._task = None
        self._id = uuid.uuid4().hex        # shu nusxaning belgisi (lease uchun)
        self._seen: set = set()
        self._failed: set = set()
        self._unparsed_alerts = 0

    # ---------------- holat va sozlamalar ----------------
    @property
    def configured(self) -> bool:
        return bool(API_ID and API_HASH and PAY_SESSION)

    def attach(self, bot):
        self.bot = bot

    async def init(self):
        await autopay_init_db()

    async def is_enabled(self) -> bool:
        return (await get_setting(SETTINGS_KEY_AUTOPAY_ON, default="1")) != "0"

    async def set_enabled(self, on: bool):
        await set_setting(SETTINGS_KEY_AUTOPAY_ON, "1" if on else "0")

    async def ttl_minutes(self) -> int:
        raw = await get_setting(SETTINGS_KEY_AUTOPAY_TTL, default=str(AUTOPAY_DEFAULT_TTL_MIN))
        try:
            v = int(raw)
        except (TypeError, ValueError):
            v = AUTOPAY_DEFAULT_TTL_MIN
        return min(max(v, 5), 120)

    async def is_ready(self) -> bool:
        """Foydalanuvchiga avto-to'lov ko'rsatish mumkinmi: yoqilgan + userbot ulangan + faol karta bor."""
        if self.state != "connected" or not await self.is_enabled():
            return False
        return bool(await ap_cards_list(active_only=True))

    # ---------------- ishga tushirish ----------------
    async def start(self) -> bool:
        """Darhol qaytadi (botni to'xtatib turmaydi). Userbot fonda ulanadi."""
        if not self.configured:
            self.state = "off"
            logging.warning("autopay: API_ID / API_HASH / PAY_SESSION yo'q — userbot ishga tushmadi")
            return False
        self.state = "waiting"
        self._task = asyncio.create_task(self._run())
        return True

    async def stop(self):
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        await self._disconnect()
        try:                               # qulfni darrov bo'shatamiz — yangi deploy kutib qolmasin
            async with _ap_tx() as tx:
                await tx.execute("DELETE FROM autopay_lease WHERE id = 1 AND holder = ?", self._id)
        except Exception:
            pass

    # Render yangi deployni ishga tushirganda eski nusxa hali tirik bo'ladi. Ikkalasi bir xil
    # sessiya bilan ulansa, Telegram sessiyani BUTUNLAY o'chiradi (AUTH_KEY_DUPLICATED).
    # Shu uchun "lease" (ijara): bir vaqtda faqat bitta nusxa userbotga ulanadi.
    async def _lease(self) -> bool:
        now = int(time.time())
        async with _ap_tx() as tx:
            n = await tx.execute(
                "UPDATE autopay_lease SET holder = ?, beat = ? WHERE id = 1 AND (holder = ? OR beat < ?)",
                self._id, now, self._id, now - AUTOPAY_LEASE_TTL)
            if n:
                return True
            n = await tx.execute(
                "INSERT INTO autopay_lease (id, holder, beat) VALUES (1, ?, ?) ON CONFLICT (id) DO NOTHING",
                self._id, now)
            return n > 0

    async def _run(self):
        tick, fails = 0, 0
        while True:
            try:
                if self.client is None:
                    self.state = "waiting"
                    if not await self._lease():          # boshqa nusxa ishlayapti — kutamiz
                        await asyncio.sleep(5)
                        continue
                    self.state = "connecting"
                    await self._connect()
                    tick = fails = 0
                else:
                    await asyncio.sleep(AUTOPAY_LEASE_BEAT)
                    if not await self._lease():          # ijara boshqa nusxaga o'tdi — darrov to'xtaymiz
                        logging.warning("autopay: ijara boshqa nusxaga o'tdi, userbot to'xtatildi")
                        await self._disconnect()
                        continue
                    tick += 1
                    if tick % AUTOPAY_CATCHUP_EVERY == 0:
                        await self._catch_up()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                fails += 1
                self.last_error = f"{type(e).__name__}: {e}"
                logging.exception("autopay: tsikl xatosi (%d)", fails)
                await self._disconnect()
                if fails >= 3:
                    self.state = "error"
                    await self._tell_admins(
                        f"\u274C Avto-to'lov userbot ishga tushmadi: {type(e).__name__}: {e}\n"
                        "PAY_SESSION / API_ID / API_HASH ni tekshiring (kurigram o'rnatilganmi?).")
                    return
                await asyncio.sleep(10)

    async def _connect(self):
        global _ApFloodWait
        try:
            from pyrogram import Client, filters
            from pyrogram.errors import FloodWait
            from pyrogram.handlers import MessageHandler
        except Exception as e:                 # ImportError yoki Python versiyasiga mos kelmaslik (RuntimeError...)
            raise RuntimeError(
                f"kutubxona import bo'lmadi: {e!r} — requirements.txt ga 'kurigram>=2.2,<3.0' qo'shing") from e
        _ApFloodWait = FloodWait
        client = Client("autopay", api_id=API_ID, api_hash=API_HASH,
                        session_string=PAY_SESSION, in_memory=True)
        client.add_handler(MessageHandler(
            self._on_message, filters.private & filters.incoming & filters.user(AUTOPAY_SOURCES)))
        await client.start()
        self.client = client
        self.state = "connected"
        self.last_error = ""
        await self._catch_up()
        await self._tell_admins("\u2705 Avto-to'lov userbot ishga tushdi")

    async def _disconnect(self):
        c, self.client = self.client, None
        if self.state == "connected":
            self.state = "waiting"
        if c:
            try:
                await c.stop()
            except Exception:
                pass

    # ---------------- to'lov yaratish / boshqarish ----------------
    async def create_payment(self, user_id: int, amount: int) -> ApPayment:
        """Kutilayotgan to'lov yaratadi: karta tanlaydi va UNIKAL summa beradi."""
        now = int(time.time())
        ttl = await self.ttl_minutes()
        cards = await ap_cards_list(active_only=True)
        if not cards:
            raise RuntimeError("Faol karta yo'q")
        cards_by_id = {c["id"]: c for c in cards}
        async with _ap_tx() as tx:
            await tx.execute(
                "UPDATE autopay_payments SET status = 'expired' WHERE status = 'pending' AND expires_at < ?",
                now - AUTOPAY_GRACE_SECONDS)
            open_rows = await tx.fetchall(
                "SELECT id, base_amount, exact_amount, card_id, expires_at FROM autopay_payments "
                "WHERE user_id = ? AND status = 'pending' AND expires_at > ? ORDER BY id", user_id, now)
            for r in open_rows:                        # shu summaga ochiq to'lov bo'lsa — o'shani qaytaramiz
                if r["base_amount"] == amount and r["card_id"] in cards_by_id:
                    c = cards_by_id[r["card_id"]]
                    return ApPayment(r["id"], r["exact_amount"], amount, c["number"], c["holder"] or "",
                                     c["last4"], r["expires_at"], ttl)
            while len(open_rows) >= AUTOPAY_MAX_OPEN:  # ochiqlari ko'p bo'lsa — eng eskisi bekor qilinadi
                oldest = open_rows.pop(0)
                await tx.execute("UPDATE autopay_payments SET status = 'cancelled' "
                                 "WHERE id = ? AND status = 'pending'", oldest["id"])
            loads = {r["card_id"]: r["c"] for r in await tx.fetchall(
                "SELECT card_id, COUNT(*) AS c FROM autopay_payments "
                "WHERE status = 'pending' AND expires_at > ? GROUP BY card_id", now)}
            used = {r["exact_amount"] for r in await tx.fetchall(
                "SELECT exact_amount FROM autopay_payments WHERE created_at > ? "
                "AND exact_amount BETWEEN ? AND ?", now - AUTOPAY_HOLD_SECONDS, amount, amount + 999)}
        least = min(loads.get(c["id"], 0) for c in cards)     # eng kam yuklangan kartalardan tasodifiy
        card = random.choice([c for c in cards if loads.get(c["id"], 0) == least])
        small, big = list(range(1, 100)), list(range(100, 1000))
        random.shuffle(small)
        random.shuffle(big)
        for off in small + big:
            exact = amount + off
            if exact in used:
                continue
            try:
                async with _ap_tx() as tx:
                    pid = await tx.insert(
                        "INSERT INTO autopay_payments (user_id, base_amount, exact_amount, card_id, card_last4, "
                        "status, created_at, expires_at) VALUES (?, ?, ?, ?, ?, 'pending', ?, ?)",
                        user_id, amount, exact, card["id"], card["last4"], now, now + ttl * 60)
            except (asyncpg.UniqueViolationError, sqlite3.IntegrityError) as e:
                if "unique" not in str(e).lower():
                    raise
                continue                                       # boshqa so'rov shu summani band qilgan
            return ApPayment(pid, exact, amount, card["number"], card["holder"] or "",
                             card["last4"], now + ttl * 60, ttl)
        raise ApTooBusy()

    async def attach_message(self, payment_id: int, message_id: int):
        async with _ap_tx() as tx:
            await tx.execute("UPDATE autopay_payments SET msg_id = ? WHERE id = ?", message_id, payment_id)

    async def payment_info(self, payment_id: int) -> Optional[dict]:
        async with _ap_conn() as tx:
            return await tx.fetchone(
                "SELECT id, user_id, base_amount, exact_amount, status, expires_at "
                "FROM autopay_payments WHERE id = ?", payment_id)

    async def cancel(self, payment_id: int, user_id: int) -> bool:
        async with _ap_tx() as tx:
            n = await tx.execute("UPDATE autopay_payments SET status = 'cancelled' "
                                 "WHERE id = ? AND user_id = ? AND status = 'pending'", payment_id, user_id)
        return n > 0

    async def stats_today(self) -> dict:
        now = int(time.time())
        async with _ap_conn() as tx:
            row = await tx.fetchone(
                "SELECT COUNT(*) AS c, COALESCE(SUM(exact_amount), 0) AS s FROM autopay_payments "
                "WHERE status = 'paid' AND paid_at >= ?", now - 86400)
            pend = await tx.scalar(
                "SELECT COUNT(*) FROM autopay_payments WHERE status = 'pending' AND expires_at > ?", now)
        return {"count": int(row["c"]), "sum": int(row["s"]), "pending": int(pend or 0)}

    async def recent(self, limit: int = 10) -> list:
        async with _ap_conn() as tx:
            return await tx.fetchall(
                "SELECT p.id, p.user_id, p.exact_amount, p.status, p.created_at, p.card_last4, "
                "u.username AS username FROM autopay_payments p "
                "LEFT JOIN users u ON u.user_id = p.user_id ORDER BY p.id DESC LIMIT ?", limit)

    # ---------------- balansga yozish ----------------
    async def _credit(self, tx, user_id: int, amount: int) -> dict:
        """Balans + referal keshbek — shu tranzaksiya ichida (xato bo'lsa hammasi bekor)."""
        await tx.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", amount, user_id)
        ref = await tx.fetchone("SELECT referred_by FROM users WHERE user_id = ?", user_id)
        referrer = ref["referred_by"] if ref else None
        cashback = 0
        if referrer:
            cashback = amount * REFERRAL_CASHBACK_PERCENT // 100
            if cashback > 0:
                await tx.execute(
                    "UPDATE users SET balance = balance + ?, referral_earnings = referral_earnings + ? "
                    "WHERE user_id = ?", cashback, cashback, referrer)
        return {"referrer": referrer if cashback > 0 else None, "cashback": cashback}

    async def force_paid(self, payment_id: int) -> Optional[dict]:
        """Admin qo'lda tasdiqlaydi (masalan, kechikkan to'lov). Balans ham qo'shiladi."""
        now = int(time.time())
        async with _ap_tx() as tx:
            row = await tx.fetchone(
                "SELECT id, user_id, exact_amount, msg_id FROM autopay_payments "
                "WHERE id = ? AND status <> 'paid'", payment_id)
            if not row:
                return None
            n = await tx.execute(
                "UPDATE autopay_payments SET status = 'paid', paid_at = ?, event_key = 'manual' "
                "WHERE id = ? AND status <> 'paid'", now, payment_id)
            if n == 0:
                return None
            cr = await self._credit(tx, row["user_id"], row["exact_amount"])
        info = {"id": row["id"], "user_id": row["user_id"], "amount": row["exact_amount"],
                "msg_id": row["msg_id"], **cr}
        await self._after_paid(info)
        return info

    # ---------------- xabarlarni o'qish ----------------
    async def _on_message(self, client, m):
        try:
            await self._handle(m)
        except Exception:
            logging.exception("autopay: xabarni qayta ishlashda xato")

    async def _catch_up(self):
        """Oxirgi xabarlarni qayta ko'radi: bot uxlab qolgan/uzilgan paytda kelganlar ham topiladi.
        Takroran ishlanmaydi (autopay_events)."""
        for src in AUTOPAY_SOURCES:
            if self.client is None:
                return
            try:
                msgs = [m async for m in self.client.get_chat_history(src, limit=AUTOPAY_HISTORY_LIMIT)]
            except _ApFloodWait as e:
                await asyncio.sleep(e.value + 1)
                continue
            except Exception as e:
                logging.warning("autopay: %s tarixini o'qib bo'lmadi: %r", src, e)
                continue
            for m in reversed(msgs):                 # eskisidan yangisiga
                await self._handle(m)

    async def _handle(self, m):
        u = m.from_user
        if not u or not u.is_bot or (u.username or "").lower() not in {s.lower() for s in AUTOPAY_SOURCES}:
            return                                   # faqat rasmiy karta botlari
        when = int(m.date.timestamp())
        if time.time() - when > AUTOPAY_HOLD_SECONDS:
            return
        mid = getattr(m, "id", None) or getattr(m, "message_id")
        await self._process(f"{u.username.lower()}:{mid}", m.text or m.caption or "", when)

    async def _process(self, key: str, text: str, when: int):
        if key in self._seen:
            return
        n = ap_parse_notification(text)
        if n is None or n.kind == "out":
            self._seen.add(key)
            return
        if n.kind is None or n.amount is None:
            self._seen.add(key)
            await self._alert_unparsed(text)
            return
        try:
            status, data = await self._match(key, n.amount, when, n.last4)
        except Exception as e:
            logging.exception("autopay: %s ni qayta ishlashda xato", key)
            if key not in self._failed:              # xato bo'lsa keyingi aylanishda qayta uriniladi
                self._failed.add(key)
                await self._tell_admins(
                    f"\u274C Avto-to'lovda xato ({fmt_money(n.amount)} so'm): {type(e).__name__}: {e}")
            return
        if len(self._seen) > 2000:
            self._seen.clear()
        self._seen.add(key)
        self.last_event_at = int(time.time())
        if status == "paid":
            await self._after_paid(data)
        elif status == "late":
            markup = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="\u2705 Balansga qo'shish", callback_data=f"apay:force:{data['id']}",
                                     style=STYLE_SUCCESS)]])
            await self._tell_admins(
                f"\u23F0 Kechikkan yoki boshqa kartaga tushgan to'lov\n"
                f"\U0001F4B0 Kelgan summa: {fmt_money(n.amount)} so'm\n"
                f"\U0001F522 So'rov #{data['id']} (ID: {data['user_id']}) — muddati o'tgan yoki karta mos emas.\n"
                f"Pul haqiqatan tushgan bo'lsa, tugmani bosing.", markup)
        # status == "dup" yoki "none": jim (begona kirim — sizning shaxsiy pulingiz bo'lishi mumkin)

    async def _match(self, key: str, amount: int, when: int, last4: tuple):
        """Bitta tranzaksiya: hodisani yozish + to'lovni topish + balansni qo'shish."""
        now = int(time.time())
        async with _ap_tx() as tx:
            n = await tx.execute(
                "INSERT INTO autopay_events (event_key, amount, result, seen_at) "
                "VALUES (?, ?, 'processing', ?) ON CONFLICT (event_key) DO NOTHING", key, amount, now)
            if n == 0:
                return "dup", None                         # bu xabar allaqachon ishlangan
            row = await tx.fetchone(
                "SELECT id, user_id, exact_amount, card_last4, msg_id FROM autopay_payments "
                "WHERE status = 'pending' AND exact_amount = ? "
                "AND created_at - 30 <= ? AND ? <= expires_at + 120 ORDER BY id LIMIT 1",
                amount, when, when)
            if row and row["card_last4"] and last4 and row["card_last4"] not in last4:
                row = None                                 # boshqa kartaga tushgan — bu to'lov emas
            if row:
                n = await tx.execute(
                    "UPDATE autopay_payments SET status = 'paid', paid_at = ?, event_key = ? "
                    "WHERE id = ? AND status = 'pending'", now, key, row["id"])
                if n == 0:
                    return "dup", None
                cr = await self._credit(tx, row["user_id"], row["exact_amount"])
                await tx.execute("UPDATE autopay_events SET result = 'matched' WHERE event_key = ?", key)
                return "paid", {"id": row["id"], "user_id": row["user_id"], "amount": row["exact_amount"],
                                "msg_id": row["msg_id"], **cr}
            late = await tx.fetchone(
                "SELECT id, user_id, exact_amount FROM autopay_payments "
                "WHERE exact_amount = ? AND status <> 'paid' AND created_at > ? ORDER BY id DESC LIMIT 1",
                amount, now - AUTOPAY_HOLD_SECONDS)
            await tx.execute("UPDATE autopay_events SET result = ? WHERE event_key = ?",
                             "late" if late else "unmatched", key)
            if late:
                return "late", {"id": late["id"], "user_id": late["user_id"], "amount": late["exact_amount"]}
            return "none", None

    async def _after_paid(self, info: dict):
        """Tranzaksiya yakunlangach: foydalanuvchi, referrer va adminlarga xabar."""
        uid, amount, pid = info["user_id"], info["amount"], info["id"]
        balance = await get_balance(uid)
        if self.bot:
            if info.get("msg_id"):
                try:
                    await self.bot.edit_message_text(
                        "\u2705 To'lov qabul qilindi.", chat_id=uid, message_id=info["msg_id"],
                        reply_markup=InlineKeyboardMarkup(inline_keyboard=[]))
                except Exception:
                    pass
            try:
                await self.bot.send_message(uid, topup_approved_text(amount, balance), reply_markup=main_menu())
            except Exception:
                pass
            if info.get("referrer") and info.get("cashback"):
                try:
                    await self.bot.send_message(
                        info["referrer"],
                        f"\U0001F381 Taklif qilgan do'stingiz balansini to'ldirdi!\n"
                        f"Sizga {fmt_money(info['cashback'])} so'm keshbek qo'shildi.")
                except Exception:
                    pass
        await self._tell_admins(f"\u2705 Avto-to'lov #{pid}: +{fmt_money(amount)} so'm (ID: {uid})")

    # ---------------- yordamchilar ----------------
    async def _tell_admins(self, text: str, markup=None):
        """Faqat asosiy adminlarga (.env ADMIN_IDS)."""
        if not self.bot:
            return
        for admin_id in ADMIN_IDS:
            try:
                await self.bot.send_message(admin_id, text, reply_markup=markup)
            except Exception:
                pass

    async def _alert_unparsed(self, text: str):
        """Karta xabari ko'rinishida, lekin tushunilmadi — adminga ko'rsatamiz (parserni moslash uchun)."""
        if self._unparsed_alerts >= 5 or not re.search(r"\d.*(uzs|so'm|сум)", _ap_norm(text).lower(), re.S):
            return
        self._unparsed_alerts += 1
        await self._tell_admins(
            "\u26A0\uFE0F Tushunilmagan karta xabari (kirim/chiqim aniqlanmadi):\n\n" + text[:600])

    async def debug_recent(self, per_source: int = 4):
        """Admin uchun: karta botlaridagi oxirgi xabarlar va parser ularni qanday tushunganini ko'rsatadi."""
        if self.client is None:
            return None
        out = []
        for src in AUTOPAY_SOURCES:
            try:
                msgs = [m async for m in self.client.get_chat_history(src, limit=per_source)]
            except Exception as e:
                out.append({"src": src, "error": repr(e)})
                continue
            for m in msgs:
                u = m.from_user
                if not u or not u.is_bot:
                    continue
                text = m.text or m.caption or ""
                out.append({"src": src, "ts": int(m.date.timestamp()), "text": text,
                            "parsed": ap_parse_notification(text)})
        return out


AUTOPAY = Autopay()
# <<< AUTOPAY CORE END


# ---------------- Matnlar va tugmalar ----------------
def _ap_time(ts: int) -> str:
    """Toshkent vaqti (UTC+5) bo'yicha 'kun.oy soat:daqiqa'."""
    return datetime.fromtimestamp(ts, tz=timezone(timedelta(hours=5))).strftime("%d.%m %H:%M")


def autopay_text(p: ApPayment) -> str:
    extra = p.amount - p.base
    lines = [
        "\U0001F4B3 Balansni to'ldirish",
        "",
        "Quyidagi kartaga ANIQ shu summani o'tkazing:",
        f"\U0001F4B0 {fmt_money(p.amount)} so'm",
        "",
        f"\U0001F4B3 {ap_fmt_card(p.card_number)}",
    ]
    if p.card_holder:
        lines.append(f"\U0001F464 {p.card_holder}")
    lines += [
        "",
        f"\u23F3 {p.ttl_min} daqiqa ichida o'tkazing. To'lov AVTOMATIK aniqlanadi va "
        f"balansingizga {fmt_money(p.amount)} so'm qo'shiladi.",
        f"\u26A0\uFE0F Summa aynan shunday bo'lishi shart: {extra} so'm qo'shimcha to'lovni "
        f"aniqlash uchun (u ham balansingizga qo'shiladi).",
    ]
    return "\n".join(lines)


def autopay_menu(p: ApPayment) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="\U0001F4B3 Karta raqamini nusxalash",
                              copy_text=CopyTextButton(text=re.sub(r"\D", "", p.card_number) or p.card_number))],
        [InlineKeyboardButton(text="\U0001F4B0 Summani nusxalash", copy_text=CopyTextButton(text=str(p.amount)))],
        [InlineKeyboardButton(text="\U0001F504 To'lovni tekshirish", callback_data=f"apay:check:{p.id}",
                              style=STYLE_SUCCESS)],
        [InlineKeyboardButton(text="\U0001F4F8 Chek yuborish (qo'lda)", callback_data=f"apay:manual:{p.id}",
                              style=STYLE_PRIMARY)],
        [InlineKeyboardButton(text=BTN_CANCEL, callback_data=f"apay:cancel:{p.id}", style=STYLE_DANGER)],
    ])


async def _autopay_offer(message: Message, state: FSMContext, amount: int) -> bool:
    """Avto-to'lov tayyor bo'lsa — foydalanuvchiga unikal summa va kartani ko'rsatadi (True).
    Tayyor bo'lmasa yoki xato bo'lsa False qaytaradi — eski (chek yuborish) oqimi davom etadi."""
    try:
        if not await AUTOPAY.is_ready():
            return False
        pay = await AUTOPAY.create_payment(message.from_user.id, amount)
    except Exception:
        logging.exception("autopay: to'lov yaratib bo'lmadi — qo'lda oqimga o'tildi")
        return False
    await state.clear()
    sent = await message.answer(autopay_text(pay), reply_markup=autopay_menu(pay))
    try:
        await AUTOPAY.attach_message(pay.id, sent.message_id)
    except Exception:
        logging.exception("autopay: xabar ID'sini saqlab bo'lmadi")
    return True


def _is_owner(user_id: int) -> bool:
    """Avto-to'lov bo'limi FAQAT .env dagi (asosiy) adminlar uchun — qo'shimcha adminlarga yopiq."""
    return user_id in ADMIN_IDS


# ---------------- Foydalanuvchi tugmalari (balans oqimi) ----------------
@router_balance.callback_query(F.data.startswith("apay:check:"))
async def autopay_user_check(callback: CallbackQuery):
    try:
        pid = int(callback.data.split(":")[2])
    except (IndexError, ValueError):
        await callback.answer()
        return
    info = await AUTOPAY.payment_info(pid)
    if not info or info["user_id"] != callback.from_user.id:
        await callback.answer("So'rov topilmadi.", show_alert=True)
        return
    if info["status"] == "paid":
        await callback.answer("\u2705 To'lov qabul qilingan, balansingiz to'ldirilgan.", show_alert=True)
    elif info["status"] == "pending" and info["expires_at"] > time.time():
        left = max(1, int((info["expires_at"] - time.time()) // 60))
        await callback.answer(
            f"\u23F3 Hali tushmadi. Qolgan vaqt: ~{left} daqiqa.\n"
            f"Pul o'tkazgan bo'lsangiz, u tushishi bilan balans avtomatik to'ldiriladi.", show_alert=True)
    else:
        await callback.answer(
            "\u231B Bu so'rovning muddati tugagan yoki bekor qilingan. Pul o'tkazgan bo'lsangiz — "
            "\"Chek yuborish\" tugmasi orqali chekni yuboring yoki adminga yozing.", show_alert=True)


@router_balance.callback_query(F.data.startswith("apay:cancel:"))
async def autopay_user_cancel(callback: CallbackQuery, state: FSMContext):
    try:
        pid = int(callback.data.split(":")[2])
    except (IndexError, ValueError):
        pid = 0
    if pid:
        await AUTOPAY.cancel(pid, callback.from_user.id)
    await state.clear()
    try:
        await callback.message.edit_text("\u274C Bekor qilindi.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[]))
    except Exception:
        try:
            await callback.message.delete()
        except Exception:
            pass
    await callback.message.answer("\U0001F3E0 Asosiy menyu.", reply_markup=main_menu())
    await callback.answer()


@router_balance.callback_query(F.data.startswith("apay:manual:"))
async def autopay_user_manual(callback: CallbackQuery, state: FSMContext):
    """Foydalanuvchi avto-to'lov o'rniga eski usulni (chek yuborish) tanladi."""
    try:
        pid = int(callback.data.split(":")[2])
    except (IndexError, ValueError):
        await callback.answer()
        return
    info = await AUTOPAY.payment_info(pid)
    if not info or info["user_id"] != callback.from_user.id:
        await callback.answer("So'rov topilmadi.", show_alert=True)
        return
    if info["status"] == "paid":
        await callback.answer("\u2705 To'lov allaqachon qabul qilingan.", show_alert=True)
        return
    await AUTOPAY.cancel(pid, callback.from_user.id)
    amount = info["base_amount"]
    await state.update_data(amount=amount)
    await state.set_state(TopUp.photo)
    card_number, card_holder = await get_card_info()
    await callback.message.edit_text(
        topup_instructions(amount, card_number, card_holder), reply_markup=_card_copy_menu(card_number))
    await callback.answer()


# ---------------- Admin panel: "Avto-to'lov" (FAQAT egasi) ----------------
_AP_STATE_LABELS = {
    "off": "\u274C sozlanmagan (API_ID / API_HASH / PAY_SESSION yo'q)",
    "waiting": "\u23F3 navbat kutmoqda (boshqa nusxa ishlayapti yoki ulanmagan)",
    "connecting": "\u23F3 ulanmoqda...",
    "connected": "\u2705 ulangan",
    "error": "\u274C xato (loglarga qarang)",
}


def _ap_back_menu(callback_data: str, text: str = "\u2B05\uFE0F Orqaga") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=text, callback_data=callback_data, style=STYLE_DANGER)],
    ])


async def _ap_edit(callback: CallbackQuery, text: str, markup):
    try:
        await callback.message.edit_text(text, reply_markup=markup)
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e).lower():
            raise


async def _ap_panel_text() -> str:
    cards = await ap_cards_list()
    on = await AUTOPAY.is_enabled()
    ttl = await AUTOPAY.ttl_minutes()
    st = await AUTOPAY.stats_today()
    on_label = "\U0001F7E2 yoqilgan" if on else "\U0001F534 o'chirilgan"
    lines = [
        "\U0001F916 Avto-to'lov (karta)",
        "",
        f"Holat: {on_label}",
        f"Userbot: {_AP_STATE_LABELS.get(AUTOPAY.state, AUTOPAY.state)}",
        f"To'lov muddati: {ttl} daqiqa",
        f"Kartalar: {len(cards)}/{AUTOPAY_MAX_CARDS}",
    ]
    if AUTOPAY.state == "error" and AUTOPAY.last_error:
        lines.append(f"Oxirgi xato: {AUTOPAY.last_error[:200]}")
    lines += [
        "",
        f"So'nggi 24 soat: {st['count']} ta to'lov, {fmt_money(st['sum'])} so'm",
        f"Kutilayotgan: {st['pending']} ta",
    ]
    if AUTOPAY.last_event_at:
        mins = max(0, int((time.time() - AUTOPAY.last_event_at) // 60))
        lines.append(f"Oxirgi kirim: {mins} daqiqa oldin")
    if not (await AUTOPAY.is_ready()):
        lines += ["", "\u26A0\uFE0F Hozir foydalanuvchilarga eski usul (chek yuborish) ko'rsatiladi: "
                      "avto-to'lov uchun yoqilgan + userbot ulangan + kamida 1 ta faol karta kerak."]
    if not _PG:
        lines += ["", "\u26A0\uFE0F Baza: SQLite. Render'da redeploy paytida eski va yangi nusxa ALOHIDA bazada "
                      "ishlaydi va userbot sessiyasi buzilishi mumkin \u2014 DATABASE_URL (Postgres) ishlatish tavsiya etiladi."]
    return "\n".join(lines)


async def _ap_panel_menu() -> InlineKeyboardMarkup:
    on = await AUTOPAY.is_enabled()
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="\U0001F534 O'chirish" if on else "\U0001F7E2 Yoqish",
                              callback_data="adm:apay:toggle", style=STYLE_DANGER if on else STYLE_SUCCESS)],
        [InlineKeyboardButton(text="\U0001F4B3 Kartalar", callback_data="adm:apay:cards", style=STYLE_PRIMARY),
         InlineKeyboardButton(text="\U0001F4CB So'nggi to'lovlar", callback_data="adm:apay:recent", style=STYLE_PRIMARY)],
        [InlineKeyboardButton(text="\U0001F9EA Oxirgi xabarlar", callback_data="adm:apay:debug", style=STYLE_PRIMARY),
         InlineKeyboardButton(text="\u23F1 Muddat", callback_data="adm:apay:ttl", style=STYLE_PRIMARY)],
        [InlineKeyboardButton(text="\U0001F504 Yangilash", callback_data="adm:apay", style=STYLE_PRIMARY)],
        [InlineKeyboardButton(text="\u2B05\uFE0F Orqaga", callback_data="adm:refresh", style=STYLE_DANGER)],
    ])


def _ap_cards_text(cards: list) -> str:
    lines = [f"\U0001F4B3 Kartalar ({len(cards)}/{AUTOPAY_MAX_CARDS})", ""]
    if not cards:
        lines.append("\u2014 Hozircha karta qo'shilmagan.")
    for i, c in enumerate(cards, 1):
        mark = "\u2705" if c["active"] else "\u23F8"
        holder = f" \u2014 {c['holder']}" if c["holder"] else ""
        lines.append(f"{i}. {ap_fmt_card(c['number'], mask=True)}{holder} {mark}")
    lines += [
        "",
        "Muhim: karta @HUMOcardbot yoki @CardXabarBot ga (o'sha Telegram akkauntda) ulangan bo'lishi kerak — "
        "aks holda to'lov avtomatik aniqlanmaydi. Bitta telefon raqamga bog'langan bir nechta karta "
        "ulash mumkin.",
    ]
    return "\n".join(lines)


def _ap_cards_menu(cards: list) -> InlineKeyboardMarkup:
    rows = []
    for c in cards:
        toggle_label = "\u23F8 O'chirib qo'yish" if c["active"] else "\u25B6\uFE0F Yoqish"
        rows.append([
            InlineKeyboardButton(
                text=f"{toggle_label} ...{c['last4']}",
                callback_data=f"adm:apay:card:tg:{c['id']}", style=STYLE_PRIMARY),
            InlineKeyboardButton(text=f"\U0001F5D1 ...{c['last4']}", callback_data=f"adm:apay:card:del:{c['id']}",
                                 style=STYLE_DANGER),
        ])
    if len(cards) < AUTOPAY_MAX_CARDS:
        rows.append([InlineKeyboardButton(text="\u2795 Karta qo'shish", callback_data="adm:apay:card:add",
                                          style=STYLE_PRIMARY)])
    rows.append([InlineKeyboardButton(text="\u2B05\uFE0F Orqaga", callback_data="adm:apay", style=STYLE_DANGER)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def _ap_show_cards(callback: CallbackQuery):
    cards = await ap_cards_list()
    await _ap_edit(callback, _ap_cards_text(cards), _ap_cards_menu(cards))


@router_admin.callback_query(F.data == "adm:apay")
async def ap_admin_panel(callback: CallbackQuery, state: FSMContext):
    if not _is_owner(callback.from_user.id):
        await callback.answer("Bu bo'lim faqat asosiy admin uchun.", show_alert=True)
        return
    await state.clear()
    await _ap_edit(callback, await _ap_panel_text(), await _ap_panel_menu())
    await callback.answer()


@router_admin.callback_query(F.data == "adm:apay:toggle")
async def ap_admin_toggle(callback: CallbackQuery):
    if not _is_owner(callback.from_user.id):
        await callback.answer("Ruxsat yo'q.", show_alert=True)
        return
    await AUTOPAY.set_enabled(not await AUTOPAY.is_enabled())
    await _ap_edit(callback, await _ap_panel_text(), await _ap_panel_menu())
    await callback.answer("Saqlandi.")


@router_admin.callback_query(F.data == "adm:apay:cards")
async def ap_admin_cards(callback: CallbackQuery, state: FSMContext):
    if not _is_owner(callback.from_user.id):
        await callback.answer("Ruxsat yo'q.", show_alert=True)
        return
    await state.clear()
    await _ap_show_cards(callback)
    await callback.answer()


@router_admin.callback_query(F.data.startswith("adm:apay:card:tg:"))
async def ap_admin_card_toggle(callback: CallbackQuery):
    if not _is_owner(callback.from_user.id):
        await callback.answer("Ruxsat yo'q.", show_alert=True)
        return
    try:
        await ap_card_toggle(int(callback.data.split(":")[4]))
    except (IndexError, ValueError):
        pass
    await _ap_show_cards(callback)
    await callback.answer("Saqlandi.")


@router_admin.callback_query(F.data.startswith("adm:apay:card:del:"))
async def ap_admin_card_delete(callback: CallbackQuery):
    if not _is_owner(callback.from_user.id):
        await callback.answer("Ruxsat yo'q.", show_alert=True)
        return
    try:
        await ap_card_delete(int(callback.data.split(":")[4]))
    except (IndexError, ValueError):
        pass
    await _ap_show_cards(callback)
    await callback.answer("O'chirildi.")


@router_admin.callback_query(F.data == "adm:apay:card:add")
async def ap_admin_card_add_start(callback: CallbackQuery, state: FSMContext):
    if not _is_owner(callback.from_user.id):
        await callback.answer("Ruxsat yo'q.", show_alert=True)
        return
    if len(await ap_cards_list()) >= AUTOPAY_MAX_CARDS:
        await callback.answer(f"Limit: {AUTOPAY_MAX_CARDS} ta karta.", show_alert=True)
        return
    await _ask(
        callback, state, AdminPanel.apay_card_number,
        "\U0001F4B3 Karta raqamini yuboring (16 ta raqam). Foydalanuvchilarga shu raqam ko'rsatiladi — "
        "diqqat bilan kiriting!\n\nMasalan: 8600 1234 5678 9012",
        _ap_back_menu("adm:apay:cards", "\u2B05\uFE0F Kartalarga qaytish"))


@router_admin.message(AdminPanel.apay_card_number, is_free_text)
async def ap_admin_card_number(message: Message, state: FSMContext, bot):
    if not _is_owner(message.from_user.id):
        return
    back = _ap_back_menu("adm:apay:cards", "\u2B05\uFE0F Kartalarga qaytish")
    digits = re.sub(r"\D", "", message.text or "")
    if len(digits) != 16:
        await _panel_edit(bot, state, message,
                          "\u274C Karta raqami 16 ta raqamdan iborat bo'lishi kerak. Qayta kiriting.", back)
        return
    data = await state.get_data()
    if not ap_luhn_ok(digits) and data.get("ap_unchecked") != digits:
        await state.update_data(ap_unchecked=digits)
        await _panel_edit(
            bot, state, message,
            "\u26A0\uFE0F Bu raqam tekshiruvdan o'tmadi \u2014 xato terilgan bo'lishi mumkin. Raqamni kartadan "
            "qayta tekshiring. Agar aniq to'g'ri bo'lsa, xuddi shu raqamni yana bir marta yuboring.", back)
        return
    await state.update_data(ap_number=digits)
    await state.set_state(AdminPanel.apay_card_holder)
    await _panel_edit(bot, state, message,
                      "\U0001F464 Karta egasining ismini yuboring (masalan: ALISHER A.). "
                      "Ko'rsatmaslik uchun \"-\" yuboring.", back)


@router_admin.message(AdminPanel.apay_card_holder, is_free_text)
async def ap_admin_card_holder(message: Message, state: FSMContext, bot):
    if not _is_owner(message.from_user.id):
        return
    data = await state.get_data()
    number = data.get("ap_number", "")
    holder = (message.text or "").strip()
    holder = "" if holder == "-" else holder[:60]
    try:
        await ap_card_add(number, holder)
    except ValueError as e:
        reason = ("\u274C Limitga yetildi." if str(e) == "max" else "\u274C Bu karta allaqachon qo'shilgan.")
        cards = await ap_cards_list()
        await _panel_edit(bot, state, message, reason + "\n\n" + _ap_cards_text(cards), _ap_cards_menu(cards))
        await state.clear()
        return
    cards = await ap_cards_list()
    await _panel_edit(bot, state, message, "\u2705 Karta qo'shildi.\n\n" + _ap_cards_text(cards), _ap_cards_menu(cards))
    await state.clear()


@router_admin.callback_query(F.data == "adm:apay:ttl")
async def ap_admin_ttl_start(callback: CallbackQuery, state: FSMContext):
    if not _is_owner(callback.from_user.id):
        await callback.answer("Ruxsat yo'q.", show_alert=True)
        return
    ttl = await AUTOPAY.ttl_minutes()
    await _ask(
        callback, state, AdminPanel.apay_ttl,
        f"\u23F1 Hozirgi to'lov muddati: {ttl} daqiqa.\n\nYangi muddatni daqiqada yuboring (5 dan 120 gacha).",
        _ap_back_menu("adm:apay"))


@router_admin.message(AdminPanel.apay_ttl, is_free_text)
async def ap_admin_ttl_receive(message: Message, state: FSMContext, bot):
    if not _is_owner(message.from_user.id):
        return
    text = (message.text or "").strip()
    if not text.isdigit() or not (5 <= int(text) <= 120):
        await _panel_edit(bot, state, message, "\u274C 5 dan 120 gacha son yuboring.", _ap_back_menu("adm:apay"))
        return
    await set_setting(SETTINGS_KEY_AUTOPAY_TTL, text)
    await _panel_edit(bot, state, message, await _ap_panel_text(), await _ap_panel_menu())
    await state.clear()


@router_admin.callback_query(F.data == "adm:apay:recent")
async def ap_admin_recent(callback: CallbackQuery):
    if not _is_owner(callback.from_user.id):
        await callback.answer("Ruxsat yo'q.", show_alert=True)
        return
    rows = await AUTOPAY.recent(10)
    labels = {"paid": "\u2705", "pending": "\u23F3", "expired": "\u231B", "cancelled": "\u274C"}
    lines = ["\U0001F4CB So'nggi avto-to'lovlar", ""]
    if not rows:
        lines.append("\u2014 Hali yo'q.")
    for r in rows:
        who = f"@{r['username']}" if r.get("username") else str(r["user_id"])
        lines.append(f"{labels.get(r['status'], '?')} #{r['id']} {fmt_money(r['exact_amount'])} so'm \u2014 {who} "
                     f"({_ap_time(r['created_at'])})")
    await _ap_edit(callback, "\n".join(lines), _ap_back_menu("adm:apay"))
    await callback.answer()


@router_admin.callback_query(F.data == "adm:apay:debug")
async def ap_admin_debug(callback: CallbackQuery):
    """Karta botlaridagi oxirgi xabarlar va parser ularni qanday tushunganini ko'rsatadi."""
    if not _is_owner(callback.from_user.id):
        await callback.answer("Ruxsat yo'q.", show_alert=True)
        return
    rows = await AUTOPAY.debug_recent(4)
    if rows is None:
        text = "\U0001F9EA Userbot ulanmagan \u2014 xabarlarni o'qib bo'lmaydi."
    else:
        lines = ["\U0001F9EA Oxirgi xabarlar (parser natijasi bilan)", ""]
        shown = 0
        for r in rows:
            if shown >= 6:
                break
            if r.get("error"):
                lines.append(f"\u2022 {r['src']}: o'qib bo'lmadi ({r['error'][:80]})")
                continue
            p = r["parsed"]
            kind = {"in": "KIRIM", "out": "chiqim", None: "?"}[p.kind] if p else "summa topilmadi"
            extra = f" {fmt_money(p.amount)} so'm" if p and p.amount is not None else ""
            l4 = f" karta:{','.join(p.last4)}" if p and p.last4 else ""
            raw = " | ".join(x.strip() for x in r["text"].splitlines() if x.strip())[:170]
            lines.append(f"\u2022 {r['src']} {_ap_time(r['ts'])} \u2192 {kind}{extra}{l4}\n  {raw}")
            shown += 1
        if shown == 0:
            lines.append("\u2014 Karta botlaridan xabar topilmadi (botga ulanganmisiz?).")
        text = "\n".join(lines)
    await _ap_edit(callback, text[:4000], _ap_back_menu("adm:apay"))
    await callback.answer()


@router_admin.callback_query(F.data.startswith("apay:force:"))
async def ap_admin_force(callback: CallbackQuery):
    """Kechikkan to'lovni admin qo'lda balansga qo'shadi."""
    if not _is_owner(callback.from_user.id):
        await callback.answer("Ruxsat yo'q.", show_alert=True)
        return
    try:
        pid = int(callback.data.split(":")[2])
    except (IndexError, ValueError):
        await callback.answer()
        return
    info = await AUTOPAY.force_paid(pid)
    if not info:
        await callback.answer("Topilmadi yoki allaqachon to'langan.", show_alert=True)
        return
    try:
        await callback.message.edit_text((callback.message.text or "") + "\n\n\u2705 QO'SHILDI",
                                         reply_markup=InlineKeyboardMarkup(inline_keyboard=[]))
    except Exception:
        pass
    await callback.answer("Balansga qo'shildi \u2705")


# ==============================================================
# MIDDLEWARE VA ISHGA TUSHIRISH (main)
# ==============================================================
class BanMiddleware(BaseMiddleware):
    """Bloklangan foydalanuvchining hech qanday xabari/tugma bosishi
    handlerlarga yetib bormaydi — jimgina e'tiborsiz qoldiriladi."""

    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any],
    ) -> Any:
        user = data.get("event_from_user")
        if user and await is_banned(user.id):
            return None
        return await handler(event, data)


class ForceSubMiddleware(BaseMiddleware):
    """Majburiy obuna kanali sozlangan bo'lsa (admin panel yoki .env orqali),
    foydalanuvchi shu kanalga a'zo bo'lmaguncha botning boshqa hech qanday
    funksiyasidan foydalana olmaydi — "✅ A'zo bo'ldim" tugmasi va adminlar
    uchun bu tekshiruv chetlab o'tiladi.
    """

    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any],
    ) -> Any:
        user = data.get("event_from_user")
        if not user:
            return await handler(event, data)

        if isinstance(event, CallbackQuery) and event.data == "forcesub:check":
            return await handler(event, data)

        # Avval kanallar sozlanganmi shuni tekshiramiz (bitta baza so'rovi) — bu
        # ko'pchilik holatda (majburiy obuna o'chiq) darhol chiqib ketadi,
        # adminlikni tekshirish uchun QO'SHIMCHA baza so'rovi yubormaydi.
        channels = await get_force_sub_channels()
        if not channels:
            return await handler(event, data)

        # /start (referal payload bilan bo'lishi ham mumkin: "/start ref123")
        # har doim cmd_start'ga yetkaziladi — Telegram start-payload'ni FAQAT
        # shu birinchi xabarda beradi, qayta yubormaydi. Agar shu yerda
        # to'xtatib qo'yilsa, foydalanuvchi keyinroq "✅ A'zo bo'ldim" bossa
        # ham referal ID abadiy yo'qolib qolar edi. cmd_start endi
        # ensure_user() (va referalni saqlashni) obunadan QAT'I NAZAR eng
        # avval bajaradi, so'ng kerak bo'lsa majburiy obuna xabarini o'zi
        # ko'rsatadi — shuning uchun bu yerda uni bloklash shart emas.
        # (`words and` tekshiruvi shart: bo'sh yoki faqat bo'sh joylardan
        # iborat xabar matnida .split() bo'sh ro'yxat qaytaradi, tekshirmasa
        # words[0] IndexError berardi.)
        if isinstance(event, Message) and event.text:
            words = event.text.split()
            if words and words[0].split("@")[0] == "/start":
                return await handler(event, data)

        if await _is_admin(user.id):
            return await handler(event, data)

        bot = data.get("bot")
        if bot and not await is_subscribed(bot, user.id):
            chat_id = event.chat.id if isinstance(event, Message) else (event.message.chat.id if event.message else user.id)
            await send_force_sub_prompt(bot, chat_id)
            if isinstance(event, CallbackQuery):
                await event.answer()
            return None

        return await handler(event, data)


class ThrottleMiddleware(BaseMiddleware):
    """Bitta foydalanuvchidan THROTTLE_SECONDS ichida kelgan ortiqcha
    so'rovlarni e'tiborsiz qoldiradi (spam/flood himoyasi). Xotirada
    saqlanadi — bot qayta ishga tushsa tozalanadi, bu muhim emas."""

    def __init__(self, min_interval: float = THROTTLE_SECONDS):
        self.min_interval = min_interval
        self._last_seen: Dict[int, float] = {}

    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any],
    ) -> Any:
        user = data.get("event_from_user")
        if user:
            now = time.monotonic()
            last = self._last_seen.get(user.id, 0.0)
            if now - last < self.min_interval:
                if isinstance(event, CallbackQuery):
                    await event.answer()
                return None
            self._last_seen[user.id] = now
        return await handler(event, data)


async def _health(request):
    return web.Response(text="Bot ishlayapti \u2705")


async def _run_health_server():
    """Render 'Web Service' turi deploy paytida kamida bitta ochiq portni talab
    qiladi (aks holda 'port scan timeout' bilan servisni o'chirib qo'yadi),
    lekin bot faqat polling orqali ishlaydi va o'zidan HTTP so'rov kutmaydi.
    Shuning uchun shu yengil health-check server ochib qo'yiladi — u haqiqiy
    trafik uchun emas, faqat Render'ning port tekshiruvidan o'tish uchun kerak.
    """
    app = web.Application()
    app.router.add_get("/", _health)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.getenv("PORT", "10000"))
    site = web.TCPSite(runner, host="0.0.0.0", port=port)
    await site.start()
    logging.info(f"Health-check server {port}-portda ochildi")


async def global_error_handler(event: ErrorEvent) -> bool:
    """
    Hech qaysi handler o'zi ushlamagan (kutilmagan) xatoliklar uchun oxirgi
    xavfsizlik tarmog'i. Bu bo'lmasa: biror funksiya ichida kutilmagan
    xatolik chiqsa (masalan API kutilmagan formatda javob qaytarsa),
    callback HECH QACHON javob olmasdi — foydalanuvchi tugmani bossa, u
    "yuklanmoqda" holatida abadiy osilib qolardi va hech qanday xabar
    ko'rsatilmasdi. Endi bunday holatda foydalanuvchiga kamida tushunarli
    xabar chiqadi, xatolikning o'zi esa logga (server konsoliga) yoziladi.
    """
    logging.error(f"Kutilmagan xatolik: {event.exception!r}", exc_info=event.exception)
    try:
        update = event.update
        if update.callback_query:
            await update.callback_query.answer(
                "\u274C Kutilmagan xatolik yuz berdi. Qayta urinib ko'ring yoki /start bosing.",
                show_alert=True,
            )
        elif update.message:
            await update.message.answer(
                "\u274C Kutilmagan xatolik yuz berdi. /start bosib qayta urinib ko'ring."
            )
    except Exception:
        pass
    return True


async def main():
    logging.basicConfig(level=logging.INFO)

    if not BOT_TOKEN:
        raise RuntimeError(
            "BOT_TOKEN o'rnatilmagan. .env faylida yoki Render Environment Variables'da "
            "BOT_TOKEN ni to'ldiring."
        )

    await init_db()
    try:
        await AUTOPAY.init()
    except Exception:
        logging.exception("Avto-to'lov jadvallarini yaratib bo'lmadi (bot qo'lda rejimda ishlayveradi)")

    bot = Bot(token=BOT_TOKEN)
    AUTOPAY.attach(bot)
    dp = Dispatcher(storage=PersistentStorage())
    dp.errors.register(global_error_handler)

    # Har bir xabar/tugma bosishi routerlarga yetib borishidan OLDIN: avval
    # bloklangan-emasligi tekshiriladi, keyin spam/flood cheklovi qo'llanadi.
    ban_middleware = BanMiddleware()
    force_sub_middleware = ForceSubMiddleware()
    throttle_middleware = ThrottleMiddleware()
    dp.message.outer_middleware(ban_middleware)
    dp.callback_query.outer_middleware(ban_middleware)
    dp.message.outer_middleware(force_sub_middleware)
    dp.callback_query.outer_middleware(force_sub_middleware)
    dp.message.outer_middleware(throttle_middleware)
    dp.callback_query.outer_middleware(throttle_middleware)

    # Diqqat: router_common (bosh menyu tugmalari uchun umumiy filtr) va
    # router_admin birinchi bo'lib qo'shiladi, keyin qolgan bo'limlar.
    dp.include_router(router_common)
    dp.include_router(router_admin)
    dp.include_router(router_start)
    dp.include_router(router_balance)
    dp.include_router(router_numbers)
    dp.include_router(router_stars)
    dp.include_router(router_premium)
    dp.include_router(router_uc)
    dp.include_router(router_orders)

    await bot.delete_webhook(drop_pending_updates=True)
    await _run_health_server()

    # Avto-to'lov userbot'i fonda ulanadi — botni to'xtatib turmaydi; xato bo'lsa bot qo'lda rejimda ishlayveradi.
    try:
        await AUTOPAY.start()
    except Exception:
        logging.exception("Avto-to'lovni ishga tushirib bo'lmadi (bot qo'lda rejimda ishlayveradi)")

    # UC buyurtmalari administrator tomonidan tasdiqlanishini/rad etilishini fonda kuzatadi
    # (rad etilsa — pul foydalanuvchi balansiga qaytariladi, bir marta).
    uc_task = asyncio.create_task(uc_sweeper(bot))
    _background_tasks.add(uc_task)
    uc_task.add_done_callback(_background_tasks.discard)

    # MUHIM (Render "zero-downtime deploy"ga oid): yangi deploy paytida
    # Render eski va yangi instansiyani BIR NECHA O'N SONIYA (hattoki
    # ~60 soniyagacha) PARALLEL ishlab turishga majbur qiladi — shu daqiqada
    # ikkalasi ham Telegram'dan getUpdates so'rasa, "Conflict: terminated by
    # other getUpdates request" xatosi chiqadi (loglarda ko'rilgan). aiogram
    # buni o'zi avtomatik qayta urinib, bir necha soniyada tuzatadi — bu
    # xavfli emas va o'z-o'zidan tuzaladi.
    #
    # Bu yerdagi signal handler esa BOSHQA narsani hal qiladi: Render eski
    # instansiyaga SIGTERM yuborgan zahoti (deploy jarayonining tabiiy
    # qismi) pollingni DARHOL to'xtatib, pastdagi `finally` blokidagi
    # tozalashni (DB pool va bot sessiyasini toza yopish) ishga tushiradi.
    # Handler bo'lmasa, jarayon SIGKILL bilan majburan o'chirilib, ochiq DB
    # ulanishlari Postgres tomonida "idle" holida osilib qolishi mumkin edi.
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass  # ba'zi platformalarda (masalan Windows) qo'llab-quvvatlanmaydi

    logging.info("Bot ishga tushdi (polling rejimida)")
    try:
        polling_task = asyncio.create_task(dp.start_polling(bot))
        stop_task = asyncio.create_task(stop_event.wait())
        await asyncio.wait({polling_task, stop_task}, return_when=asyncio.FIRST_COMPLETED)
        if not polling_task.done():
            logging.info("To'xtatish signali qabul qilindi — polling yakunlanmoqda...")
            await dp.stop_polling()
            polling_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await polling_task
    finally:
        # Server to'xtatilganda (Render/VPS restart, deploy, Ctrl+C) ochiq
        # ulanishlarni tartibli yopamiz — aks holda Postgres'da "idle"
        # ulanishlar to'planib qoladi.
        # Avto-to'lov: userbot'ni to'xtatib, "ijara"ni bo'shatamiz (yangi deploy kutib qolmasin).
        # DB pool yopilishidan OLDIN bajarilishi shart.
        uc_task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await uc_task
        with contextlib.suppress(Exception):
            await AUTOPAY.stop()
        if _pool is not None:
            await _pool.close()
        await client.close()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
