"""
==================================================================
 NOMER BOT — bitta faylga birlashtirilgan versiya
 (Raqam / Telegram Stars / Telegram Premium sotish boti, SmmUpper
 Hamkorlik API v2 orqali; aiogram 3, SQLite yoki Postgres bilan)
==================================================================
"""
import asyncio
import json
import logging
import os
import re
import time
import uuid
from typing import Any, Awaitable, Callable, Dict, Optional

import aiohttp
import aiosqlite
import asyncpg
from aiogram import BaseMiddleware, Bot, Dispatcher, F, Router
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
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

# Narxga qo'shiladigan foyda foizi. Masalan 10 -> SmmUpper narxiga +10%
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
"""

_SCHEMA_PG = """
CREATE TABLE IF NOT EXISTS users (
    user_id BIGINT PRIMARY KEY,
    username TEXT,
    full_name TEXT,
    balance BIGINT NOT NULL DEFAULT 0,
    banned BOOLEAN NOT NULL DEFAULT FALSE,
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
"""


async def init_db():
    global _pool
    if _PG:
        _pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
        async with _pool.acquire() as conn:
            await conn.execute(_SCHEMA_PG)
    else:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.executescript(_SCHEMA_SQLITE)
            await db.commit()


async def ensure_user(user_id: int, username: Optional[str], full_name: Optional[str]):
    if _PG:
        async with _pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO users (user_id, username, full_name, balance, created_at) "
                "VALUES ($1, $2, $3, 0, $4) ON CONFLICT (user_id) DO NOTHING",
                user_id, username, full_name, int(time.time()),
            )
            await conn.execute(
                "UPDATE users SET username = $1, full_name = $2 WHERE user_id = $3",
                username, full_name, user_id,
            )
    else:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT OR IGNORE INTO users (user_id, username, full_name, balance, created_at) "
                "VALUES (?, ?, ?, 0, ?)",
                (user_id, username, full_name, int(time.time())),
            )
            await db.execute(
                "UPDATE users SET username = ?, full_name = ? WHERE user_id = ?",
                (username, full_name, user_id),
            )
            await db.commit()


async def get_balance(user_id: int) -> int:
    if _PG:
        async with _pool.acquire() as conn:
            row = await conn.fetchrow("SELECT balance FROM users WHERE user_id = $1", user_id)
            return row["balance"] if row else 0
    else:
        async with aiosqlite.connect(DB_PATH) as db:
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
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "UPDATE users SET balance = balance + ? WHERE user_id = ?", (delta, user_id)
            )
            await db.commit()


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
        async with aiosqlite.connect(DB_PATH) as db:
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
        async with aiosqlite.connect(DB_PATH) as db:
            cur = await db.execute("SELECT banned FROM users WHERE user_id = ?", (user_id,))
            row = await cur.fetchone()
            return bool(row[0]) if row else False


async def set_banned(user_id: int, banned: bool):
    if _PG:
        async with _pool.acquire() as conn:
            await conn.execute("UPDATE users SET banned = $1 WHERE user_id = $2", banned, user_id)
    else:
        async with aiosqlite.connect(DB_PATH) as db:
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
        async with aiosqlite.connect(DB_PATH) as db:
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
                "SELECT COUNT(*) AS c FROM orders WHERE user_id = $1", row["user_id"]
            )
            result = dict(row)
            result["order_count"] = orders_row["c"]
            return result
    else:
        async with aiosqlite.connect(DB_PATH) as db:
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
            cur2 = await db.execute("SELECT COUNT(*) FROM orders WHERE user_id = ?", (row["user_id"],))
            count_row = await cur2.fetchone()
            result = dict(row)
            result["order_count"] = count_row[0]
            return result


async def create_order(user_id: int, order_type: str, ref: Optional[str], server: Optional[int],
                        price: int, details: dict, status: str = "processing") -> int:
    details_json = json.dumps(details, ensure_ascii=False)
    if _PG:
        async with _pool.acquire() as conn:
            row = await conn.fetchrow(
                "INSERT INTO orders (user_id, order_type, ref, server, status, price, details, created_at) "
                "VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING id",
                user_id, order_type, ref, server, status, price, details_json, int(time.time()),
            )
            return row["id"]
    else:
        async with aiosqlite.connect(DB_PATH) as db:
            cur = await db.execute(
                "INSERT INTO orders (user_id, order_type, ref, server, status, price, details, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (user_id, order_type, ref, server, status, price, details_json, int(time.time())),
            )
            await db.commit()
            return cur.lastrowid


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
        async with aiosqlite.connect(DB_PATH) as db:
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
        async with aiosqlite.connect(DB_PATH) as db:
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
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                "SELECT * FROM orders WHERE user_id = ? ORDER BY id DESC LIMIT ?", (user_id, limit)
            )
            return await cur.fetchall()


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
        async with aiosqlite.connect(DB_PATH) as db:
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
        async with aiosqlite.connect(DB_PATH) as db:
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
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("SELECT * FROM topups WHERE id = ?", (topup_id,))
            return await cur.fetchone()


async def set_topup_status(topup_id: int, status: str):
    if _PG:
        async with _pool.acquire() as conn:
            await conn.execute("UPDATE topups SET status = $1 WHERE id = $2", status, topup_id)
    else:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE topups SET status = ? WHERE id = ?", (status, topup_id))
            await db.commit()


async def get_setting(key: str, default: Optional[str] = None) -> Optional[str]:
    """Botning ichki sozlamalarini (masalan SmmUpper API kaliti) o'qiydi —
    admin panel orqali .env'ga qayta kirmasdan o'zgartirish mumkin bo'lishi
    uchun. Baza bo'sh bo'lsa `default` qaytariladi."""
    if _PG:
        async with _pool.acquire() as conn:
            row = await conn.fetchrow("SELECT value FROM settings WHERE key = $1", key)
            return row["value"] if row else default
    else:
        async with aiosqlite.connect(DB_PATH) as db:
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
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT INTO settings (key, value) VALUES (?, ?) "
                "ON CONFLICT (key) DO UPDATE SET value = ?",
                (key, value, value),
            )
            await db.commit()


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
        async with aiosqlite.connect(DB_PATH) as db:
            cur = await db.execute(
                "SELECT COUNT(*) FROM topups WHERE user_id = ? AND status = 'pending' AND id != ?",
                (user_id, exclude_topup_id),
            )
            row = await cur.fetchone()
            return row[0]


async def get_stats() -> dict:
    """Admin panel uchun umumiy statistika: foydalanuvchilar, buyurtmalar
    (turlari bo'yicha), tushum va kutilayotgan to'ldirish so'rovlari."""
    day_ago = int(time.time()) - 86400
    order_types = ("number", "stars", "premium")

    if _PG:
        async with _pool.acquire() as conn:
            users_total = (await conn.fetchrow("SELECT COUNT(*) AS c FROM users"))["c"]
            users_banned = (await conn.fetchrow(
                "SELECT COUNT(*) AS c FROM users WHERE banned = TRUE"))["c"]
            users_new_today = (await conn.fetchrow(
                "SELECT COUNT(*) AS c FROM users WHERE created_at >= $1", day_ago))["c"]
            balance_total = (await conn.fetchrow(
                "SELECT COALESCE(SUM(balance), 0) AS s FROM users"))["s"]

            orders_by_type = {}
            for order_type in order_types:
                row = await conn.fetchrow(
                    "SELECT COUNT(*) AS c FROM orders WHERE order_type = $1", order_type
                )
                orders_by_type[order_type] = row["c"]
            orders_total = sum(orders_by_type.values())
            orders_today = (await conn.fetchrow(
                "SELECT COUNT(*) AS c FROM orders WHERE created_at >= $1", day_ago))["c"]

            revenue_total = (await conn.fetchrow(
                "SELECT COALESCE(SUM(price), 0) AS s FROM orders WHERE status != 'refunded'"))["s"]
            revenue_today = (await conn.fetchrow(
                "SELECT COALESCE(SUM(price), 0) AS s FROM orders "
                "WHERE status != 'refunded' AND created_at >= $1", day_ago))["s"]

            topups_pending = (await conn.fetchrow(
                "SELECT COUNT(*) AS c FROM topups WHERE status = 'pending'"))["c"]
    else:
        async with aiosqlite.connect(DB_PATH) as db:
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
            for order_type in order_types:
                orders_by_type[order_type] = await scalar(
                    "SELECT COUNT(*) FROM orders WHERE order_type = ?", (order_type,))
            orders_total = sum(orders_by_type.values())
            orders_today = await scalar(
                "SELECT COUNT(*) FROM orders WHERE created_at >= ?", (day_ago,))

            revenue_total = await scalar(
                "SELECT COALESCE(SUM(price), 0) FROM orders WHERE status != 'refunded'")
            revenue_today = await scalar(
                "SELECT COALESCE(SUM(price), 0) FROM orders "
                "WHERE status != 'refunded' AND created_at >= ?", (day_ago,))

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
    def __init__(self, api_key: str = SMMUPPER_API_KEY, base_url: str = SMMUPPER_BASE_URL):
        self.api_key = api_key  # .env'dagi standart qiymat — DB bo'sh bo'lsa shu ishlatiladi
        self.base_url = base_url

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

        timeout = aiohttp.ClientTimeout(total=20)
        async with aiohttp.ClientSession(timeout=timeout) as session:
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
    async def get_prices(self) -> dict:
        return await self._request("getPrices")

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

    # 8. Buyurtma holati
    async def get_order(self, order_id) -> dict:
        return await self._request("getOrder", order_id=order_id)


# ==============================================================
# NARXLASH (pricing)
# ==============================================================
SETTINGS_KEY_MARKUP = "markup_percent"


async def get_markup_percent() -> float:
    """Admin panel orqali bazaga saqlangan narx ustamasi bo'lsa o'shani,
    aks holda .env (MARKUP_PERCENT) dagisini qaytaradi."""
    raw = await get_setting(SETTINGS_KEY_MARKUP, default=str(MARKUP_PERCENT))
    try:
        return float(raw)
    except (TypeError, ValueError):
        return MARKUP_PERCENT


async def with_markup(base_price) -> int:
    """Bazaviy (SmmUpper) narxga sozlangan foyda foizini qo'shib, yaxlit
    so'mga aylantiradi. Raqam, Stars va Premium — uchalasi ham shu bitta
    funksiyadan foydalanadi, shuning uchun ustama hammasiga bir xilda
    qo'llanadi."""
    percent = await get_markup_percent()
    return round(float(base_price) * (1 + percent / 100))


# ==============================================================
# MATNLAR (texts)
# ==============================================================
WELCOME = (
    "\U0001F44B Assalomu alaykum!\n\n"
    "Bu bot orqali siz:\n"
    "\U0001F4F1 Virtual raqam (SMS kod uchun)\n"
    "\u2B50 Telegram Stars\n"
    "\U0001F48E Telegram Premium\n\n"
    "sotib olishingiz mumkin. Quyidagi menyudan tanlang \U0001F447"
)

MENU_TITLE = "Bosh menyu:"

BTN_MENU = "\U0001F3E0 Menyu"
BTN_BALANCE = "\U0001F4B0 Balans"
BTN_TOPUP = "\U0001F4B3 Balansni to'ldirish"
BTN_NUMBER = "\U0001F4F1 Raqam sotib olish"
BTN_STARS = "\u2B50 Stars sotib olish"
BTN_PREMIUM = "\U0001F48E Premium sotib olish"
BTN_ORDERS = "\U0001F4CB Buyurtmalarim"
BTN_CANCEL = "\u274C Bekor qilish"
BTN_CHECK_CODE = "\U0001F504 Kodni tekshirish"
BTN_CONFIRM = "\u2705 Tasdiqlash"

# Bosh menyudagi tugma matnlari — bular FSM holatida turgan "erkin matn"
# handlerlar tomonidan "username" yoki "summa" deb noto'g'ri qabul qilinmasligi kerak.
RESERVED_TEXTS = {
    BTN_MENU, BTN_BALANCE, BTN_TOPUP, BTN_NUMBER,
    BTN_STARS, BTN_PREMIUM, BTN_ORDERS, BTN_CANCEL,
}


def fmt_money(amount) -> str:
    return f"{int(amount):,}".replace(",", " ")


def balance_text(amount: int) -> str:
    return f"\U0001F4B0 Balansingiz: {fmt_money(amount)} so'm"


def insufficient_balance(price: int, balance: int) -> str:
    return (
        f"\u274C Balansingiz yetarli emas. "
        f"Kerakli summa: {fmt_money(price)} so'm, sizda: {fmt_money(balance)} so'm."
    )


TOPUP_ASK_AMOUNT = "Necha so'mga balansni to'ldirmoqchisiz? Summani kiriting (masalan: 50000)."
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


def channel_number_notice(buyer: str, country: str, price: int) -> str:
    return (
        f"\U0001F195 Yangi buyurtma \u2014 \U0001F4F1 Raqam\n"
        f"\U0001F464 {buyer}\n"
        f"\U0001F30D Davlat: {country}\n"
        f"\U0001F4B5 Narx: {fmt_money(price)} so'm"
    )


def channel_stars_notice(buyer: str, target_username: str, amount: int, price: int) -> str:
    return (
        f"\U0001F195 Yangi buyurtma \u2014 \u2B50 Stars\n"
        f"\U0001F464 {buyer}\n"
        f"\U0001F3AF Kimga: @{target_username}\n"
        f"\u2B50 Miqdor: {amount}\n"
        f"\U0001F4B5 Narx: {fmt_money(price)} so'm"
    )


def channel_premium_notice(buyer: str, target_username: str, months: int, price: int) -> str:
    return (
        f"\U0001F195 Yangi buyurtma \u2014 \U0001F48E Premium\n"
        f"\U0001F464 {buyer}\n"
        f"\U0001F3AF Kimga: @{target_username}\n"
        f"\U0001F4C5 Muddat: {months} oy\n"
        f"\U0001F4B5 Narx: {fmt_money(price)} so'm"
    )


def admin_user_card(user: dict) -> str:
    banned = "\U0001F6AB Ha" if user.get("banned") else "Yo'q"
    username = f"@{user['username']}" if user.get("username") else "\u2014"
    return (
        f"\U0001F464 {user.get('full_name') or '\u2014'} ({username})\n"
        f"\U0001F522 ID: {user['user_id']}\n"
        f"\U0001F4B0 Balans: {fmt_money(user['balance'])} so'm\n"
        f"\U0001F4E6 Buyurtmalar soni: {user.get('order_count', 0)}\n"
        f"\U0001F6AB Bloklangan: {banned}"
    )


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


ASK_MARKUP_PERCENT = (
    "Narxlarga qo'shiladigan foyda foizini kiriting (masalan: 10 — bu SmmUpper "
    "narxining ustiga +10% qo'shib sotish degani).\n"
    "Ustama qo'ymaslik uchun: 0\n\n"
    "Bu foiz \U0001F4F1 Raqam, \u2B50 Stars va \U0001F48E Premium — uchalasiga ham "
    "bir vaqtda qo'llanadi."
)
NOT_A_VALID_PERCENT = "\u274C Noto'g'ri qiymat. Faqat son kiriting, masalan: 10 yoki 0"


def markup_saved(percent: float) -> str:
    return f"\u2705 Narx ustamasi saqlandi: {percent:g}%\n\nBarcha yangi buyurtmalarda shu foiz qo'llaniladi."


def current_markup_line(percent: float) -> str:
    return f"\U0001F4C8 Joriy narx ustamasi: {percent:g}%"


FORCE_SUB_PROMPT = (
    "\U0001F510 Botdan foydalanish uchun avval quyidagi kanalga a'zo bo'ling, "
    "so'ng \u2705 tugmasini bosing."
)
FORCE_SUB_STILL_NOT = "\u274C Hali kanalga a'zo emassiz. Avval a'zo bo'ling, keyin qayta tekshiring."
FORCE_SUB_OK = "\u2705 Rahmat! Endi botdan foydalanishingiz mumkin."

ASK_CHANNEL_ID = (
    "Raqam/Stars/Premium sotib olinganda xabar yuboriladigan kanalni yuboring.\n\n"
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
    "Majburiy obuna uchun kanalni yuboring.\n\n"
    "\u2022 Ochiq kanal: @kanalim\n"
    "\u2022 Yopiq kanal: -100 bilan boshlanuvchi ID\n\n"
    "O'chirib qo'yish uchun: 0\n\n"
    "\u26A0\uFE0F Botni shu kanalga oldindan ADMIN qilib (a'zolarni ko'rish huquqi "
    "bilan) qo'shib qo'ying."
)
ASK_FORCE_SUB_URL = (
    "Endi \"Kanalga o'tish\" tugmasi ochadigan havolani yuboring "
    "(masalan: https://t.me/+AbCdEfGh).\n\n"
    "Ochiq kanal bo'lsa va @username'dan avtomatik hosil qilinishini xohlasangiz: -"
)
NOT_A_VALID_FORCE_SUB_CHANNEL = "\u274C Noto'g'ri format. @kanalim yoki -100... ko'rinishida yuboring."


def current_force_sub_line(channel) -> str:
    if not channel:
        return "\U0001F510 Majburiy obuna: \u2014 (o'chiq)"
    return f"\U0001F510 Majburiy obuna: {channel}"


def force_sub_disabled_ok() -> str:
    return "\u2705 Majburiy obuna o'chirildi."


def force_sub_saved(channel, url) -> str:
    return f"\u2705 Majburiy obuna sozlandi: {channel}\nHavola: {url or '(avtomatik)'}"


ASK_REFUND_SECONDS = (
    "Raqam uchun SMS kod kelmasa, foydalanuvchi necha SONIYADAN keyin pulini "
    "o'zi qaytarib olishi mumkinligini kiriting (masalan: 600 — bu 10 daqiqa)."
)
NOT_A_VALID_SECONDS = "\u274C Noto'g'ri qiymat. Faqat musbat son kiriting, masalan: 600"


def current_refund_seconds_line(seconds: int) -> str:
    return f"\u23F1 Pul qaytarish kutish vaqti: {seconds} soniya (~{seconds // 60} daqiqa)"


def refund_seconds_saved(seconds: int) -> str:
    return f"\u2705 Saqlandi: {seconds} soniya (~{seconds // 60} daqiqa)"


ASK_TOGGLE_ADMIN = (
    "Admin qilib qo'shmoqchi (yoki adminlikdan olib tashlamoqchi) bo'lgan "
    "foydalanuvchi ID raqamini yuboring.\n\n"
    "\u2139\uFE0F Agar u ro'yxatda bo'lmasa \u2014 qo'shiladi, bo'lsa \u2014 olib tashlanadi."
)


def current_extra_admins_line(ids: list) -> str:
    if not ids:
        return "\U0001F465 Qo'shimcha adminlar: \u2014 (yo'q)"
    return "\U0001F465 Qo'shimcha adminlar: " + ", ".join(str(x) for x in ids)


def admin_toggled(user_id: int, added: bool) -> str:
    if added:
        return f"\u2705 {user_id} endi admin. U ham /admin buyrug'i orqali panelga kira oladi."
    return f"\u2705 {user_id} adminlikdan olib tashlandi."


_TYPE_LABEL_STATS = {
    "number": "\U0001F4F1 Raqam",
    "stars": "\u2B50 Stars",
    "premium": "\U0001F48E Premium",
}


def stats_text(s: dict) -> str:
    by_type_lines = "\n".join(
        f"   {_TYPE_LABEL_STATS[k]}: {s['orders_by_type'].get(k, 0)}"
        for k in ("number", "stars", "premium")
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
        f"   Jami: {fmt_money(s['revenue_total'])} so'm\n"
        f"   Bugun: {fmt_money(s['revenue_today'])} so'm\n\n"
        f"\u23F3 Kutilayotgan balans to'ldirish so'rovlari: {s['topups_pending']}"
    )


# ==============================================================
# TUGMALAR (keyboards)
# ==============================================================
# Bot API 9.4 (2026-02-09) — tugmalarga rang berish uchun "style" maydoni
# qo'shildi (KeyboardButton va InlineKeyboardButton). Ruxsat etilgan qiymatlar
# faqat shular: "danger" (qizil), "success" (yashil), "primary" (koʼk).
# Aiogram kutubxonasi bu maydonni hali rasman "tanimagan" versiyada boʼlsa ham,
# Telegram obyektlari noma'lum maydonlarni qabul qilib, serverga toʼgʼri
# yuborib beradi — shuning uchun bu yerda oddiy satr sifatida ishlatiladi.
STYLE_DANGER = "danger"    # qizil — bekor qilish / rad etish kabi "orqaga" tugmalar
STYLE_SUCCESS = "success"  # yashil — tasdiqlash / tasdiqlangan amallar
STYLE_PRIMARY = "primary"  # koʼk — asosiy tanlov tugmalari


def main_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_BALANCE), KeyboardButton(text=BTN_ORDERS)],
            [KeyboardButton(text=BTN_NUMBER)],
            [KeyboardButton(text=BTN_STARS), KeyboardButton(text=BTN_PREMIUM)],
            [KeyboardButton(text=BTN_MENU)],
        ],
        resize_keyboard=True,
    )


def balance_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=BTN_TOPUP, callback_data="topup:start", style=STYLE_PRIMARY)],
    ])


def cancel_inline() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=BTN_CANCEL, callback_data="cancel", style=STYLE_DANGER)],
    ])


def number_type_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="\U0001F4F1 Oddiy raqam (SMS kod uchun)", callback_data="numtype:regular", style=STYLE_PRIMARY)],
        [InlineKeyboardButton(text="\U0001F510 Tayyor akkaunt (2FA parol bilan)", callback_data="numtype:ready", style=STYLE_PRIMARY)],
        [InlineKeyboardButton(text=BTN_CANCEL, callback_data="cancel", style=STYLE_DANGER)],
    ])


def countries_menu(server: int, countries: dict) -> InlineKeyboardMarkup:
    rows = []
    row = []
    for code, info in countries.items():
        price = info.get("price", "?")
        label = f"{code} — {fmt_money(price)} so'm" if isinstance(price, (int, float)) else str(code)
        row.append(InlineKeyboardButton(text=label, callback_data=f"cty:{server}:{code}", style=STYLE_PRIMARY))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton(text=BTN_CANCEL, callback_data="cancel", style=STYLE_DANGER)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def confirm_menu(confirm_data: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=BTN_CONFIRM, callback_data=confirm_data, style=STYLE_SUCCESS)],
        [InlineKeyboardButton(text=BTN_CANCEL, callback_data="cancel", style=STYLE_DANGER)],
    ])


def stars_amount_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="50", callback_data="starsamt:50", style=STYLE_PRIMARY),
            InlineKeyboardButton(text="100", callback_data="starsamt:100", style=STYLE_PRIMARY),
            InlineKeyboardButton(text="500", callback_data="starsamt:500", style=STYLE_PRIMARY),
        ],
        [InlineKeyboardButton(text="\u270F\uFE0F Boshqa summa", callback_data="starsamt:custom", style=STYLE_PRIMARY)],
        [InlineKeyboardButton(text=BTN_CANCEL, callback_data="cancel", style=STYLE_DANGER)],
    ])


def premium_months_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="3 oy", callback_data="premmonths:3", style=STYLE_PRIMARY),
            InlineKeyboardButton(text="6 oy", callback_data="premmonths:6", style=STYLE_PRIMARY),
            InlineKeyboardButton(text="12 oy", callback_data="premmonths:12", style=STYLE_PRIMARY),
        ],
        [InlineKeyboardButton(text=BTN_CANCEL, callback_data="cancel", style=STYLE_DANGER)],
    ])


def check_code_menu(order_pk: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=BTN_CHECK_CODE, callback_data=f"numcheck:{order_pk}", style=STYLE_PRIMARY)],
        [InlineKeyboardButton(text="\U0001F4B8 Pulni qaytarish", callback_data=f"numrefund:{order_pk}", style=STYLE_DANGER)],
    ])


def admin_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="\U0001F50E Foydalanuvchi qidirish", callback_data="adm:find", style=STYLE_PRIMARY)],
        [InlineKeyboardButton(text="\U0001F4B0 Balans qo'shish/ayirish", callback_data="adm:balance", style=STYLE_PRIMARY)],
        [
            InlineKeyboardButton(text="\U0001F6AB Bloklash", callback_data="adm:ban", style=STYLE_DANGER),
            InlineKeyboardButton(text="\u2705 Blokdan chiqarish", callback_data="adm:unban", style=STYLE_SUCCESS),
        ],
        [InlineKeyboardButton(text="\U0001F4E2 Xabar yuborish (broadcast)", callback_data="adm:broadcast", style=STYLE_PRIMARY)],
        [InlineKeyboardButton(text="\U0001F511 API kalitni sozlash", callback_data="adm:apikey", style=STYLE_PRIMARY)],
        [InlineKeyboardButton(text="\U0001F4C8 Narx ustamasini sozlash (%)", callback_data="adm:markup", style=STYLE_PRIMARY)],
        [InlineKeyboardButton(text="\U0001F4E2 Xarid kanalini sozlash", callback_data="adm:channel", style=STYLE_PRIMARY)],
        [InlineKeyboardButton(text="\U0001F4B3 To'lov kartasini sozlash", callback_data="adm:card", style=STYLE_PRIMARY)],
        [InlineKeyboardButton(text="\U0001F510 Majburiy obunani sozlash", callback_data="adm:forcesub", style=STYLE_PRIMARY)],
        [InlineKeyboardButton(text="\u23F1 Pul qaytarish vaqtini sozlash", callback_data="adm:refundtime", style=STYLE_PRIMARY)],
        [InlineKeyboardButton(text="\U0001F465 Qo'shimcha adminlar", callback_data="adm:toggleadmin", style=STYLE_PRIMARY)],
        [InlineKeyboardButton(text="\U0001F4CA Statistika", callback_data="adm:stats", style=STYLE_PRIMARY)],
        [InlineKeyboardButton(text="\U0001F504 Yangilash", callback_data="adm:refresh", style=STYLE_PRIMARY)],
    ])


def admin_cancel_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="\u2B05\uFE0F Admin panelga qaytish", callback_data="adm:refresh", style=STYLE_DANGER)],
    ])


def force_sub_menu(channel_url: str) -> InlineKeyboardMarkup:
    rows = []
    if channel_url:
        rows.append([InlineKeyboardButton(text="\U0001F4E2 Kanalga o'tish", url=channel_url)])
    rows.append([InlineKeyboardButton(text="\u2705 A'zo bo'ldim", callback_data="forcesub:check", style=STYLE_SUCCESS)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def topup_review_menu(topup_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="\u2705 Tasdiqlash", callback_data=f"topup:approve:{topup_id}", style=STYLE_SUCCESS),
            InlineKeyboardButton(text="\u274C Rad etish", callback_data=f"topup:reject:{topup_id}", style=STYLE_DANGER),
        ],
    ])


# ==============================================================
# FSM HOLATLARI (states)
# ==============================================================
class TopUp(StatesGroup):
    amount = State()
    photo = State()


class BuyNumber(StatesGroup):
    choosing_country = State()
    confirming = State()


class BuyStars(StatesGroup):
    username = State()
    amount = State()
    confirming = State()


class BuyPremium(StatesGroup):
    months = State()
    username = State()
    confirming = State()


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
    toggle_admin = State()


# ==============================================================
# UMUMIY HANDLERLAR (handler_common)
# ==============================================================
router_common = Router(name="common")

SETTINGS_KEY_CHANNEL_ID = "channel_id"
SETTINGS_KEY_CARD_NUMBER = "card_number"
SETTINGS_KEY_CARD_HOLDER = "card_holder"
SETTINGS_KEY_FORCE_SUB_CHANNEL = "force_sub_channel"
SETTINGS_KEY_FORCE_SUB_URL = "force_sub_url"
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


async def get_force_sub_channel():
    """Admin panel orqali bazaga saqlangan majburiy obuna kanali bo'lsa
    o'shani, aks holda .env (FORCE_SUB_CHANNEL) dagisini qaytaradi.
    Hech biri sozlanmagan bo'lsa (None, "") — majburiy obuna o'chiq."""
    raw = await get_setting(SETTINGS_KEY_FORCE_SUB_CHANNEL, default=None)
    channel = raw if raw is not None else FORCE_SUB_CHANNEL

    url = await get_setting(SETTINGS_KEY_FORCE_SUB_URL, default=None)
    if url is None:
        url = FORCE_SUB_CHANNEL_URL
    if not url and channel.startswith("@"):
        url = f"https://t.me/{channel.lstrip('@')}"
    return channel, url


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


async def toggle_extra_admin(user_id: int) -> bool:
    """user_id ro'yxatda bo'lsa olib tashlaydi, bo'lmasa qo'shadi.
    Qaytariladi: True — qo'shildi, False — olib tashlandi."""
    ids = await get_extra_admin_ids()
    if user_id in ids:
        ids.remove(user_id)
        added = False
    else:
        ids.append(user_id)
        added = True
    await set_setting(SETTINGS_KEY_EXTRA_ADMINS, ",".join(str(x) for x in ids))
    return added


async def notify_channel(bot, text: str):
    """Raqam / Stars / Premium sotib olinganda kanalga xabar yuboradi.
    Kanal sozlanmagan bo'lsa — hech narsa qilmaydi. Kanalga yuborish
    muvaffaqiyatsiz bo'lsa (masalan, bot hali admin qilib qo'shilmagan bo'lsa),
    xatolik foydalanuvchiga ta'sir qilmasligi uchun jimgina e'tiborsiz qoldiriladi.
    """
    channel_id = await get_channel_id()
    if not channel_id:
        return
    try:
        await bot.send_message(channel_id, text)
    except Exception:
        pass


async def is_subscribed(bot, user_id: int) -> bool:
    """Majburiy obuna kanaliga foydalanuvchi a'zo-yo'qligini tekshiradi.
    Kanal sozlanmagan bo'lsa — tekshirilmaydi (True qaytadi).
    Bot kanalga admin qilib qo'shilmagan yoki boshqa sabab bilan tekshira
    olmasa — botni butunlay to'xtatib qo'ymaslik uchun xavfsiz tomonga
    (True, ya'ni "a'zo") og'ib ketiladi.
    """
    channel, _ = await get_force_sub_channel()
    if not channel:
        return True
    try:
        member = await bot.get_chat_member(channel, user_id)
        return member.status not in _NOT_SUBSCRIBED_STATUSES
    except TelegramBadRequest:
        return True
    except Exception:
        return True


async def send_force_sub_prompt(bot, chat_id: int):
    _, url = await get_force_sub_channel()
    try:
        await bot.send_message(
            chat_id, FORCE_SUB_PROMPT,
            reply_markup=force_sub_menu(url),
        )
    except Exception:
        pass


@router_common.callback_query(F.data == "forcesub:check")
async def force_sub_check(callback: CallbackQuery, bot):
    if await is_subscribed(bot, callback.from_user.id):
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


@router_common.callback_query(F.data == "cancel")
async def cancel_any(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.answer("Bekor qilindi.", reply_markup=main_menu())
    await callback.answer()


# ==============================================================
# START HANDLER (handler_start)
# ==============================================================
router_start = Router(name="start")


@router_start.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    await ensure_user(message.from_user.id, message.from_user.username, message.from_user.full_name)
    await message.answer(WELCOME, reply_markup=main_menu())


@router_start.message(F.text == BTN_MENU)
async def show_menu(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(MENU_TITLE, reply_markup=main_menu())


# ==============================================================
# BALANS HANDLER (handler_balance)
# ==============================================================
router_balance = Router(name="balance")


@router_balance.message(F.text == BTN_BALANCE)
async def show_balance(message: Message, state: FSMContext):
    await state.clear()
    balance = await get_balance(message.from_user.id)
    await message.answer(balance_text(balance), reply_markup=balance_menu())


@router_balance.callback_query(F.data == "topup:start")
async def topup_start(callback: CallbackQuery, state: FSMContext):
    await state.set_state(TopUp.amount)
    await callback.message.answer(TOPUP_ASK_AMOUNT, reply_markup=cancel_inline())
    await callback.answer()


@router_balance.message(TopUp.amount, is_free_text)
async def topup_amount(message: Message, state: FSMContext):
    text = message.text.strip().replace(" ", "")
    if not text.isdigit() or int(text) <= 0:
        await message.answer(TOPUP_NOT_A_NUMBER)
        return

    amount = int(text)
    await state.update_data(amount=amount)
    await state.set_state(TopUp.photo)
    card_number, card_holder = await get_card_info()
    await message.answer(topup_instructions(amount, card_number, card_holder), reply_markup=cancel_inline())


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

    for admin_id in ADMIN_IDS:
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


@router_numbers.message(F.text == BTN_NUMBER)
async def start_number_flow(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Qanday raqam kerak?", reply_markup=number_type_menu())


async def _fetch_countries_with_fallback(primary_server: int, fallback_server: Optional[int]):
    try:
        data = await client.available_countries(primary_server)
        countries = data.get("countries") or {}
        if countries:
            return primary_server, countries
    except SmmUpperError:
        pass

    if fallback_server:
        try:
            data = await client.available_countries(fallback_server)
            return fallback_server, (data.get("countries") or {})
        except SmmUpperError:
            pass

    return primary_server, {}


@router_numbers.callback_query(F.data == "numtype:regular")
async def choose_regular(callback: CallbackQuery, state: FSMContext):
    await callback.answer("Davlatlar yuklanmoqda...")
    server, countries = await _fetch_countries_with_fallback(1, 2)
    if not countries:
        await callback.message.answer("\u274C Hozircha mavjud davlat yo'q. Birozdan so'ng qayta urinib ko'ring.")
        return

    await state.set_state(BuyNumber.choosing_country)
    await state.update_data(server=server, countries=countries)
    await callback.message.answer("Davlatni tanlang:", reply_markup=countries_menu(server, countries))


@router_numbers.callback_query(F.data == "numtype:ready")
async def choose_ready(callback: CallbackQuery, state: FSMContext):
    await callback.answer("Yuklanmoqda...")
    try:
        data = await client.available_countries(3)
        countries = data.get("countries") or {}
    except SmmUpperError as e:
        await callback.message.answer(f"\u274C Tayyor akkauntlar hozircha mavjud emas: {e.message}")
        return

    if not countries:
        await callback.message.answer("\u274C Hozircha tayyor akkaunt yo'q.")
        return

    await state.set_state(BuyNumber.choosing_country)
    await state.update_data(server=3, countries=countries)
    await callback.message.answer("Davlatni tanlang:", reply_markup=countries_menu(3, countries))


@router_numbers.callback_query(BuyNumber.choosing_country, F.data.startswith("cty:"))
async def choose_country(callback: CallbackQuery, state: FSMContext):
    _, server_str, country = callback.data.split(":")
    server = int(server_str)

    data = await state.get_data()
    countries = data.get("countries", {})
    info = countries.get(country, {})
    base_price = info.get("price", 0)
    price = await with_markup(base_price)

    balance = await get_balance(callback.from_user.id)
    await state.update_data(country=country, price=price)
    await state.set_state(BuyNumber.confirming)

    text = (
        f"\U0001F30D Davlat: {country}\n"
        f"\U0001F4B5 Narx: {fmt_money(price)} so'm\n"
        f"\U0001F4B0 Balansingiz: {fmt_money(balance)} so'm"
    )

    if balance < price:
        await callback.message.answer(text + "\n\n" + insufficient_balance(price, balance),
                                       reply_markup=balance_menu())
        await state.clear()
        await callback.answer()
        return

    await callback.message.answer(text + "\n\nTasdiqlaysizmi?", reply_markup=confirm_menu("buynum:confirm"))
    await callback.answer()


@router_numbers.callback_query(BuyNumber.confirming, F.data == "buynum:confirm")
async def confirm_number(callback: CallbackQuery, state: FSMContext, bot):
    data = await state.get_data()
    server = data["server"]
    country = data["country"]
    est_price = data["price"]

    # Balansni SHU YERDA, bitta atomik amal bilan tekshirib-va-yechib qo'yamiz —
    # SmmUpper'ga ketadigan (sekin) so'rovdan OLDIN. Aks holda ikkita xaridni bir
    # vaqtda tasdiqlash orqali balansni race condition bilan minusga tushirish
    # mumkin bo'lardi.
    if not await try_deduct_balance(callback.from_user.id, est_price):
        balance = await get_balance(callback.from_user.id)
        await callback.message.answer(insufficient_balance(est_price, balance))
        await state.clear()
        await callback.answer()
        return

    await callback.answer("Amalga oshirilmoqda...")

    try:
        result = await client.get_number(server, country, request_id=new_request_id())
    except SmmUpperError as e:
        await change_balance(callback.from_user.id, est_price)
        await callback.message.answer(f"\u274C Xatolik: {e.message}\n\U0001F4B0 Pulingiz balansga qaytarildi.")
        await state.clear()
        return

    actual_price = await with_markup(result.get("price", 0))
    if actual_price != est_price:
        await change_balance(callback.from_user.id, est_price - actual_price)

    ref = result.get("hash_code") or result.get("number") or str(result.get("id"))
    order_pk = await create_order(
        user_id=callback.from_user.id,
        order_type="number",
        ref=ref,
        server=server,
        price=actual_price,
        details=result,
        status="processing",
    )

    number = result.get("number", "?")

    buyer = callback.from_user.full_name
    if callback.from_user.username:
        buyer += f" (@{callback.from_user.username})"
    await notify_channel(bot, channel_number_notice(buyer, country, actual_price))

    sent = await callback.message.answer(
        f"\u2705 Raqam olindi: {number}\n"
        f"\U0001F4B5 Narx: {fmt_money(actual_price)} so'm\n\n"
        f"\u23F3 SMS kod kutilmoqda...",
        reply_markup=check_code_menu(order_pk),
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

        if data.get("success"):
            code = data.get("code", "?")
            password = data.get("password") or ""
            text = f"\u2705 SMS kod keldi: {code}"
            if password:
                text += f"\n\U0001F511 2FA parol: {password}"
            await update_order_status(order_pk, "done", {**result, "code": code, "password": password})
            await _clear_buttons(bot, user_id, purchase_message_id)
            try:
                await bot.send_message(user_id, text)
            except Exception:
                pass
            return

    try:
        await bot.send_message(
            user_id,
            "\u231B Kod hali kelmadi. Pastdagi tugma orqali istalgan vaqt tekshirishingiz mumkin.",
            reply_markup=check_code_menu(order_pk),
        )
    except Exception:
        pass


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

    result = json.loads(row["details"]) if row["details"] else {}
    server = row["server"]

    try:
        data = await _try_get_code(server, result)
    except SmmUpperError as e:
        await callback.answer(f"Xatolik: {e.message}", show_alert=True)
        return

    if data.get("success"):
        code = data.get("code", "?")
        password = data.get("password") or ""
        await update_order_status(order_pk, "done", {**result, "code": code, "password": password})
        try:
            await callback.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        text = f"Kod: {code}"
        if password:
            text += f"\n2FA parol: {password}"
        await callback.answer(text, show_alert=True)
    else:
        await callback.answer("\u23F3 Kod hali kelmagan. Birozdan so'ng qayta tekshiring.", show_alert=True)


@router_numbers.callback_query(F.data.startswith("numrefund:"))
async def refund_number_order(callback: CallbackQuery):
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

    new_balance = await get_balance(callback.from_user.id)
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await callback.message.answer(
        f"\U0001F4B8 {fmt_money(refund['price'])} so'm balansingizga qaytarildi.\n"
        f"\U0001F4B0 Joriy balans: {fmt_money(new_balance)} so'm"
    )
    await callback.answer()


# ==============================================================
# STARS HANDLER (handler_stars)
# ==============================================================
router_stars = Router(name="stars")

USERNAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{4,31}$")
MIN_STARS = 50


@router_stars.message(F.text == BTN_STARS)
async def start_stars_flow(message: Message, state: FSMContext):
    await state.clear()
    await state.set_state(BuyStars.username)
    await message.answer(
        "Kimga Stars sotib olamiz? Telegram username kiriting (masalan: durov).",
        reply_markup=cancel_inline(),
    )


@router_stars.message(BuyStars.username, is_free_text)
async def stars_username(message: Message, state: FSMContext):
    username = message.text.strip().lstrip("@")
    if not USERNAME_RE.match(username):
        await message.answer("Username noto'g'ri ko'rinadi. Qayta kiriting (masalan: durov).")
        return

    await state.update_data(username=username)
    await state.set_state(BuyStars.amount)
    await message.answer(
        f"Nechta Stars? (min {MIN_STARS}, yoki summani yozib yuboring)",
        reply_markup=stars_amount_menu(),
    )


@router_stars.callback_query(BuyStars.amount, F.data.startswith("starsamt:"))
async def stars_amount_choice(callback: CallbackQuery, state: FSMContext):
    value = callback.data.split(":", 1)[1]
    if value == "custom":
        await callback.message.answer(f"Nechta Stars kerak? Sonini yozing (min {MIN_STARS}).",
                                       reply_markup=cancel_inline())
        await callback.answer()
        return

    await callback.answer()
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
    except (SmmUpperError, KeyError) as e:
        await message.answer(f"\u274C Narxlarni olib bo'lmadi: {e}")
        return

    price = await with_markup(price_per_star * amount)
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
        await message.answer(text + "\n\n" + insufficient_balance(price, balance),
                              reply_markup=balance_menu())
        await state.clear()
        return

    await message.answer(text + "\n\nTasdiqlaysizmi?", reply_markup=confirm_menu("buystars:confirm"))


@router_stars.callback_query(BuyStars.confirming, F.data == "buystars:confirm")
async def confirm_stars(callback: CallbackQuery, state: FSMContext, bot):
    data = await state.get_data()
    username = data["username"]
    amount = data["amount"]
    est_price = data["price"]

    if not await try_deduct_balance(callback.from_user.id, est_price):
        balance = await get_balance(callback.from_user.id)
        await callback.message.answer(insufficient_balance(est_price, balance))
        await state.clear()
        await callback.answer()
        return

    await callback.answer("Amalga oshirilmoqda...")

    try:
        result = await client.buy_stars(username, amount, request_id=new_request_id())
    except SmmUpperError as e:
        await change_balance(callback.from_user.id, est_price)
        await callback.message.answer(f"\u274C Xatolik: {e.message}\n\U0001F4B0 Pulingiz balansga qaytarildi.")
        await state.clear()
        return

    actual_price = await with_markup(result.get("price", 0))
    if actual_price != est_price:
        await change_balance(callback.from_user.id, est_price - actual_price)
    order_pk = await create_order(
        user_id=callback.from_user.id,
        order_type="stars",
        ref=result.get("order_id"),
        server=None,
        price=actual_price,
        details=result,
        status="processing",
    )

    buyer = callback.from_user.full_name
    if callback.from_user.username:
        buyer += f" (@{callback.from_user.username})"
    await notify_channel(bot, channel_stars_notice(buyer, username, amount, actual_price))

    await callback.message.answer(
        f"\u2705 Buyurtma qabul qilindi!\n"
        f"\U0001F464 @{username}\n"
        f"\u2B50 {amount} Stars\n"
        f"\U0001F4B5 {fmt_money(actual_price)} so'm\n"
        f"\U0001F522 Buyurtma raqami: {result.get('order_id')} (#{order_pk})\n\n"
        f"Holatini «{BTN_ORDERS}» bo'limidan kuzatishingiz mumkin."
    )
    await state.clear()


# ==============================================================
# PREMIUM HANDLER (handler_premium)
# ==============================================================
router_premium = Router(name="premium")


@router_premium.message(F.text == BTN_PREMIUM)
async def start_premium_flow(message: Message, state: FSMContext):
    await state.clear()
    await state.set_state(BuyPremium.months)
    await message.answer("Necha oylik Premium kerak?", reply_markup=premium_months_menu())


@router_premium.callback_query(BuyPremium.months, F.data.startswith("premmonths:"))
async def premium_months_choice(callback: CallbackQuery, state: FSMContext):
    months = int(callback.data.split(":")[1])
    await state.update_data(months=months)
    await state.set_state(BuyPremium.username)
    await callback.message.answer("Kimga? Telegram username kiriting (masalan: durov).",
                                   reply_markup=cancel_inline())
    await callback.answer()


@router_premium.message(BuyPremium.username, is_free_text)
async def premium_username(message: Message, state: FSMContext):
    username = message.text.strip().lstrip("@")
    if not USERNAME_RE.match(username):
        await message.answer("Username noto'g'ri ko'rinadi. Qayta kiriting (masalan: durov).")
        return

    data = await state.get_data()
    months = data["months"]

    try:
        prices = await client.get_prices()
        base_price = prices["premium"][str(months)]["price"]
    except (SmmUpperError, KeyError) as e:
        await message.answer(f"\u274C Narxni olib bo'lmadi: {e}")
        return

    price = await with_markup(base_price)
    balance = await get_balance(message.from_user.id)

    await state.update_data(username=username, price=price)
    await state.set_state(BuyPremium.confirming)

    text = (
        f"\U0001F464 Kimga: @{username}\n"
        f"\U0001F48E Muddat: {months} oy\n"
        f"\U0001F4B5 Narx: {fmt_money(price)} so'm\n"
        f"\U0001F4B0 Balansingiz: {fmt_money(balance)} so'm"
    )

    if balance < price:
        await message.answer(text + "\n\n" + insufficient_balance(price, balance),
                              reply_markup=balance_menu())
        await state.clear()
        return

    await message.answer(text + "\n\nTasdiqlaysizmi?", reply_markup=confirm_menu("buyprem:confirm"))


@router_premium.callback_query(BuyPremium.confirming, F.data == "buyprem:confirm")
async def confirm_premium(callback: CallbackQuery, state: FSMContext, bot):
    data = await state.get_data()
    username = data["username"]
    months = data["months"]
    est_price = data["price"]

    if not await try_deduct_balance(callback.from_user.id, est_price):
        balance = await get_balance(callback.from_user.id)
        await callback.message.answer(insufficient_balance(est_price, balance))
        await state.clear()
        await callback.answer()
        return

    await callback.answer("Amalga oshirilmoqda...")

    try:
        result = await client.buy_premium(username, months, request_id=new_request_id())
    except SmmUpperError as e:
        await change_balance(callback.from_user.id, est_price)
        await callback.message.answer(f"\u274C Xatolik: {e.message}\n\U0001F4B0 Pulingiz balansga qaytarildi.")
        await state.clear()
        return

    actual_price = await with_markup(result.get("price", 0))
    if actual_price != est_price:
        await change_balance(callback.from_user.id, est_price - actual_price)
    order_pk = await create_order(
        user_id=callback.from_user.id,
        order_type="premium",
        ref=result.get("order_id"),
        server=None,
        price=actual_price,
        details=result,
        status="processing",
    )

    buyer = callback.from_user.full_name
    if callback.from_user.username:
        buyer += f" (@{callback.from_user.username})"
    await notify_channel(bot, channel_premium_notice(buyer, username, months, actual_price))

    await callback.message.answer(
        f"\u2705 Buyurtma qabul qilindi!\n"
        f"\U0001F464 @{username}\n"
        f"\U0001F48E {months} oy Premium\n"
        f"\U0001F4B5 {fmt_money(actual_price)} so'm\n"
        f"\U0001F522 Buyurtma raqami: {result.get('order_id')} (#{order_pk})\n\n"
        f"Holatini «{BTN_ORDERS}» bo'limidan kuzatishingiz mumkin."
    )
    await state.clear()


# ==============================================================
# BUYURTMALAR HANDLER (handler_orders)
# ==============================================================
router_orders = Router(name="orders")

FINAL_STATUSES = {"done", "failed", "error", "refunded"}

_STATUS_EMOJI = {
    "done": "\u2705", "processing": "\u23F3", "pending": "\u23F3", "waiting": "\u23F3",
    "failed": "\u274C", "error": "\u274C", "review": "\U0001F575",
}
_TYPE_LABEL = {
    "number": "\U0001F4F1 Raqam", "stars": "\u2B50 Stars", "premium": "\U0001F48E Premium",
}


@router_orders.message(F.text == BTN_ORDERS)
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
async def check_order(callback: CallbackQuery):
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

    try:
        data = await client.get_order(row["ref"])
    except SmmUpperError as e:
        await callback.answer(f"Xatolik: {e.message}", show_alert=True)
        return

    result = data.get("result", {})
    status = result.get("status", row["status"])
    await update_order_status(order_pk, status, result)
    await callback.answer(f"Holat: {status}", show_alert=True)


# ==============================================================
# ADMIN PANEL HANDLER (handler_admin)
# ==============================================================
router_admin = Router(name="admin")


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


async def _panel_text() -> str:
    try:
        data = await client.get_balance()
        balance = data["result"]["balance"]
        balance_line = f"SmmUpper hisobingizdagi balans: {fmt_money(balance)} so'm"
    except (SmmUpperError, KeyError) as e:
        balance_line = f"SmmUpper balansini olishda xatolik: {e}"

    current_key = await get_setting(SETTINGS_KEY_API_KEY, default=SMMUPPER_API_KEY)
    current_markup = await get_markup_percent()
    current_channel = await get_channel_id()
    card_number, card_holder = await get_card_info()
    fs_channel, _ = await get_force_sub_channel()
    refund_seconds = await get_refund_eligible_seconds()
    extra_admins = await get_extra_admin_ids()
    return (
        f"{ADMIN_TITLE}\n\n{balance_line}\n"
        f"{current_api_key_line(_mask_key(current_key))}\n"
        f"{current_markup_line(current_markup)}\n"
        f"{current_channel_line(current_channel)}\n"
        f"{current_card_line(card_number, card_holder)}\n"
        f"{current_force_sub_line(fs_channel)}\n"
        f"{current_refund_seconds_line(refund_seconds)}\n"
        f"{current_extra_admins_line(extra_admins)}"
    )


async def _show_panel(message: Message):
    await message.answer(await _panel_text(), reply_markup=admin_menu())


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
    await callback.message.edit_text(await _panel_text(), reply_markup=admin_menu())
    await callback.answer()


# ---------- Foydalanuvchi qidirish ----------

@router_admin.callback_query(F.data == "adm:find")
async def admin_find_start(callback: CallbackQuery, state: FSMContext):
    if not await _is_admin(callback.from_user.id):
        await callback.answer("Ruxsat yo'q.", show_alert=True)
        return
    await state.set_state(AdminPanel.find_user)
    await callback.message.answer(ASK_FIND_USER, reply_markup=admin_cancel_menu())
    await callback.answer()


@router_admin.message(AdminPanel.find_user, is_free_text)
async def admin_find_receive(message: Message, state: FSMContext):
    if not await _is_admin(message.from_user.id):
        return
    user = await find_user(message.text.strip())
    await state.clear()
    if not user:
        await message.answer("Foydalanuvchi topilmadi.", reply_markup=admin_cancel_menu())
        return
    await message.answer(admin_user_card(user), reply_markup=admin_cancel_menu())


# ---------- Balans qo'shish/ayirish ----------

@router_admin.callback_query(F.data == "adm:balance")
async def admin_balance_start(callback: CallbackQuery, state: FSMContext):
    if not await _is_admin(callback.from_user.id):
        await callback.answer("Ruxsat yo'q.", show_alert=True)
        return
    await state.set_state(AdminPanel.balance_id)
    await callback.message.answer(ASK_BALANCE_ID, reply_markup=admin_cancel_menu())
    await callback.answer()


@router_admin.message(AdminPanel.balance_id, is_free_text)
async def admin_balance_id_receive(message: Message, state: FSMContext):
    if not await _is_admin(message.from_user.id):
        return
    text = message.text.strip()
    if not text.isdigit():
        await message.answer(NOT_A_VALID_ID, reply_markup=admin_cancel_menu())
        return
    await state.update_data(target_id=int(text))
    await state.set_state(AdminPanel.balance_amount)
    await message.answer(ASK_BALANCE_AMOUNT, reply_markup=admin_cancel_menu())


@router_admin.message(AdminPanel.balance_amount, is_free_text)
async def admin_balance_amount_receive(message: Message, state: FSMContext, bot):
    if not await _is_admin(message.from_user.id):
        return
    text = message.text.strip()
    if not text.lstrip("-").isdigit():
        await message.answer(NOT_A_VALID_AMOUNT, reply_markup=admin_cancel_menu())
        return

    data = await state.get_data()
    user_id = data["target_id"]
    amount = int(text)
    await state.clear()

    await change_balance(user_id, amount)
    new_balance = await get_balance(user_id)
    await message.answer(
        f"\u2705 Bajarildi. {user_id} balansi endi: {fmt_money(new_balance)} so'm",
        reply_markup=admin_cancel_menu(),
    )
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
    await state.set_state(AdminPanel.ban_id)
    await callback.message.answer(ASK_BAN_ID, reply_markup=admin_cancel_menu())
    await callback.answer()


@router_admin.message(AdminPanel.ban_id, is_free_text)
async def admin_ban_receive(message: Message, state: FSMContext):
    if not await _is_admin(message.from_user.id):
        return
    text = message.text.strip()
    if not text.isdigit():
        await message.answer(NOT_A_VALID_ID, reply_markup=admin_cancel_menu())
        return
    await state.clear()
    user_id = int(text)
    await set_banned(user_id, True)
    await message.answer(f"\U0001F6AB {user_id} bloklandi.", reply_markup=admin_cancel_menu())


@router_admin.callback_query(F.data == "adm:unban")
async def admin_unban_start(callback: CallbackQuery, state: FSMContext):
    if not await _is_admin(callback.from_user.id):
        await callback.answer("Ruxsat yo'q.", show_alert=True)
        return
    await state.set_state(AdminPanel.unban_id)
    await callback.message.answer(ASK_UNBAN_ID, reply_markup=admin_cancel_menu())
    await callback.answer()


@router_admin.message(AdminPanel.unban_id, is_free_text)
async def admin_unban_receive(message: Message, state: FSMContext):
    if not await _is_admin(message.from_user.id):
        return
    text = message.text.strip()
    if not text.isdigit():
        await message.answer(NOT_A_VALID_ID, reply_markup=admin_cancel_menu())
        return
    await state.clear()
    user_id = int(text)
    await set_banned(user_id, False)
    await message.answer(f"\u2705 {user_id} blokdan chiqarildi.", reply_markup=admin_cancel_menu())


# ---------- Broadcast ----------

@router_admin.callback_query(F.data == "adm:broadcast")
async def admin_broadcast_start(callback: CallbackQuery, state: FSMContext):
    if not await _is_admin(callback.from_user.id):
        await callback.answer("Ruxsat yo'q.", show_alert=True)
        return
    await state.set_state(AdminPanel.broadcast_text)
    await callback.message.answer(ASK_BROADCAST_TEXT, reply_markup=admin_cancel_menu())
    await callback.answer()


@router_admin.message(AdminPanel.broadcast_text, is_free_text)
async def admin_broadcast_receive(message: Message, state: FSMContext, bot):
    if not await _is_admin(message.from_user.id):
        return
    text = message.text
    await state.clear()

    user_ids = await get_all_user_ids()
    await message.answer(f"\u23F3 {len(user_ids)} foydalanuvchiga yuborilmoqda...")

    sent = 0
    failed = 0
    for user_id in user_ids:
        try:
            await bot.send_message(user_id, text)
            sent += 1
        except Exception:
            failed += 1
        await asyncio.sleep(0.05)

    await message.answer(
        f"\u2705 Yuborildi: {sent}\n\u274C Yuborilmadi: {failed}",
        reply_markup=admin_cancel_menu(),
    )


# ---------- API kalitni sozlash ----------

@router_admin.callback_query(F.data == "adm:apikey")
async def admin_apikey_start(callback: CallbackQuery, state: FSMContext):
    if not await _is_admin(callback.from_user.id):
        await callback.answer("Ruxsat yo'q.", show_alert=True)
        return
    current_key = await get_setting(SETTINGS_KEY_API_KEY, default=SMMUPPER_API_KEY)
    await state.set_state(AdminPanel.api_key)
    await callback.message.answer(
        f"{current_api_key_line(_mask_key(current_key))}\n\n{ASK_API_KEY}",
        reply_markup=admin_cancel_menu(),
    )
    await callback.answer()


@router_admin.message(AdminPanel.api_key, is_free_text)
async def admin_apikey_receive(message: Message, state: FSMContext):
    if not await _is_admin(message.from_user.id):
        return
    new_key = message.text.strip()
    await state.clear()

    if not new_key or len(new_key) < 6:
        await message.answer("\u274C Kalit juda qisqa ko'rinyapti, qaytadan tekshiring.",
                              reply_markup=admin_cancel_menu())
        return

    await set_setting(SETTINGS_KEY_API_KEY, new_key)

    # Yangi kalit ishlayotganini darhol tekshirib ko'ramiz
    try:
        data = await client.get_balance()
        balance = data["result"]["balance"]
        check_line = f"\u2705 Kalit tekshirildi \u2014 SmmUpper balansi: {fmt_money(balance)} so'm"
    except (SmmUpperError, KeyError) as e:
        check_line = f"\u26A0\uFE0F Kalit saqlandi, lekin tekshirishda xatolik: {e}"

    await message.answer(
        api_key_saved(_mask_key(new_key)) + "\n\n" + check_line,
        reply_markup=admin_cancel_menu(),
    )


# ---------- Narx ustamasini sozlash ----------

@router_admin.callback_query(F.data == "adm:markup")
async def admin_markup_start(callback: CallbackQuery, state: FSMContext):
    if not await _is_admin(callback.from_user.id):
        await callback.answer("Ruxsat yo'q.", show_alert=True)
        return
    current = await get_markup_percent()
    await state.set_state(AdminPanel.markup_percent)
    await callback.message.answer(
        f"{current_markup_line(current)}\n\n{ASK_MARKUP_PERCENT}",
        reply_markup=admin_cancel_menu(),
    )
    await callback.answer()


@router_admin.message(AdminPanel.markup_percent, is_free_text)
async def admin_markup_receive(message: Message, state: FSMContext):
    if not await _is_admin(message.from_user.id):
        return
    text = message.text.strip().replace(",", ".")
    try:
        percent = float(text)
    except ValueError:
        await message.answer(NOT_A_VALID_PERCENT, reply_markup=admin_cancel_menu())
        return

    await state.clear()
    await set_setting(SETTINGS_KEY_MARKUP, str(percent))
    await message.answer(markup_saved(percent), reply_markup=admin_cancel_menu())


# ---------- Xarid kanalini sozlash ----------

@router_admin.callback_query(F.data == "adm:channel")
async def admin_channel_start(callback: CallbackQuery, state: FSMContext):
    if not await _is_admin(callback.from_user.id):
        await callback.answer("Ruxsat yo'q.", show_alert=True)
        return
    current = await get_channel_id()
    await state.set_state(AdminPanel.channel_id)
    await callback.message.answer(
        f"{current_channel_line(current)}\n\n{ASK_CHANNEL_ID}",
        reply_markup=admin_cancel_menu(),
    )
    await callback.answer()


@router_admin.message(AdminPanel.channel_id, is_free_text)
async def admin_channel_receive(message: Message, state: FSMContext, bot):
    if not await _is_admin(message.from_user.id):
        return
    text = message.text.strip()
    await state.clear()

    if text == "0":
        await set_setting(SETTINGS_KEY_CHANNEL_ID, "")
        await message.answer(CHANNEL_DISABLED, reply_markup=admin_cancel_menu())
        return

    if not (text.startswith("@") or text.lstrip("-").isdigit()):
        await message.answer(NOT_A_VALID_CHANNEL, reply_markup=admin_cancel_menu())
        return

    await set_setting(SETTINGS_KEY_CHANNEL_ID, text)

    # Kanal to'g'ri sozlanganini va bot xabar yubora olishini darhol tekshiramiz
    try:
        target = int(text) if text.lstrip("-").isdigit() else text
        await bot.send_message(target, "\u2705 Bot shu kanalga xarid xabarlarini yuboradi.")
        await message.answer(channel_saved_ok(text), reply_markup=admin_cancel_menu())
    except Exception as e:
        await message.answer(channel_saved_warning(text, str(e)), reply_markup=admin_cancel_menu())


# ---------- To'lov kartasini sozlash ----------

@router_admin.callback_query(F.data == "adm:card")
async def admin_card_start(callback: CallbackQuery, state: FSMContext):
    if not await _is_admin(callback.from_user.id):
        await callback.answer("Ruxsat yo'q.", show_alert=True)
        return
    card_number, card_holder = await get_card_info()
    await state.set_state(AdminPanel.card_number)
    await callback.message.answer(
        f"{current_card_line(card_number, card_holder)}\n\n{ASK_CARD_NUMBER}",
        reply_markup=admin_cancel_menu(),
    )
    await callback.answer()


@router_admin.message(AdminPanel.card_number, is_free_text)
async def admin_card_number_receive(message: Message, state: FSMContext):
    if not await _is_admin(message.from_user.id):
        return
    card_number = message.text.strip()
    await state.update_data(card_number=card_number)
    await state.set_state(AdminPanel.card_holder)
    await message.answer(ASK_CARD_HOLDER, reply_markup=admin_cancel_menu())


@router_admin.message(AdminPanel.card_holder, is_free_text)
async def admin_card_holder_receive(message: Message, state: FSMContext):
    if not await _is_admin(message.from_user.id):
        return
    card_holder = message.text.strip()
    data = await state.get_data()
    card_number = data["card_number"]
    await state.clear()

    await set_setting(SETTINGS_KEY_CARD_NUMBER, card_number)
    await set_setting(SETTINGS_KEY_CARD_HOLDER, card_holder)
    await message.answer(card_saved(card_number, card_holder), reply_markup=admin_cancel_menu())


# ---------- Majburiy obunani sozlash ----------

@router_admin.callback_query(F.data == "adm:forcesub")
async def admin_forcesub_start(callback: CallbackQuery, state: FSMContext):
    if not await _is_admin(callback.from_user.id):
        await callback.answer("Ruxsat yo'q.", show_alert=True)
        return
    channel, _ = await get_force_sub_channel()
    await state.set_state(AdminPanel.force_sub_channel)
    await callback.message.answer(
        f"{current_force_sub_line(channel)}\n\n{ASK_FORCE_SUB_CHANNEL}",
        reply_markup=admin_cancel_menu(),
    )
    await callback.answer()


@router_admin.message(AdminPanel.force_sub_channel, is_free_text)
async def admin_forcesub_channel_receive(message: Message, state: FSMContext):
    if not await _is_admin(message.from_user.id):
        return
    text = message.text.strip()

    if text == "0":
        await state.clear()
        await set_setting(SETTINGS_KEY_FORCE_SUB_CHANNEL, "")
        await set_setting(SETTINGS_KEY_FORCE_SUB_URL, "")
        await message.answer(force_sub_disabled_ok(), reply_markup=admin_cancel_menu())
        return

    if not (text.startswith("@") or text.lstrip("-").isdigit()):
        await message.answer(NOT_A_VALID_FORCE_SUB_CHANNEL, reply_markup=admin_cancel_menu())
        return

    await state.update_data(fs_channel=text)
    await state.set_state(AdminPanel.force_sub_url)
    await message.answer(ASK_FORCE_SUB_URL, reply_markup=admin_cancel_menu())


@router_admin.message(AdminPanel.force_sub_url, is_free_text)
async def admin_forcesub_url_receive(message: Message, state: FSMContext):
    if not await _is_admin(message.from_user.id):
        return
    data = await state.get_data()
    channel = data["fs_channel"]
    url_text = message.text.strip()
    url = "" if url_text == "-" else url_text
    await state.clear()

    await set_setting(SETTINGS_KEY_FORCE_SUB_CHANNEL, channel)
    await set_setting(SETTINGS_KEY_FORCE_SUB_URL, url)
    _, resolved_url = await get_force_sub_channel()
    await message.answer(force_sub_saved(channel, resolved_url), reply_markup=admin_cancel_menu())


# ---------- Pul qaytarish vaqtini sozlash ----------

@router_admin.callback_query(F.data == "adm:refundtime")
async def admin_refundtime_start(callback: CallbackQuery, state: FSMContext):
    if not await _is_admin(callback.from_user.id):
        await callback.answer("Ruxsat yo'q.", show_alert=True)
        return
    current = await get_refund_eligible_seconds()
    await state.set_state(AdminPanel.refund_seconds)
    await callback.message.answer(
        f"{current_refund_seconds_line(current)}\n\n{ASK_REFUND_SECONDS}",
        reply_markup=admin_cancel_menu(),
    )
    await callback.answer()


@router_admin.message(AdminPanel.refund_seconds, is_free_text)
async def admin_refundtime_receive(message: Message, state: FSMContext):
    if not await _is_admin(message.from_user.id):
        return
    text = message.text.strip()
    if not text.isdigit() or int(text) <= 0:
        await message.answer(NOT_A_VALID_SECONDS, reply_markup=admin_cancel_menu())
        return

    seconds = int(text)
    await state.clear()
    await set_setting(SETTINGS_KEY_REFUND_SECONDS, str(seconds))
    await message.answer(refund_seconds_saved(seconds), reply_markup=admin_cancel_menu())


# ---------- Qo'shimcha adminlar ----------

@router_admin.callback_query(F.data == "adm:toggleadmin")
async def admin_toggleadmin_start(callback: CallbackQuery, state: FSMContext):
    if not await _is_admin(callback.from_user.id):
        await callback.answer("Ruxsat yo'q.", show_alert=True)
        return
    ids = await get_extra_admin_ids()
    await state.set_state(AdminPanel.toggle_admin)
    await callback.message.answer(
        f"{current_extra_admins_line(ids)}\n\n{ASK_TOGGLE_ADMIN}",
        reply_markup=admin_cancel_menu(),
    )
    await callback.answer()


@router_admin.message(AdminPanel.toggle_admin, is_free_text)
async def admin_toggleadmin_receive(message: Message, state: FSMContext):
    if not await _is_admin(message.from_user.id):
        return
    text = message.text.strip()
    if not text.isdigit():
        await message.answer(NOT_A_VALID_ID, reply_markup=admin_cancel_menu())
        return

    user_id = int(text)
    await state.clear()

    if user_id in ADMIN_IDS:
        await message.answer(
            f"\u2139\uFE0F {user_id} allaqachon asosiy (.env) admin \u2014 bu yerdan olib tashlab bo'lmaydi.",
            reply_markup=admin_cancel_menu(),
        )
        return

    added = await toggle_extra_admin(user_id)
    await message.answer(admin_toggled(user_id, added), reply_markup=admin_cancel_menu())


# ---------- Statistika ----------

@router_admin.callback_query(F.data == "adm:stats")
async def admin_stats(callback: CallbackQuery, state: FSMContext):
    if not await _is_admin(callback.from_user.id):
        await callback.answer("Ruxsat yo'q.", show_alert=True)
        return
    await state.clear()
    stats = await get_stats()
    await callback.message.answer(stats_text(stats), reply_markup=admin_cancel_menu())
    await callback.answer()


# ---------- Balansni to'ldirish so'rovlarini tasdiqlash / rad etish ----------

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

    await set_topup_status(topup_id, "approved")
    await change_balance(topup["user_id"], topup["amount"])
    new_balance = await get_balance(topup["user_id"])

    try:
        await bot.send_message(
            topup["user_id"],
            topup_approved_text(topup["amount"], new_balance),
        )
    except Exception:
        pass

    old_caption = callback.message.caption or ""
    await callback.message.edit_caption(caption=old_caption + "\n\n\u2705 TASDIQLANDI")
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

    await set_topup_status(topup_id, "rejected")

    try:
        await bot.send_message(topup["user_id"], TOPUP_REJECTED_USER)
    except Exception:
        pass

    old_caption = callback.message.caption or ""
    await callback.message.edit_caption(caption=old_caption + "\n\n\u274C RAD ETILDI")
    await callback.answer("Rad etildi \u274C")


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

        # Avval kanal sozlanganmi shuni tekshiramiz (bitta baza so'rovi) — bu
        # ko'pchilik holatda (majburiy obuna o'chiq) darhol chiqib ketadi,
        # adminlikni tekshirish uchun QO'SHIMCHA baza so'rovi yubormaydi.
        channel, _ = await get_force_sub_channel()
        if not channel:
            return await handler(event, data)

        if await _is_admin(user.id):
            return await handler(event, data)

        bot = data.get("bot")
        if bot and not await is_subscribed(bot, user.id):
            chat_id = event.chat.id if isinstance(event, Message) else event.message.chat.id
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


async def main():
    logging.basicConfig(level=logging.INFO)

    if not BOT_TOKEN:
        raise RuntimeError(
            "BOT_TOKEN o'rnatilmagan. .env faylida yoki Render Environment Variables'da "
            "BOT_TOKEN ni to'ldiring."
        )

    await init_db()

    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher()

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
    dp.include_router(router_orders)

    await bot.delete_webhook(drop_pending_updates=True)
    await _run_health_server()
    logging.info("Bot ishga tushdi (polling rejimida)")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
