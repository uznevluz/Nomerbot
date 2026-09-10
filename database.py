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
import json
import time
from typing import Optional

import aiosqlite
import asyncpg

from config import DB_PATH, DATABASE_URL

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
