import os

from dotenv import load_dotenv

load_dotenv()

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
