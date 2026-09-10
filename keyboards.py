from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
)

import texts as t

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
            [KeyboardButton(text=t.BTN_BALANCE), KeyboardButton(text=t.BTN_ORDERS)],
            [KeyboardButton(text=t.BTN_NUMBER)],
            [KeyboardButton(text=t.BTN_STARS), KeyboardButton(text=t.BTN_PREMIUM)],
            [KeyboardButton(text=t.BTN_MENU)],
        ],
        resize_keyboard=True,
    )


def balance_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t.BTN_TOPUP, callback_data="topup:start", style=STYLE_PRIMARY)],
    ])


def cancel_inline() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t.BTN_CANCEL, callback_data="cancel", style=STYLE_DANGER)],
    ])


def number_type_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="\U0001F4F1 Oddiy raqam (SMS kod uchun)", callback_data="numtype:regular", style=STYLE_PRIMARY)],
        [InlineKeyboardButton(text="\U0001F510 Tayyor akkaunt (2FA parol bilan)", callback_data="numtype:ready", style=STYLE_PRIMARY)],
        [InlineKeyboardButton(text=t.BTN_CANCEL, callback_data="cancel", style=STYLE_DANGER)],
    ])


def countries_menu(server: int, countries: dict) -> InlineKeyboardMarkup:
    rows = []
    row = []
    for code, info in countries.items():
        price = info.get("price", "?")
        label = f"{code} — {t.fmt_money(price)} so'm" if isinstance(price, (int, float)) else str(code)
        row.append(InlineKeyboardButton(text=label, callback_data=f"cty:{server}:{code}", style=STYLE_PRIMARY))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton(text=t.BTN_CANCEL, callback_data="cancel", style=STYLE_DANGER)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def confirm_menu(confirm_data: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t.BTN_CONFIRM, callback_data=confirm_data, style=STYLE_SUCCESS)],
        [InlineKeyboardButton(text=t.BTN_CANCEL, callback_data="cancel", style=STYLE_DANGER)],
    ])


def stars_amount_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="50", callback_data="starsamt:50", style=STYLE_PRIMARY),
            InlineKeyboardButton(text="100", callback_data="starsamt:100", style=STYLE_PRIMARY),
            InlineKeyboardButton(text="500", callback_data="starsamt:500", style=STYLE_PRIMARY),
        ],
        [InlineKeyboardButton(text="\u270F\uFE0F Boshqa summa", callback_data="starsamt:custom", style=STYLE_PRIMARY)],
        [InlineKeyboardButton(text=t.BTN_CANCEL, callback_data="cancel", style=STYLE_DANGER)],
    ])


def premium_months_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="3 oy", callback_data="premmonths:3", style=STYLE_PRIMARY),
            InlineKeyboardButton(text="6 oy", callback_data="premmonths:6", style=STYLE_PRIMARY),
            InlineKeyboardButton(text="12 oy", callback_data="premmonths:12", style=STYLE_PRIMARY),
        ],
        [InlineKeyboardButton(text=t.BTN_CANCEL, callback_data="cancel", style=STYLE_DANGER)],
    ])


def check_code_menu(order_pk: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t.BTN_CHECK_CODE, callback_data=f"numcheck:{order_pk}", style=STYLE_PRIMARY)],
        [InlineKeyboardButton(text="\U0001F4B8 Pulni qaytarish", callback_data=f"numrefund:{order_pk}", style=STYLE_DANGER)],
    ])


def topup_review_menu(topup_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="\u2705 Tasdiqlash", callback_data=f"topup:approve:{topup_id}", style=STYLE_SUCCESS),
            InlineKeyboardButton(text="\u274C Rad etish", callback_data=f"topup:reject:{topup_id}", style=STYLE_DANGER),
        ],
    ])
