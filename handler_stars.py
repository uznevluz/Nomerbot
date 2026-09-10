import re

from aiogram import F, Router
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message

import database as db
import keyboards as kb
import texts as t
from handler_common import is_free_text, notify_channel
from pricing import with_markup
from smmupper_api import SmmUpperClient, SmmUpperError, new_request_id
from states import BuyStars

router = Router(name="stars")
client = SmmUpperClient()

USERNAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{4,31}$")
MIN_STARS = 50


@router.message(F.text == t.BTN_STARS)
async def start_stars_flow(message: Message, state: FSMContext):
    await state.clear()
    await state.set_state(BuyStars.username)
    await message.answer(
        "Kimga Stars sotib olamiz? Telegram username kiriting (masalan: durov).",
        reply_markup=kb.cancel_inline(),
    )


@router.message(BuyStars.username, is_free_text)
async def stars_username(message: Message, state: FSMContext):
    username = message.text.strip().lstrip("@")
    if not USERNAME_RE.match(username):
        await message.answer("Username noto'g'ri ko'rinadi. Qayta kiriting (masalan: durov).")
        return

    await state.update_data(username=username)
    await state.set_state(BuyStars.amount)
    await message.answer(
        f"Nechta Stars? (min {MIN_STARS}, yoki summani yozib yuboring)",
        reply_markup=kb.stars_amount_menu(),
    )


@router.callback_query(BuyStars.amount, F.data.startswith("starsamt:"))
async def stars_amount_choice(callback: CallbackQuery, state: FSMContext):
    value = callback.data.split(":", 1)[1]
    if value == "custom":
        await callback.message.answer(f"Nechta Stars kerak? Sonini yozing (min {MIN_STARS}).")
        await callback.answer()
        return

    await callback.answer()
    await _process_stars_amount(callback.message, state, int(value), callback.from_user.id)


@router.message(BuyStars.amount, is_free_text)
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

    price = with_markup(price_per_star * amount)
    balance = await db.get_balance(user_id)
    data = await state.get_data()
    username = data.get("username")

    await state.update_data(amount=amount, price=price)
    await state.set_state(BuyStars.confirming)

    text = (
        f"\U0001F464 Kimga: @{username}\n"
        f"\u2B50 Miqdor: {amount}\n"
        f"\U0001F4B5 Narx: {t.fmt_money(price)} so'm\n"
        f"\U0001F4B0 Balansingiz: {t.fmt_money(balance)} so'm"
    )

    if balance < price:
        await message.answer(text + "\n\n" + t.insufficient_balance(price, balance),
                              reply_markup=kb.balance_menu())
        await state.clear()
        return

    await message.answer(text + "\n\nTasdiqlaysizmi?", reply_markup=kb.confirm_menu("buystars:confirm"))


@router.callback_query(BuyStars.confirming, F.data == "buystars:confirm")
async def confirm_stars(callback: CallbackQuery, state: FSMContext, bot):
    data = await state.get_data()
    username = data["username"]
    amount = data["amount"]
    est_price = data["price"]

    if not await db.try_deduct_balance(callback.from_user.id, est_price):
        balance = await db.get_balance(callback.from_user.id)
        await callback.message.answer(t.insufficient_balance(est_price, balance))
        await state.clear()
        await callback.answer()
        return

    await callback.answer("Amalga oshirilmoqda...")

    try:
        result = await client.buy_stars(username, amount, request_id=new_request_id())
    except SmmUpperError as e:
        await db.change_balance(callback.from_user.id, est_price)
        await callback.message.answer(f"\u274C Xatolik: {e.message}\n\U0001F4B0 Pulingiz balansga qaytarildi.")
        await state.clear()
        return

    actual_price = with_markup(result.get("price", 0))
    if actual_price != est_price:
        await db.change_balance(callback.from_user.id, est_price - actual_price)
    order_pk = await db.create_order(
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
    await notify_channel(bot, t.channel_stars_notice(buyer, username, amount, actual_price))

    await callback.message.answer(
        f"\u2705 Buyurtma qabul qilindi!\n"
        f"\U0001F464 @{username}\n"
        f"\u2B50 {amount} Stars\n"
        f"\U0001F4B5 {t.fmt_money(actual_price)} so'm\n"
        f"\U0001F522 Buyurtma raqami: {result.get('order_id')} (#{order_pk})\n\n"
        f"Holatini «{t.BTN_ORDERS}» bo'limidan kuzatishingiz mumkin."
    )
    await state.clear()
