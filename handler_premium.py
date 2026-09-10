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
from states import BuyPremium

router = Router(name="premium")
client = SmmUpperClient()

USERNAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{4,31}$")


@router.message(F.text == t.BTN_PREMIUM)
async def start_premium_flow(message: Message, state: FSMContext):
    await state.clear()
    await state.set_state(BuyPremium.months)
    await message.answer("Necha oylik Premium kerak?", reply_markup=kb.premium_months_menu())


@router.callback_query(BuyPremium.months, F.data.startswith("premmonths:"))
async def premium_months_choice(callback: CallbackQuery, state: FSMContext):
    months = int(callback.data.split(":")[1])
    await state.update_data(months=months)
    await state.set_state(BuyPremium.username)
    await callback.message.answer("Kimga? Telegram username kiriting (masalan: durov).")
    await callback.answer()


@router.message(BuyPremium.username, is_free_text)
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

    price = with_markup(base_price)
    balance = await db.get_balance(message.from_user.id)

    await state.update_data(username=username, price=price)
    await state.set_state(BuyPremium.confirming)

    text = (
        f"\U0001F464 Kimga: @{username}\n"
        f"\U0001F48E Muddat: {months} oy\n"
        f"\U0001F4B5 Narx: {t.fmt_money(price)} so'm\n"
        f"\U0001F4B0 Balansingiz: {t.fmt_money(balance)} so'm"
    )

    if balance < price:
        await message.answer(text + "\n\n" + t.insufficient_balance(price, balance),
                              reply_markup=kb.balance_menu())
        await state.clear()
        return

    await message.answer(text + "\n\nTasdiqlaysizmi?", reply_markup=kb.confirm_menu("buyprem:confirm"))


@router.callback_query(BuyPremium.confirming, F.data == "buyprem:confirm")
async def confirm_premium(callback: CallbackQuery, state: FSMContext, bot):
    data = await state.get_data()
    username = data["username"]
    months = data["months"]
    est_price = data["price"]

    if not await db.try_deduct_balance(callback.from_user.id, est_price):
        balance = await db.get_balance(callback.from_user.id)
        await callback.message.answer(t.insufficient_balance(est_price, balance))
        await state.clear()
        await callback.answer()
        return

    await callback.answer("Amalga oshirilmoqda...")

    try:
        result = await client.buy_premium(username, months, request_id=new_request_id())
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
    await notify_channel(bot, t.channel_premium_notice(buyer, username, months, actual_price))

    await callback.message.answer(
        f"\u2705 Buyurtma qabul qilindi!\n"
        f"\U0001F464 @{username}\n"
        f"\U0001F48E {months} oy Premium\n"
        f"\U0001F4B5 {t.fmt_money(actual_price)} so'm\n"
        f"\U0001F522 Buyurtma raqami: {result.get('order_id')} (#{order_pk})\n\n"
        f"Holatini «{t.BTN_ORDERS}» bo'limidan kuzatishingiz mumkin."
    )
    await state.clear()
