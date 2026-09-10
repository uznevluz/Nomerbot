import asyncio
import json
import time
from typing import Optional

from aiogram import F, Router
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message

import database as db
import keyboards as kb
import texts as t
from config import REFUND_ELIGIBLE_SECONDS
from handler_common import notify_channel
from pricing import with_markup
from smmupper_api import SmmUpperClient, SmmUpperError, new_request_id
from states import BuyNumber

router = Router(name="numbers")
client = SmmUpperClient()

POLL_ATTEMPTS = 12
POLL_DELAY_SECONDS = 5

# asyncio faqat "kuchsiz" (weak) havola saqlaydi — agar boshqa hech kim
# task'ga havola ushlab turmasa, u tugamasdan turib "chiqindi" deb yig'ib
# tashlanishi mumkin. Shuning uchun fon tekshiruvlari shu to'plamda ushlab
# turiladi (tugagach avtomatik chiqarib tashlanadi).
_background_tasks: set = set()


@router.message(F.text == t.BTN_NUMBER)
async def start_number_flow(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Qanday raqam kerak?", reply_markup=kb.number_type_menu())


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


@router.callback_query(F.data == "numtype:regular")
async def choose_regular(callback: CallbackQuery, state: FSMContext):
    await callback.answer("Davlatlar yuklanmoqda...")
    server, countries = await _fetch_countries_with_fallback(1, 2)
    if not countries:
        await callback.message.answer("\u274C Hozircha mavjud davlat yo'q. Birozdan so'ng qayta urinib ko'ring.")
        return

    await state.set_state(BuyNumber.choosing_country)
    await state.update_data(server=server, countries=countries)
    await callback.message.answer("Davlatni tanlang:", reply_markup=kb.countries_menu(server, countries))


@router.callback_query(F.data == "numtype:ready")
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
    await callback.message.answer("Davlatni tanlang:", reply_markup=kb.countries_menu(3, countries))


@router.callback_query(BuyNumber.choosing_country, F.data.startswith("cty:"))
async def choose_country(callback: CallbackQuery, state: FSMContext):
    _, server_str, country = callback.data.split(":")
    server = int(server_str)

    data = await state.get_data()
    countries = data.get("countries", {})
    info = countries.get(country, {})
    base_price = info.get("price", 0)
    price = with_markup(base_price)

    balance = await db.get_balance(callback.from_user.id)
    await state.update_data(country=country, price=price)
    await state.set_state(BuyNumber.confirming)

    text = (
        f"\U0001F30D Davlat: {country}\n"
        f"\U0001F4B5 Narx: {t.fmt_money(price)} so'm\n"
        f"\U0001F4B0 Balansingiz: {t.fmt_money(balance)} so'm"
    )

    if balance < price:
        await callback.message.answer(text + "\n\n" + t.insufficient_balance(price, balance),
                                       reply_markup=kb.balance_menu())
        await state.clear()
        await callback.answer()
        return

    await callback.message.answer(text + "\n\nTasdiqlaysizmi?", reply_markup=kb.confirm_menu("buynum:confirm"))
    await callback.answer()


@router.callback_query(BuyNumber.confirming, F.data == "buynum:confirm")
async def confirm_number(callback: CallbackQuery, state: FSMContext, bot):
    data = await state.get_data()
    server = data["server"]
    country = data["country"]
    est_price = data["price"]

    # Balansni SHU YERDA, bitta atomik amal bilan tekshirib-va-yechib qo'yamiz —
    # SmmUpper'ga ketadigan (sekin) so'rovdan OLDIN. Aks holda ikkita xaridni bir
    # vaqtda tasdiqlash orqali balansni race condition bilan minusga tushirish
    # mumkin bo'lardi.
    if not await db.try_deduct_balance(callback.from_user.id, est_price):
        balance = await db.get_balance(callback.from_user.id)
        await callback.message.answer(t.insufficient_balance(est_price, balance))
        await state.clear()
        await callback.answer()
        return

    await callback.answer("Amalga oshirilmoqda...")

    try:
        result = await client.get_number(server, country, request_id=new_request_id())
    except SmmUpperError as e:
        await db.change_balance(callback.from_user.id, est_price)
        await callback.message.answer(f"\u274C Xatolik: {e.message}\n\U0001F4B0 Pulingiz balansga qaytarildi.")
        await state.clear()
        return

    actual_price = with_markup(result.get("price", 0))
    if actual_price != est_price:
        await db.change_balance(callback.from_user.id, est_price - actual_price)

    ref = result.get("hash_code") or result.get("number") or str(result.get("id"))
    order_pk = await db.create_order(
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
    await notify_channel(bot, t.channel_number_notice(buyer, country, actual_price))

    await callback.message.answer(
        f"\u2705 Raqam olindi: {number}\n"
        f"\U0001F4B5 Narx: {t.fmt_money(actual_price)} so'm\n\n"
        f"\u23F3 SMS kod kutilmoqda...",
        reply_markup=kb.check_code_menu(order_pk),
    )

    await state.clear()
    task = asyncio.create_task(_poll_code(bot, callback.from_user.id, order_pk, server, result))
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)


async def _try_get_code(server: int, result: dict) -> dict:
    if server == 1:
        return await client.get_code(server, hash_code=result.get("hash_code"))
    elif server == 2:
        return await client.get_code(server, number=result.get("number"))
    else:
        return await client.get_code(server, id=result.get("id"))


async def _poll_code(bot, user_id: int, order_pk: int, server: int, result: dict):
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
            await db.update_order_status(order_pk, "done", {**result, "code": code, "password": password})
            try:
                await bot.send_message(user_id, text)
            except Exception:
                pass
            return

    try:
        await bot.send_message(
            user_id,
            "\u231B Kod hali kelmadi. Pastdagi tugma orqali istalgan vaqt tekshirishingiz mumkin.",
            reply_markup=kb.check_code_menu(order_pk),
        )
    except Exception:
        pass


@router.callback_query(F.data.startswith("numcheck:"))
async def manual_check_code(callback: CallbackQuery):
    order_pk = int(callback.data.split(":")[1])
    row = await db.get_order_row(order_pk)
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
        await db.update_order_status(order_pk, "done", {**result, "code": code, "password": password})
        text = f"Kod: {code}"
        if password:
            text += f"\n2FA parol: {password}"
        await callback.answer(text, show_alert=True)
    else:
        await callback.answer("\u23F3 Kod hali kelmagan. Birozdan so'ng qayta tekshiring.", show_alert=True)


@router.callback_query(F.data.startswith("numrefund:"))
async def refund_number_order(callback: CallbackQuery):
    order_pk = int(callback.data.split(":")[1])
    row = await db.get_order_row(order_pk)
    if not row or row["user_id"] != callback.from_user.id:
        await callback.answer("Buyurtma topilmadi.", show_alert=True)
        return
    if row["status"] != "processing":
        await callback.answer("Bu buyurtma allaqachon yakunlangan.", show_alert=True)
        return

    elapsed = int(time.time()) - row["created_at"]
    if elapsed < REFUND_ELIGIBLE_SECONDS:
        wait_min = (REFUND_ELIGIBLE_SECONDS - elapsed) // 60 + 1
        await callback.answer(
            f"\u23F3 Hali erta \u2014 SMS kelishi mumkin. Yana ~{wait_min} daqiqadan so'ng qaytadan urinib ko'ring.",
            show_alert=True,
        )
        return

    refund = await db.refund_order(order_pk, min_age_seconds=REFUND_ELIGIBLE_SECONDS)
    if not refund:
        await callback.answer("Pulni qaytarib bo'lmadi \u2014 balki allaqachon yakunlangan.", show_alert=True)
        return

    new_balance = await db.get_balance(callback.from_user.id)
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await callback.message.answer(
        f"\U0001F4B8 {t.fmt_money(refund['price'])} so'm balansingizga qaytarildi.\n"
        f"\U0001F4B0 Joriy balans: {t.fmt_money(new_balance)} so'm"
    )
    await callback.answer()
