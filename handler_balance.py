from aiogram import F, Router
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message

import config
import database as db
import keyboards as kb
import texts as t
from handler_common import is_free_text
from states import TopUp

router = Router(name="balance")


@router.message(F.text == t.BTN_BALANCE)
async def show_balance(message: Message, state: FSMContext):
    await state.clear()
    balance = await db.get_balance(message.from_user.id)
    await message.answer(t.balance_text(balance), reply_markup=kb.balance_menu())


@router.callback_query(F.data == "topup:start")
async def topup_start(callback: CallbackQuery, state: FSMContext):
    await state.set_state(TopUp.amount)
    await callback.message.answer(t.TOPUP_ASK_AMOUNT, reply_markup=kb.cancel_inline())
    await callback.answer()


@router.message(TopUp.amount, is_free_text)
async def topup_amount(message: Message, state: FSMContext):
    text = message.text.strip().replace(" ", "")
    if not text.isdigit() or int(text) <= 0:
        await message.answer(t.TOPUP_NOT_A_NUMBER)
        return

    amount = int(text)
    await state.update_data(amount=amount)
    await state.set_state(TopUp.photo)
    await message.answer(t.topup_instructions(amount, config.CARD_NUMBER, config.CARD_HOLDER))


@router.message(TopUp.photo, F.photo)
async def topup_photo(message: Message, state: FSMContext, bot):
    data = await state.get_data()
    amount = data.get("amount", 0)
    photo_file_id = message.photo[-1].file_id

    topup_id = await db.create_topup(message.from_user.id, amount, photo_file_id)
    await state.clear()

    other_pending = await db.count_other_pending_topups(message.from_user.id, topup_id)

    caption = (
        f"\U0001F195 Balans to'ldirish so'rovi\n"
        f"\U0001F464 {message.from_user.full_name} (ID: {message.from_user.id})\n"
        f"\U0001F4B0 Summa: {t.fmt_money(amount)} so'm\n"
        f"\U0001F522 So'rov ID: {topup_id}"
    )
    if other_pending:
        caption += t.duplicate_topup_warning(other_pending)

    for admin_id in config.ADMIN_IDS:
        try:
            await bot.send_photo(
                admin_id, photo_file_id, caption=caption,
                reply_markup=kb.topup_review_menu(topup_id),
            )
        except Exception:
            pass

    await message.answer(t.TOPUP_SENT_TO_ADMIN, reply_markup=kb.main_menu())


@router.message(TopUp.photo, is_free_text)
async def topup_photo_missing(message: Message):
    await message.answer(t.TOPUP_SEND_PHOTO)
