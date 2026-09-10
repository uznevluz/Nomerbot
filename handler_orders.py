from aiogram import F, Router
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

import database as db
import texts as t
from smmupper_api import SmmUpperClient, SmmUpperError

router = Router(name="orders")
client = SmmUpperClient()

FINAL_STATUSES = {"done", "failed", "error"}

_STATUS_EMOJI = {
    "done": "\u2705", "processing": "\u23F3", "pending": "\u23F3", "waiting": "\u23F3",
    "failed": "\u274C", "error": "\u274C", "review": "\U0001F575",
}
_TYPE_LABEL = {
    "number": "\U0001F4F1 Raqam", "stars": "\u2B50 Stars", "premium": "\U0001F48E Premium",
}


@router.message(F.text == t.BTN_ORDERS)
async def list_orders(message: Message, state: FSMContext):
    await state.clear()
    rows = await db.list_orders(message.from_user.id, limit=10)
    if not rows:
        await message.answer(t.NO_ORDERS_YET)
        return

    lines = ["\U0001F4CB Oxirgi buyurtmalaringiz:"]
    buttons = []
    for row in rows:
        status = row["status"]
        lines.append(
            f"\n{_STATUS_EMOJI.get(status, '\u2754')} {_TYPE_LABEL.get(row['order_type'], row['order_type'])} "
            f"— {t.fmt_money(row['price'])} so'm — #{row['id']} ({status})"
        )
        if status not in FINAL_STATUSES and row["ref"]:
            buttons.append([InlineKeyboardButton(
                text=f"\U0001F504 #{row['id']} holatini tekshirish",
                callback_data=f"ordercheck:{row['id']}",
            )])

    markup = InlineKeyboardMarkup(inline_keyboard=buttons) if buttons else None
    await message.answer("\n".join(lines), reply_markup=markup)


@router.callback_query(F.data.startswith("ordercheck:"))
async def check_order(callback: CallbackQuery):
    order_pk = int(callback.data.split(":")[1])
    row = await db.get_order_row(order_pk)
    if not row or row["user_id"] != callback.from_user.id:
        await callback.answer("Topilmadi.", show_alert=True)
        return

    try:
        data = await client.get_order(row["ref"])
    except SmmUpperError as e:
        await callback.answer(f"Xatolik: {e.message}", show_alert=True)
        return

    result = data.get("result", {})
    status = result.get("status", row["status"])
    await db.update_order_status(order_pk, status, result)
    await callback.answer(f"Holat: {status}", show_alert=True)
