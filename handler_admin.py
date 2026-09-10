import asyncio

from aiogram import F, Router
from aiogram.filters import Command, CommandObject
from aiogram.types import CallbackQuery, Message

import config
import database as db
import texts as t
from smmupper_api import SmmUpperClient, SmmUpperError

router = Router(name="admin")
client = SmmUpperClient()


def _is_admin(user_id: int) -> bool:
    return user_id in config.ADMIN_IDS


@router.message(Command("admin"))
async def admin_panel(message: Message):
    if not _is_admin(message.from_user.id):
        return

    try:
        data = await client.get_balance()
        balance = data["result"]["balance"]
        text = f"\U0001F527 Admin panel\n\nSmmUpper hisobingizdagi balans: {t.fmt_money(balance)} so'm"
    except (SmmUpperError, KeyError) as e:
        text = f"\U0001F527 Admin panel\n\nSmmUpper balansini olishda xatolik: {e}"

    await message.answer(text + t.ADMIN_HELP)


@router.message(Command("find"))
async def find_user_cmd(message: Message, command: CommandObject):
    if not _is_admin(message.from_user.id):
        return
    if not command.args:
        await message.answer("Foydalanish: /find <id yoki @username>")
        return

    user = await db.find_user(command.args.strip())
    if not user:
        await message.answer("Foydalanuvchi topilmadi.")
        return
    await message.answer(t.admin_user_card(user))


@router.message(Command("addbalance"))
async def add_balance_cmd(message: Message, command: CommandObject, bot):
    if not _is_admin(message.from_user.id):
        return
    parts = (command.args or "").split()
    if len(parts) != 2 or not parts[0].isdigit() or not parts[1].lstrip("-").isdigit():
        await message.answer("Foydalanish: /addbalance <user_id> <miqdor>\nMasalan: /addbalance 123456789 50000\n(ayirish uchun manfiy son: /addbalance 123456789 -20000)")
        return

    user_id = int(parts[0])
    amount = int(parts[1])
    await db.change_balance(user_id, amount)
    new_balance = await db.get_balance(user_id)
    await message.answer(f"\u2705 Bajarildi. {user_id} balansi endi: {t.fmt_money(new_balance)} so'm")

    try:
        await bot.send_message(user_id, t.balance_adjusted_by_admin(amount, new_balance))
    except Exception:
        pass


@router.message(Command("ban"))
async def ban_cmd(message: Message, command: CommandObject):
    if not _is_admin(message.from_user.id):
        return
    if not command.args or not command.args.strip().isdigit():
        await message.answer("Foydalanish: /ban <user_id>")
        return
    user_id = int(command.args.strip())
    await db.set_banned(user_id, True)
    await message.answer(f"\U0001F6AB {user_id} bloklandi.")


@router.message(Command("unban"))
async def unban_cmd(message: Message, command: CommandObject):
    if not _is_admin(message.from_user.id):
        return
    if not command.args or not command.args.strip().isdigit():
        await message.answer("Foydalanish: /unban <user_id>")
        return
    user_id = int(command.args.strip())
    await db.set_banned(user_id, False)
    await message.answer(f"\u2705 {user_id} blokdan chiqarildi.")


@router.message(Command("broadcast"))
async def broadcast_cmd(message: Message, command: CommandObject, bot):
    if not _is_admin(message.from_user.id):
        return
    if not command.args:
        await message.answer("Foydalanish: /broadcast <xabar matni>")
        return

    user_ids = await db.get_all_user_ids()
    await message.answer(f"\u23F3 {len(user_ids)} foydalanuvchiga yuborilmoqda...")

    sent = 0
    failed = 0
    for user_id in user_ids:
        try:
            await bot.send_message(user_id, command.args)
            sent += 1
        except Exception:
            failed += 1
        await asyncio.sleep(0.05)

    await message.answer(f"\u2705 Yuborildi: {sent}\n\u274C Yuborilmadi: {failed}")


@router.callback_query(F.data.startswith("topup:approve:"))
async def approve_topup(callback: CallbackQuery, bot):
    if not _is_admin(callback.from_user.id):
        await callback.answer("Ruxsat yo'q.", show_alert=True)
        return

    topup_id = int(callback.data.split(":")[2])
    topup = await db.get_topup(topup_id)
    if not topup:
        await callback.answer("So'rov topilmadi.", show_alert=True)
        return
    if topup["status"] != "pending":
        await callback.answer("Bu so'rov allaqachon ko'rib chiqilgan.", show_alert=True)
        return

    await db.set_topup_status(topup_id, "approved")
    await db.change_balance(topup["user_id"], topup["amount"])
    new_balance = await db.get_balance(topup["user_id"])

    try:
        await bot.send_message(
            topup["user_id"],
            t.topup_approved_text(topup["amount"], new_balance),
        )
    except Exception:
        pass

    old_caption = callback.message.caption or ""
    await callback.message.edit_caption(caption=old_caption + "\n\n\u2705 TASDIQLANDI")
    await callback.answer("Tasdiqlandi \u2705")


@router.callback_query(F.data.startswith("topup:reject:"))
async def reject_topup(callback: CallbackQuery, bot):
    if not _is_admin(callback.from_user.id):
        await callback.answer("Ruxsat yo'q.", show_alert=True)
        return

    topup_id = int(callback.data.split(":")[2])
    topup = await db.get_topup(topup_id)
    if not topup:
        await callback.answer("So'rov topilmadi.", show_alert=True)
        return
    if topup["status"] != "pending":
        await callback.answer("Bu so'rov allaqachon ko'rib chiqilgan.", show_alert=True)
        return

    await db.set_topup_status(topup_id, "rejected")

    try:
        await bot.send_message(topup["user_id"], t.TOPUP_REJECTED_USER)
    except Exception:
        pass

    old_caption = callback.message.caption or ""
    await callback.message.edit_caption(caption=old_caption + "\n\n\u274C RAD ETILDI")
    await callback.answer("Rad etildi \u274C")
