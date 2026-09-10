from aiogram import F, Router
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.types import Message

import database as db
import keyboards as kb
import texts as t

router = Router(name="start")


@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    await db.ensure_user(message.from_user.id, message.from_user.username, message.from_user.full_name)
    await message.answer(t.WELCOME, reply_markup=kb.main_menu())


@router.message(F.text == t.BTN_MENU)
async def show_menu(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(t.MENU_TITLE, reply_markup=kb.main_menu())
