from aiogram import F, Router
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message

import config
import keyboards as kb
import texts as t

router = Router(name="common")


def is_free_text(message: Message) -> bool:
    """FSM holatida 'erkin matn' kutayotgan handlerlar uchun filtr:
    agar foydalanuvchi bosh menyu tugmalaridan birini bossa, bu matnni
    username/summa deb noto'g'ri qabul qilmaslik uchun False qaytaradi —
    shunda navbatdagi (tegishli) handler ishga tushadi.
    """
    return bool(message.text) and message.text not in t.RESERVED_TEXTS


async def notify_channel(bot, text: str):
    """Raqam / Stars / Premium sotib olinganda CHANNEL_ID kanaliga xabar yuboradi.
    .env'da CHANNEL_ID sozlanmagan bo'lsa — hech narsa qilmaydi. Kanalga yuborish
    muvaffaqiyatsiz bo'lsa (masalan, bot hali admin qilib qo'shilmagan bo'lsa),
    xatolik foydalanuvchiga ta'sir qilmasligi uchun jimgina e'tiborsiz qoldiriladi.
    """
    if not config.CHANNEL_ID:
        return
    try:
        await bot.send_message(config.CHANNEL_ID, text)
    except Exception:
        pass


@router.callback_query(F.data == "cancel")
async def cancel_any(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.answer("Bekor qilindi.", reply_markup=kb.main_menu())
    await callback.answer()
