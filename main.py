import asyncio
import logging
import os
import time
from typing import Any, Awaitable, Callable, Dict

from aiogram import BaseMiddleware, Bot, Dispatcher
from aiogram.types import CallbackQuery, TelegramObject
from aiohttp import web

import database as db
from config import BOT_TOKEN, THROTTLE_SECONDS
import handler_admin as admin
import handler_balance as balance
import handler_common as common
import handler_numbers as numbers
import handler_orders as orders
import handler_premium as premium
import handler_start as start
import handler_stars as stars


class BanMiddleware(BaseMiddleware):
    """Bloklangan foydalanuvchining hech qanday xabari/tugma bosishi
    handlerlarga yetib bormaydi — jimgina e'tiborsiz qoldiriladi."""

    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any],
    ) -> Any:
        user = data.get("event_from_user")
        if user and await db.is_banned(user.id):
            return None
        return await handler(event, data)


class ThrottleMiddleware(BaseMiddleware):
    """Bitta foydalanuvchidan THROTTLE_SECONDS ichida kelgan ortiqcha
    so'rovlarni e'tiborsiz qoldiradi (spam/flood himoyasi). Xotirada
    saqlanadi — bot qayta ishga tushsa tozalanadi, bu muhim emas."""

    def __init__(self, min_interval: float = THROTTLE_SECONDS):
        self.min_interval = min_interval
        self._last_seen: Dict[int, float] = {}

    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any],
    ) -> Any:
        user = data.get("event_from_user")
        if user:
            now = time.monotonic()
            last = self._last_seen.get(user.id, 0.0)
            if now - last < self.min_interval:
                if isinstance(event, CallbackQuery):
                    await event.answer()
                return None
            self._last_seen[user.id] = now
        return await handler(event, data)


async def _health(request):
    return web.Response(text="Bot ishlayapti \u2705")


async def _run_health_server():
    """Render 'Web Service' turi deploy paytida kamida bitta ochiq portni talab
    qiladi (aks holda 'port scan timeout' bilan servisni o'chirib qo'yadi),
    lekin bot faqat polling orqali ishlaydi va o'zidan HTTP so'rov kutmaydi.
    Shuning uchun shu yengil health-check server ochib qo'yiladi — u haqiqiy
    trafik uchun emas, faqat Render'ning port tekshiruvidan o'tish uchun kerak.
    """
    app = web.Application()
    app.router.add_get("/", _health)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.getenv("PORT", "10000"))
    site = web.TCPSite(runner, host="0.0.0.0", port=port)
    await site.start()
    logging.info(f"Health-check server {port}-portda ochildi")


async def main():
    logging.basicConfig(level=logging.INFO)

    if not BOT_TOKEN:
        raise RuntimeError(
            "BOT_TOKEN o'rnatilmagan. .env faylida yoki Render Environment Variables'da "
            "BOT_TOKEN ni to'ldiring."
        )

    await db.init_db()

    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher()

    # Har bir xabar/tugma bosishi routerlarga yetib borishidan OLDIN: avval
    # bloklangan-emasligi tekshiriladi, keyin spam/flood cheklovi qo'llanadi.
    ban_middleware = BanMiddleware()
    throttle_middleware = ThrottleMiddleware()
    dp.message.outer_middleware(ban_middleware)
    dp.callback_query.outer_middleware(ban_middleware)
    dp.message.outer_middleware(throttle_middleware)
    dp.callback_query.outer_middleware(throttle_middleware)

    # Diqqat: common.router (bosh menyu tugmalari uchun umumiy filtr) va
    # admin.router birinchi bo'lib qo'shiladi, keyin qolgan bo'limlar.
    dp.include_router(common.router)
    dp.include_router(admin.router)
    dp.include_router(start.router)
    dp.include_router(balance.router)
    dp.include_router(numbers.router)
    dp.include_router(stars.router)
    dp.include_router(premium.router)
    dp.include_router(orders.router)

    await bot.delete_webhook(drop_pending_updates=True)
    await _run_health_server()
    logging.info("Bot ishga tushdi (polling rejimida)")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
