"""
SmmUpper — Hamkorlik API v2 uchun klient.
Hujjat: https://smmupper.uz/api/v2/docs

Barcha so'rovlar GET, javob JSON. Har bir so'rovda api_key bo'lishi shart.
Muvaffaqiyat: {"success": true, ...}, xato: {"success": false, "error": "..."}.
"""
import asyncio
import uuid
from typing import Optional

import aiohttp

from config import SMMUPPER_API_KEY, SMMUPPER_BASE_URL


class SmmUpperError(Exception):
    """SmmUpper API 'success': false qaytarganda ko'tariladi."""

    def __init__(self, message: str, status: Optional[int] = None):
        self.message = message
        self.status = status
        super().__init__(message)


def new_request_id() -> str:
    """Xarid so'rovlari (buyStars/buyPremium/getNumber) uchun noyob request_id.
    Hujjatga ko'ra 6-64 belgi: harf, raqam, . _ - bo'lishi kerak — uuid4().hex (32 ta belgi) mos keladi.
    """
    return uuid.uuid4().hex


class SmmUpperClient:
    def __init__(self, api_key: str = SMMUPPER_API_KEY, base_url: str = SMMUPPER_BASE_URL):
        self.api_key = api_key
        self.base_url = base_url

    async def _request(self, action: str, *, raise_on_error: bool = True,
                        _retry_on_limit: bool = True, **params) -> dict:
        query = {"action": action, "api_key": self.api_key}
        for key, value in params.items():
            if value is not None:
                query[key] = value

        timeout = aiohttp.ClientTimeout(total=20)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(self.base_url, params=query) as resp:
                if resp.status == 429 and _retry_on_limit:
                    retry_after = 5
                    try:
                        retry_after = int(resp.headers.get("Retry-After", "5"))
                    except ValueError:
                        pass
                    await asyncio.sleep(min(retry_after, 15))
                    return await self._request(
                        action, raise_on_error=raise_on_error, _retry_on_limit=False, **params
                    )

                try:
                    data = await resp.json(content_type=None)
                except Exception:
                    text = await resp.text()
                    raise SmmUpperError(f"Server javobini o'qib bo'lmadi: {text[:200]}", resp.status)

                if raise_on_error and not data.get("success"):
                    raise SmmUpperError(data.get("error", "Noma'lum xato"), resp.status)

                return data

    # 1. Balans (bizning hamkor hisobimizning SmmUpper'dagi balansi)
    async def get_balance(self) -> dict:
        return await self._request("getBalance")

    # 2. Narxlar (Stars / Premium)
    async def get_prices(self) -> dict:
        return await self._request("getPrices")

    # 3. Davlatlar ro'yxati
    async def available_countries(self, server: int) -> dict:
        return await self._request("available_countries", server=server)

    # 4. Raqam olish
    async def get_number(self, server: int, country: str, request_id: Optional[str] = None) -> dict:
        return await self._request(
            "getNumber", server=server, country=country, request_id=request_id
        )

    # 5. SMS kodni olish — status:"waiting" xato emas, shuning uchun raise_on_error=False
    async def get_code(self, server: int, *, hash_code: Optional[str] = None,
                        number: Optional[str] = None, id: Optional[str] = None) -> dict:
        params = {"server": server}
        if server == 1:
            params["hash_code"] = hash_code
        elif server == 2:
            params["number"] = number
        else:
            params["id"] = id
        return await self._request("getCode", raise_on_error=False, **params)

    # 6. Stars sotib olish
    async def buy_stars(self, username: str, amount: int, request_id: Optional[str] = None) -> dict:
        return await self._request(
            "buyStars", username=username, amount=amount, request_id=request_id
        )

    # 7. Premium sotib olish
    async def buy_premium(self, username: str, months: int, request_id: Optional[str] = None) -> dict:
        return await self._request(
            "buyPremium", username=username, months=months, request_id=request_id
        )

    # 8. Buyurtma holati
    async def get_order(self, order_id) -> dict:
        return await self._request("getOrder", order_id=order_id)
