WELCOME = (
    "\U0001F44B Assalomu alaykum!\n\n"
    "Bu bot orqali siz:\n"
    "\U0001F4F1 Virtual raqam (SMS kod uchun)\n"
    "\u2B50 Telegram Stars\n"
    "\U0001F48E Telegram Premium\n\n"
    "sotib olishingiz mumkin. Quyidagi menyudan tanlang \U0001F447"
)

MENU_TITLE = "Bosh menyu:"

BTN_MENU = "\U0001F3E0 Menyu"
BTN_BALANCE = "\U0001F4B0 Balans"
BTN_TOPUP = "\U0001F4B3 Balansni to'ldirish"
BTN_NUMBER = "\U0001F4F1 Raqam sotib olish"
BTN_STARS = "\u2B50 Stars sotib olish"
BTN_PREMIUM = "\U0001F48E Premium sotib olish"
BTN_ORDERS = "\U0001F4CB Buyurtmalarim"
BTN_CANCEL = "\u274C Bekor qilish"
BTN_CHECK_CODE = "\U0001F504 Kodni tekshirish"
BTN_CONFIRM = "\u2705 Tasdiqlash"

# Bosh menyudagi tugma matnlari — bular FSM holatida turgan "erkin matn"
# handlerlar tomonidan "username" yoki "summa" deb noto'g'ri qabul qilinmasligi kerak.
RESERVED_TEXTS = {
    BTN_MENU, BTN_BALANCE, BTN_TOPUP, BTN_NUMBER,
    BTN_STARS, BTN_PREMIUM, BTN_ORDERS, BTN_CANCEL,
}


def fmt_money(amount) -> str:
    return f"{int(amount):,}".replace(",", " ")


def balance_text(amount: int) -> str:
    return f"\U0001F4B0 Balansingiz: {fmt_money(amount)} so'm"


def insufficient_balance(price: int, balance: int) -> str:
    return (
        f"\u274C Balansingiz yetarli emas. "
        f"Kerakli summa: {fmt_money(price)} so'm, sizda: {fmt_money(balance)} so'm."
    )


TOPUP_ASK_AMOUNT = "Necha so'mga balansni to'ldirmoqchisiz? Summani kiriting (masalan: 50000)."
TOPUP_NOT_A_NUMBER = "Iltimos, faqat musbat son kiriting. Masalan: 50000"


def topup_instructions(amount: int, card_number: str, card_holder: str) -> str:
    return (
        f"\U0001F4B3 {fmt_money(amount)} so'mni quyidagi kartaga o'tkazing:\n\n"
        f"{card_number}\n"
        f"{card_holder}\n\n"
        f"To'lov chekining skrinshotini shu yerga (rasm sifatida) yuboring \U0001F447"
    )


TOPUP_SENT_TO_ADMIN = "\u2705 So'rovingiz adminga yuborildi. Tasdiqlangach, balansingiz to'ldiriladi."
TOPUP_SEND_PHOTO = "Iltimos, to'lov chekining skrinshotini rasm sifatida yuboring."
TOPUP_REJECTED_USER = "\u274C Balansni to'ldirish so'rovingiz rad etildi. Admin bilan bog'laning."


def topup_approved_text(amount: int, balance: int) -> str:
    return (
        f"\u2705 Balansingiz {fmt_money(amount)} so'mga to'ldirildi. "
        f"Yangi balans: {fmt_money(balance)} so'm"
    )


NO_ORDERS_YET = "Hali buyurtmalar yo'q."


def channel_number_notice(buyer: str, country: str, price: int) -> str:
    return (
        f"\U0001F195 Yangi buyurtma \u2014 \U0001F4F1 Raqam\n"
        f"\U0001F464 {buyer}\n"
        f"\U0001F30D Davlat: {country}\n"
        f"\U0001F4B5 Narx: {fmt_money(price)} so'm"
    )


def channel_stars_notice(buyer: str, target_username: str, amount: int, price: int) -> str:
    return (
        f"\U0001F195 Yangi buyurtma \u2014 \u2B50 Stars\n"
        f"\U0001F464 {buyer}\n"
        f"\U0001F3AF Kimga: @{target_username}\n"
        f"\u2B50 Miqdor: {amount}\n"
        f"\U0001F4B5 Narx: {fmt_money(price)} so'm"
    )


def channel_premium_notice(buyer: str, target_username: str, months: int, price: int) -> str:
    return (
        f"\U0001F195 Yangi buyurtma \u2014 \U0001F48E Premium\n"
        f"\U0001F464 {buyer}\n"
        f"\U0001F3AF Kimga: @{target_username}\n"
        f"\U0001F4C5 Muddat: {months} oy\n"
        f"\U0001F4B5 Narx: {fmt_money(price)} so'm"
    )


def admin_user_card(user: dict) -> str:
    banned = "\U0001F6AB Ha" if user.get("banned") else "Yo'q"
    username = f"@{user['username']}" if user.get("username") else "\u2014"
    return (
        f"\U0001F464 {user.get('full_name') or '\u2014'} ({username})\n"
        f"\U0001F522 ID: {user['user_id']}\n"
        f"\U0001F4B0 Balans: {fmt_money(user['balance'])} so'm\n"
        f"\U0001F4E6 Buyurtmalar soni: {user.get('order_count', 0)}\n"
        f"\U0001F6AB Bloklangan: {banned}"
    )


def balance_adjusted_by_admin(amount: int, new_balance: int) -> str:
    if amount >= 0:
        return (
            f"\U0001F4B0 Balansingizga {fmt_money(amount)} so'm qo'shildi.\n"
            f"Joriy balans: {fmt_money(new_balance)} so'm"
        )
    return (
        f"\U0001F4B0 Balansingizdan {fmt_money(-amount)} so'm ayirildi.\n"
        f"Joriy balans: {fmt_money(new_balance)} so'm"
    )


def duplicate_topup_warning(count: int) -> str:
    return (
        f"\n\n\u26A0\uFE0F Diqqat: bu foydalanuvchidan yana {count} ta kutilayotgan "
        f"so'rov bor \u2014 ikkalasini ham tasdiqlab qo'ymang!"
    )


ADMIN_HELP = (
    "\n\n\U0001F4CB Buyruqlar:\n"
    "/find <id yoki @username> \u2014 foydalanuvchini qidirish\n"
    "/addbalance <id> <miqdor> \u2014 balans qo'shish (ayirish uchun manfiy son)\n"
    "/ban <id> \u2014 foydalanuvchini bloklash\n"
    "/unban <id> \u2014 blokdan chiqarish\n"
    "/broadcast <matn> \u2014 barcha foydalanuvchilarga xabar yuborish"
)
