# Nomer Bot — SmmUpper orqali raqam / Stars / Premium sotish boti

Bu bot foydalanuvchilarga botning ichida ochiladigan **shaxsiy balans** orqali:

- 📱 Virtual raqam (SMS kod uchun) yoki tayyor akkaunt (2FA)
- ⭐ Telegram Stars
- 💎 Telegram Premium

sotib olish imkonini beradi. Orqa tomonda **SmmUpper — Hamkorlik API**
(https://smmupper.uz/api/v2/docs) ishlatiladi.

## Qanday ishlaydi

1. Foydalanuvchi `/start` bosadi, botда ichki balansga ega bo'ladi (hammasi 0 so'mdan boshlanadi).
2. Balansni to'ldirish: foydalanuvchi summani kiritadi → botда ko'rsatilgan kartangizga
   o'tkazadi → to'lov chekining skrinshotini yuboradi → bu sizga (admin) rasm va
   ✅/❌ tugmalar bilan yuboriladi → tasdiqlasangiz, foydalanuvchi balansi avtomatik
   to'ldiriladi.
3. Foydalanuvchi raqam / Stars / Premium tanlaydi → narx uning balansidan yechiladi →
   bot SmmUpper API orqali buyurtmani amalga oshiradi → natija (raqam, SMS kod,
   buyurtma raqami) foydalanuvchiga yuboriladi. Raqam uchun SMS kod avtomatik
   kutib olinadi (fon rejimida ~1 daqiqa tekshiriladi), kelmasa "🔄 Kodni tekshirish"
   tugmasi orqali istalgan vaqt qayta so'rash mumkin. 10 daqiqadan keyin ham kod
   kelmasa, foydalanuvchi "💸 Pulni qaytarish" tugmasi orqali pulini o'ziga
   qaytarib olishi mumkin (buyurtma "refunded" deb belgilanadi, ikki marta
   qaytarib bo'lmaydi).
4. Narxga ustama (sizning foydangiz) qo'shish uchun `.env` dagi `MARKUP_PERCENT`
   qiymatini o'zgartiring — masalan `10` desangiz, SmmUpper narxiga ustiga 10%
   qo'shib sotasiz. Standart holatda `0` — ya'ni SmmUpper narxi qanday bo'lsa,
   xaridorga ham o'shanday chiqadi.
5. `/admin` buyrug'i — SmmUpper hisobingizdagi joriy balansni va quyidagi
   buyruqlar ro'yxatini ko'rsatadi (buni vaqti-vaqti bilan tekshirib, kerak
   bo'lsa SmmUpper botида to'ldirib turing — bu sizning SmmUpper'dagi
   HAMKOR balansingiz, foydalanuvchilarning botdagi ichki balansidan
   alohida narsa).
6. **Kanalga xabar (ixtiyoriy):** `.env` dagi `CHANNEL_ID` ni to'ldirsangiz,
   kimdir raqam / Stars / Premium sotib olganda bot shu kanalga avtomatik
   xabar yuboradi. Buning uchun botingizni o'sha kanalga **admin** qilib
   qo'shing (kamida "Xabar yuborish" huquqi bilan) — aks holda xabar
   yuborilmaydi (lekin bu foydalanuvchiga ta'sir qilmaydi, xarid baribir
   normal davom etadi).

## Admin buyruqlari

Faqat `ADMIN_IDS`da ko'rsatilgan ID'lar ishlata oladi:

- `/admin` — SmmUpper balansi + shu buyruqlar ro'yxati
- `/find <id yoki @username>` — foydalanuvchini topib, balansi/buyurtmalar
  soni/bloklanganligini ko'rsatadi
- `/addbalance <user_id> <miqdor>` — foydalanuvchi balansiga qo'lda pul
  qo'shadi (ayirish uchun manfiy son: `/addbalance 123456789 -20000`);
  foydalanuvchiga ham xabar boradi
- `/ban <user_id>` / `/unban <user_id>` — foydalanuvchini bloklaydi/blokdan
  chiqaradi. Bloklangan foydalanuvchining hech qanday xabari botga
  ta'sir qilmaydi (jimgina e'tiborsiz qoldiriladi)
- `/broadcast <matn>` — barcha foydalanuvchilarga xabar yuboradi, oxirida
  nechtasiga yetib borgani/bormaganini ko'rsatadi

Shuningdek, bot endi spamni o'zi cheklaydi (bir foydalanuvchidan juda tez-tez
kelgan takroriy bosishlar e'tiborsiz qoldiriladi) va balans to'ldirish
so'rovi kelganda, agar o'sha foydalanuvchidan boshqa kutilayotgan so'rov ham
bo'lsa, sizga (admin) shu haqda ogohlantirish beriladi — bitta chekni ikki
marta tasdiqlab qo'ymasligingiz uchun.

## Rangli tugmalar

2026-yil fevralida Telegram Bot API'ga tugmalarga rang berish imkoniyati
qo'shildi (Bot API 9.4). Shunga ko'ra botdagi tugmalar endi: bekor
qilish/rad etish — 🔴 qizil, tasdiqlash — 🟢 yashil, qolgan tanlov
tugmalari — 🔵 koʼk rangda ko'rinadi. Bu Telegram ilovangiz versiyasiga
bog'liq — agar ilovangiz eskiroq bo'lsa, ranglar hali ko'rinmasligi mumkin,
lekin botning ishlashiga bu ta'sir qilmaydi.

## O'rnatish (faqat telefondan, kompyutersiz)

### 1. Zip faylni "chiqaring" (extract)
GitHub zip faylni o'zi ochib bermaydi — avval telefoningizda uni ichidagi
fayllarga aylantirish (extract) kerak:
- **Android**: "Fayllar" (Files) ilovasini oching → Yuklab olinganlar
  (Downloads) → `nomer_bot.zip` ustiga bosing (yoki uzoq bosib ushlab turing)
  → chiqqan menyudan **"Chiqarish" / "Extract"** ni tanlang → `nomer_bot`
  nomli yangi papka paydo bo'ladi.
- **iPhone**: "Fayllar" (Files) ilovasida `nomer_bot.zip` ustiga bosing — u
  avtomatik ochilib, `nomer_bot` nomli papkaga aylanadi.

### 2. GitHub'da repo yarating
- github.com saytiga kiring → yuqoridagi **+** belgisi (yoki **New repository**)
  → nom bering (masalan `nomer-bot`) → **Create repository**.

### 3. Fayllarni yuklang
- Yangi repo sahifasida **"Add file"** → **"Upload files"** ni bosing.
- **"choose your files"** tugmasini bosing — fayl tanlash oynasi ochiladi.
- 1-qadamda chiqargan `nomer_bot` papkasini toping, ICHIGA kiring — endi
  `main.py`, `config.py` va qolgan hammasi bitta tekis ro'yxatda ko'rinadi
  (ichida boshqa papka yo'q — hammasi shu darajada, shuning uchun bittа
  urinishda hammasini belgilash oson).
- Hammasini belgilang — **faqat `.env` faylini TASHLAB KETING** (uni
  yuklamang — u yerda maxfiy tokenlar bo'ladi, sozlamalarni pastda Render'da
  alohida kiritasiz).
- Pastga tushib, **"Commit changes"** tugmasini bosing.
- *Agar fayllarni bir nechtasini birga belgilash imkoni bo'lmasa*: xuddi shu
  "Upload files" oynasiga fayllarni bir nechta bosqichda (masalan 5 tadan)
  qo'shib, har safar "Commit changes" bosishingiz ham mumkin — repo bir xil
  bo'lib chiqadi.

### 4. Render'da deploy qiling
- render.com'da hisob oching (GitHub orqali kirsangiz qulay) →
  **New +** → **Web Service** → repongizni tanlang.
- Sozlamalar:
  - **Build Command**: `pip install -r requirements.txt`
  - **Start Command**: `python main.py`
- **Environment** bo'limiga quyidagilarni qo'shing (`.env.example` faylига qarang):
  - `BOT_TOKEN` — @BotFather'dan olingan token
  - `ADMIN_IDS` — Telegram ID'ingiz (@userinfobot orqali bilib olasiz);
    bir nechta admin bo'lsa vergul bilan: `111111,222222`
  - `SMMUPPER_API_KEY` — SmmUpper botidagi "API (hamkorlik)" bo'limidan
  - `CARD_NUMBER` va `CARD_HOLDER` — balans to'ldirish uchun karta ma'lumotlaringiz
  - `MARKUP_PERCENT` — ixtiyoriy, standart `0`
  - `CHANNEL_ID` — ixtiyoriy; buyurtmalar e'lon qilinadigan kanal (`-100...`
    ID yoki `@kanal_username`). Bo'sh qoldirsangiz, kanalga hech narsa
    yuborilmaydi
  - `DATABASE_URL` — ixtiyoriy, lekin **tavsiya etiladi**: pastdagi
    "Ma'lumotlar yo'qolmasligi" bo'limiga qarang
- **Create Web Service** tugmasini bosing — Render avtomatik deploy qiladi.

### 5. Sinab ko'ring
- Deploy tugagach, Telegram'da botingizga `/start` yuboring.
- O'zingizga (admin) test uchun balans qo'shish kerak bo'lsa: balansni to'ldirish
  oqimidan o'tib, o'zingiz yuborgan so'rovni o'zingiz tasdiqlashingiz mumkin.

## Loyihaning tuzilishi

Barcha fayllar bitta darajada (papkasiz) — telefondan yuklashni osonlashtirish
uchun atayin shunday qilingan:

```
nomer_bot/
├── main.py              # Botni ishga tushiruvchi fayl
├── config.py            # .env dan sozlamalarni o'qiydi
├── database.py          # Baza: foydalanuvchilar, buyurtmalar, to'ldirish so'rovlari (SQLite yoki Postgres)
├── smmupper_api.py      # SmmUpper API bilan ishlash
├── pricing.py           # Narxga ustama qo'shish
├── keyboards.py         # Tugmalar
├── texts.py             # Barcha matnlar
├── states.py            # Bosqichma-bosqich suhbat holatlari (FSM)
├── handler_common.py    # Umumiy: bekor qilish, menyu tugmalari filtri
├── handler_start.py     # /start va bosh menyu
├── handler_balance.py   # Balans va uni to'ldirish
├── handler_numbers.py   # Raqam sotib olish + SMS kod kutish
├── handler_stars.py     # Stars sotib olish
├── handler_premium.py   # Premium sotib olish
├── handler_orders.py    # Buyurtmalar tarixi
└── handler_admin.py     # Admin panel: to'ldirish so'rovlari, /find, /addbalance, /ban, /broadcast
```

## Ma'lumotlar yo'qolmasligi (Postgres)

Standart holatda ma'lumotlar (balans, buyurtmalar) `bot.db` nomli SQLite
faylда saqlanadi. Muammo: Render'ning bepul tarifida bu fayl har doim ham
qayta deploy qilinganda saqlanib qolavermaydi — ya'ni foydalanuvchilarning
balansi/tarixi birdaniga o'chib ketishi mumkin.

**Yechim (bepul, tavsiya etiladi):** `.env`da `DATABASE_URL` ni to'ldirsangiz,
bot avtomatik ravishda SQLite o'rniga shu manzildagi PostgreSQL'ni ishlatadi —
boshqa hech narsani o'zgartirish shart emas.

1. [neon.tech](https://neon.tech) saytiga kiring, GitHub orqali ro'yxatdan o'ting
   (bepul tarifi yetarli).
2. Yangi loyiha (**New Project**) yarating.
3. Ochilgan sahifada **"Connection string"** (yoki "Connection details")
   bo'limini toping — `postgresql://...` bilan boshlanadigan manzil
   ko'rsatilgan bo'ladi. Uni nusxalang.
4. Render'dagi servisingiz → **Environment** → yangi o'zgaruvchi qo'shing:
   nomi `DATABASE_URL`, qiymati — nusxalagan manzil.
5. Saqlang — Render odatda avtomatik qayta deploy qiladi (aks holda
   **Manual Deploy** tugmasini bosing).

Agar `DATABASE_URL` bo'sh qoldirilsa, bot avvalgidek SQLite bilan ishlashda
davom etadi (sodda, lekin yuqoridagi xavf bilan).

## Eslatmalar

- Bot **polling** rejimida ishlaydi (webhook emas) — shuning uchun alohida
  domen yoki SSL sertifikat kerak emas. Lekin Render'ning **Web Service**
  turi deploy paytida kamida bitta ochiq portni talab qiladi (aks holda
  "port scan timeout" bilan o'chirib qo'yadi) — shuning uchun `main.py`
  ichida shunga mo'ljallangan yengil health-check server (`aiohttp`) ham
  ishga tushadi; bu haqiqiy trafik uchun emas, faqat Render'ning talabini
  qondirish uchun.
- Balansni tekshirish va yechish (raqam/Stars/Premium sotib olishda) bitta
  atomik baza amali orqali qilinadi — shu tufayli bir foydalanuvchi ikkita
  xaridni bir vaqtda tasdiqlab, balansini minusga tushira olmaydi. SmmUpper
  so'rovi xato qaytarsa, yechilgan pul avtomatik balansga qaytariladi.
- `available_countries` javobida qaysi davlatlar chiqishi to'liq SmmUpper'ning
  o'zida qanday sozlanganiga bog'liq — bot ularni har safar jonli so'raydi,
  hech narsa qattiq kodlanmagan.
