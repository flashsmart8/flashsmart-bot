import os, json, re, logging, time
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton, WebAppInfo
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes
import httpx
from anthropic import AsyncAnthropic

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
ANTHROPIC_KEY = os.environ["ANTHROPIC_API_KEY"]
FB_URL = os.environ["FIREBASE_URL"]
FB_EMAIL = os.environ["FIREBASE_EMAIL"]
FB_PASSWORD = os.environ["FIREBASE_PASSWORD"]
FB_API_KEY = os.environ["FIREBASE_API_KEY"]
OWNER_ID = int(os.environ.get("OWNER_TELEGRAM_ID", "0"))

client = AsyncAnthropic(api_key=ANTHROPIC_KEY)
_fb_token = None
_fb_token_exp = 0
_db_cache = None
_db_cache_time = 0
DB_CACHE_TTL = 30  # seconds

async def get_fb_token():
    global _fb_token, _fb_token_exp
    if _fb_token and time.time() < _fb_token_exp:
        return _fb_token
    async with httpx.AsyncClient() as http:
        r = await http.post(
            f"https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword?key={FB_API_KEY}",
            json={"email": FB_EMAIL, "password": FB_PASSWORD, "returnSecureToken": True}
        )
        data = r.json()
        if "idToken" not in data:
            raise Exception(f"Firebase auth error: {data.get('error', {}).get('message', 'Unknown')}")
        _fb_token = data["idToken"]
        _fb_token_exp = time.time() + 3000
    return _fb_token

async def fb_get(path=""):
    global _fb_token, _fb_token_exp
    token = await get_fb_token()
    url = f"{FB_URL}.json?auth={token}" if not path else f"{FB_URL}/{path}.json?auth={token}"
    async with httpx.AsyncClient(timeout=30) as http:
        r = await http.get(url)
        if r.status_code == 401:
            _fb_token = None
            _fb_token_exp = 0
            token = await get_fb_token()
            url = f"{FB_URL}.json?auth={token}" if not path else f"{FB_URL}/{path}.json?auth={token}"
            r = await http.get(url)
        return r.json()

async def fb_patch(data):
    global _fb_token, _fb_token_exp
    token = await get_fb_token()
    async with httpx.AsyncClient(timeout=30) as http:
        r = await http.patch(f"{FB_URL}.json?auth={token}", json=data)
        if r.status_code == 401:
            _fb_token = None
            _fb_token_exp = 0
            token = await get_fb_token()
            r = await http.patch(f"{FB_URL}.json?auth={token}", json=data)
        return r.json()

async def load_db(force=False):
    global _db_cache, _db_cache_time
    if not force and _db_cache and time.time() - _db_cache_time < DB_CACHE_TTL:
        return _db_cache
    data = await fb_get()
    if not data or not isinstance(data, dict):
        return _db_cache or {}
    for key in ["products","invoices","incomes","payments","clients","returns"]:
        if key in data and isinstance(data[key], dict):
            data[key] = list(data[key].values())
    logger.info(f"DB loaded: products={len(data.get('products',[]))}, clients={len(data.get('clients',[]))}, invoices={len(data.get('invoices',[]))}")
    _db_cache = data
    _db_cache_time = time.time()
    return data

# ── Helpers ──
def get_stock(pid, products, invoices):
    p = next((x for x in products if x.get("id") == pid), None)
    if not p: return 0
    sold = sum(item.get("qty",0) for inv in invoices for item in (inv.get("items") or []) if item.get("productId") == pid)
    return max(0, (p.get("income") or 0) - sold)

def get_price(product, price_level):
    """Returns price per selling unit (pack). Unit price × pcsPerPack."""
    level_map = {"gurt":"priceGurt","d1":"priceD1","d2":"priceD2"}
    field = level_map.get(price_level, "priceD2")
    unit_price = product.get(field) or product.get("priceD2") or 0
    pcs = product.get("pcsPerPack") or 1
    return round(unit_price * pcs, 2)

def find_client_by_tg(clients, tg_id):
    return next((c for c in clients if str(c.get("telegramId","")) == str(tg_id)), None)

def find_client_by_phone(clients, phone):
    phone_clean = re.sub(r'\D','', phone)
    for c in clients:
        cp = re.sub(r'\D','', c.get("phone",""))
        if cp and (cp == phone_clean or cp.endswith(phone_clean[-9:]) or phone_clean.endswith(cp[-9:])):
            return c
    return None

def stock_level_label(stock_units, pcs_per_pack=1):
    """Return word-level stock status instead of raw numbers (trade secret protection).
    Thresholds are in PACKS, not individual items.
    >10 packs → 'є в наявності'; 1-10 packs → 'обмежена кількість'; 0 → 'немає'"""
    packs = stock_units // pcs_per_pack if pcs_per_pack > 1 else stock_units
    if packs <= 0:
        return "❌ немає в наявності"
    if packs <= 10:
        return "⚠️ обмежена кількість"
    return "✅ є в наявності"

# ── Conversations & pending orders ──
conversations = {}
pending_orders = {}  # user_id -> {client, items, total}
pending_registration = {}  # user_id -> {phone, step}

SYSTEM_PROMPT = """Ти — Олег, досвідчений менеджер з гуртових продажів мобільних аксесуарів компанії FlashSmart. Працюєш з дилерами і оптовими клієнтами. Знаєш ринок аксесуарів до техніки Apple та Android вздовж і впоперек.

═══════════════════════════════
МОВНІ ПРАВИЛА (НАЙВИЩИЙ ПРІОРИТЕТ — ПОРУШЕННЯ НЕПРИПУСТИМІ):
═══════════════════════════════

ОСНОВНЕ ПРАВИЛО:
— Якщо клієнт пише українською — відповідай ТІЛЬКИ чистою живою українською. ЖОДНОГО російського слова чи кальки.
— Якщо клієнт пише російською — відповідай ТІЛЬКИ російською, без жодного українського слова.
— Перед відправкою КОЖНОЇ відповіді перечитай її і переконайся що кожне слово і кожна граматична конструкція є природною для обраної мови.

═══════════════════════════════
БЛОК 1. ЗАБОРОНЕНІ РОСІЙСЬКІ СЛОВА (заміни на українські):
═══════════════════════════════
"нужен/нужно" → "потрібен/потрібно"
"есть" → "є"
"конечно" → "звичайно"
"получається/получити" → "виходить/отримати"
"посмотріть/посмотрим" → "подивитись/подивимось"
"скідка" → "знижка"
"заказ" → "замовлення"
"вообще" → "взагалі"
"пожалуйста" → "будь ласка"
"можно" → "можна"
"хорошо" → "добре" або "гаразд"
"предлагаю" → "пропоную"
"обращайтесь" → "звертайтесь"
"подскажіть" → "підкажіть"
"класні/класно" → "чудові/чудово"
"здєсь" → "тут"
"сейчас" → "зараз"
"тоже" → "також" або "теж"
"только" → "тільки"
"если" → "якщо"
"кстати" → "до речі"
"разберемось" → "розберемось"
"помочь" → "допомогти"
"подожди/подождіть" → "зачекай/зачекайте"
"красиво" → "гарно"
"дєшево/дєшевше" → "дешево/дешевше"
"побольше" → "побільше"
"нравиться" → "подобається"
"цєна" → "ціна"
"получіш" → "отримаєш"
"висилаю" → "надсилаю"
"слідуючий" → "наступний"
"ладно" → "гаразд" або "добре"
"вибір" (рос. "выбор") в значенні "обирати" → "вибір" (укр. ок), але "сделать виборку" → "вибрати"
"любий" (рос.) → "будь-який" (укр.)
"немного" → "небагато" або "трохи"
"немножко" → "трішки" або "небагато"
"залишилось обмежена кількість" → "обмежена кількість" (без "залишилось" — це граматично невірно)
"нет в наличии" → "немає в наявності"

═══════════════════════════════
БЛОК 2. КАНЦЕЛЯРИЗМИ І КАЛЬКИ — пиши живою мовою, не штучно:
═══════════════════════════════
"являється" → "є" (НІКОЛИ не пиши "являється")
"при умові" → "за умови"
"у зв'язку з тим, що" → "оскільки" або "бо"
"в результаті чого" → "тому"
"з метою" → "щоб"
"на даний момент" → "зараз"
"в цілому" → "загалом" або просто прибери
"має місце" → "є" або "відбувається"
"здійснити замовлення" → "оформити замовлення" або "замовити"
"проводити продаж" → "продавати"
"у відповідності з" → "відповідно до"
"приймати участь" → "брати участь"
"носити характер" → "бути" (наприклад, "носить регулярний характер" → "регулярний")
"виходячи з вищесказаного" → НЕ пиши взагалі, просто переходь до висновку
"даний/даного" → "цей/цього" ("в даний час" → "зараз")
"необхідно" → "треба" або "потрібно"

═══════════════════════════════
БЛОК 3. КАЛЬКИ З РОСІЙСЬКИМИ ПРИЙМЕННИКАМИ І КОНСТРУКЦІЯМИ:
═══════════════════════════════
"по питанню" → "щодо" ("питання щодо ціни", не "питання по ціні")
"по поводу" → "щодо"
"по причині" → "через"
"за рахунок" (в значенні "завдяки") → "завдяки"
"в залежності" → "залежно"
"в основному" → "переважно" або "здебільшого"
"в принципі" → "загалом" або "взагалі"
"в кінці кінців" → "врешті-решт" або "зрештою"
"більш краще" / "більш дешевше" → НІКОЛИ так не кажи. Тільки "краще" / "дешевше"
"самий найкращий" → НІКОЛИ. Тільки "найкращий"
"дуже сильно" → "дуже" (без "сильно")
"оплачувати за товар" → "платити за товар" або "оплачувати товар" (без "за")
"відповісти на ваше запитання" → ок
"дякую вас" (рос. калька) → "дякую вам" або "дякую"

═══════════════════════════════
БЛОК 4. ПРАВИЛА ПРО СЛОВО "ГУРТ":
═══════════════════════════════
"Гурт" у нашій системі — це НАЗВА РІВНЯ ЦІНИ клієнта (як "Дилер 1", "Дилер 2"). Це НЕ місце зберігання, не склад, не категорія товару.
ЗАБОРОНЕНО:
— "товар у гурту" ✗
— "є в гурту" ✗
— "знаходиться в гурту" ✗
— "Все у гурту" ✗
ПРАВИЛЬНО:
— "є в наявності" / "є на складі" / "в асортименті" / просто "є"
— "ціна по рівню Гурт: $X.XX" або "ваш рівень — Гурт, ціна $X.XX" — коли ОЗНАЧУЄШ рівень

═══════════════════════════════
БЛОК 5. ОРФОГРАФІЯ І ЗАКІНЧЕННЯ:
═══════════════════════════════
— Закінчення -ться у дієсловах: "подобається" (не "подобаеться"), "зустрічається"
— Літера 'є' там де російське 'е': "є", "ємність", "Європа", "проєкт"
— Літера 'і' там де російське 'и' у багатьох коренях: "білий", "цікавий", "дім", "він"
— Кличний відмінок при звертанні: "Романе", "Олександре", "Маріє" (а не "Роман,", "Олександр,")
   Виняток: можна використовувати повне ім'я в називному, якщо звертаєшся ввічливо ("Олексієнко Роман Анатолійович, ...")
— Апостроф там де треба: "ім'я", "об'єм", "м'який" (не "имя", "объем", "мякий")

═══════════════════════════════
БЛОК 6. ПРИРОДНІСТЬ І СТИЛЬ:
═══════════════════════════════
— Не починай кожне речення з "Дякую" — це штучно. Дякуй раз на діалог.
— Не пиши "Будь ласка" перед кожною дією — використовуй коли реально просиш.
— Не пиши "Як у мене справи?" або інші завчені фрази-привітання.
— Не вибачайся коли немає за що ("Вибачте, але..." — прибери "Вибачте, але")
— Уникай повторів того самого слова в одному реченні (заміни синонімом)
— НЕ ставь крапки в кінці коротких відповідей у месенджері. Звичайна жива мова в чаті — без крапок: "Так, є в наявності" а не "Так, є в наявності."
   Крапки потрібні тільки у складних реченнях з кількома частинами.

🚫 КРИТИЧНО — НЕ ВІТАЙСЯ ПОВТОРНО:
Привітання ("Привіт", "Доброго дня", "Вітаю", "Я — Олег", "Олег зі FlashSmart") — ТІЛЬКИ ОДИН РАЗ за діалог: на самому початку, у відповідь на /start або перше повідомлення клієнта.
У ВСІХ наступних відповідях — ВІДРАЗУ переходь до суті, БЕЗ привітання, БЕЗ представлення себе.
ЗАБОРОНЕНО на 2-й, 3-й, 4-й і далі відповіді:
— "Привіт!" ✗
— "Доброго дня!" ✗
— "Олег зі FlashSmart." ✗
— "Я — Олег." ✗
— будь-яке самопредставлення чи привітання
ПРАВИЛЬНО на наступні запити:
Клієнт: "що є зі скла?"
Бот: "Зі скла є Doberman Glass Clear для всіх актуальних iPhone..." ✓ (одразу до справи)
Клієнт: "а ціна?"
Бот: "Базові моделі — $2.40, Pro/Max — від $3.12..." ✓ (без "Привіт ще раз!")
Перевір історію розмови — якщо ти вже привітався у цьому чаті раніше, НЕ повторюй привітання ніколи.

═══════════════════════════════
БЛОК 7. ЗВЕРТАННЯ — ЗАВЖДИ НА "ВИ":
═══════════════════════════════
КРИТИЧНО: завжди звертайся до клієнта на "Ви" з великої літери. Це гуртові продажі, дилери — потрібна повага і діловий тон.
ЗАБОРОНЕНО:
— "тобі цікаво" ✗
— "дивись" ✗
— "вибирай" ✗
— "як тобі" ✗
— будь-які "ти/тобі/тебе/твій"
ПРАВИЛЬНО:
— "Вам цікаво" / "Вас цікавить"
— "подивіться"
— "оберіть"
— "як Вам"
— "Ви/Вам/Вас/Ваш" — завжди з великої літери в особистому звертанні
Виняток: групові звертання типу "якщо хочете" (де "Ви" мається на увазі) — теж ок.
НІКОЛИ не змішуй "Ви" і "ти" в межах однієї відповіді — це непослідовно.

═══════════════════════════════
БЛОК 8. ЗАЛИШКИ ТОВАРУ — НІКОЛИ НЕ НАЗИВАЙ КОНКРЕТНУ КІЛЬКІСТЬ:
═══════════════════════════════
ЦЕ КОМЕРЦІЙНА ТАЄМНИЦЯ. Конкуренти можуть зайти як клієнти і витягнути дані про твої запаси через бот.
ЗАБОРОНЕНО:
— "є 24 шт" ✗
— "залишилось 5 упаковок" ✗
— "по 24-25 шт" ✗
— "кількість від 10 до 25 шт" ✗
— "зі складу йде по 100 шт" ✗
— взагалі будь-які цифри про залишок чи запас
ПРАВИЛЬНО (3 рівні наявності):
— Якщо за даними зі складу >10 упаковок: "є в наявності" / "в асортименті" / "доступно"
— Якщо 1-10 упаковок: "залишилось небагато" / "обмежена кількість" / "краще не зволікати"
— Якщо 0: "немає в наявності" / "зараз немає"
ВАЖЛИВО: коли система повертає тобі дані з функції check_stock з конкретною кількістю — НЕ передавай цю кількість клієнту. Переклади у словесний рівень наявності.
Виняток: якщо клієнт ПРЯМО запитує "скільки штук залишилось" — м'яко відповідай "точну кількість уточню у менеджера, але загалом — є в наявності" або "достатньо для Вашого замовлення, давайте оформлювати".

═══════════════════════════════
БЛОК 9. УЗГОДЖЕННЯ РОДУ І ВІДМІНЮВАННЯ:
═══════════════════════════════
Перевіряй рід іменника перед вживанням прикметника або займенника:
— "модель" — жіночий рід → "яка модель", "ця модель", "потрібна модель", "цікава модель" (НЕ "який модель")
— "ціна" — жіночий → "яка ціна", "хороша ціна"
— "товар" — чоловічий → "який товар", "цей товар"
— "замовлення" — середній → "яке замовлення", "це замовлення"
— "позиція" — жіночий → "яка позиція", "ця позиція"
— "кабель" — чоловічий → "який кабель", "цей кабель"
— "адаптер" — чоловічий → "який адаптер"
— "знижка" — жіночий → "яка знижка"
Перед відправкою — перевір що прикметник/займенник у тому самому роді й числі що іменник.

═══════════════════════════════
🚫 БЛОК 10. ЗАБОРОНА ВИГАДУВАТИ ТОВАРИ І ХАРАКТЕРИСТИКИ:
═══════════════════════════════
ЦЕ КРИТИЧНО ВАЖЛИВО. Клієнт замовляє на основі твоїх слів. Якщо ти збрехав — ми втрачаємо клієнта і репутацію.

ЖОРСТКЕ ПРАВИЛО:
— Згадуй ТІЛЬКИ ті товари, які повернула функція check_stock або list_products у цій розмові.
— Описуй товар ТІЛЬКИ через ті дані, які повернула система: назва, ціна, наявність, категорія.
— НІКОЛИ не вигадуй назв товарів, моделей, серій, які не повертала система.
— НІКОЛИ не додавай маркетингових характеристик з власних знань: "преміум-клас", "найкраще покриття", "гнучке скло", "9D захист", "повне обгорткове покриття", "матовий ефект", "протиударне", "олеофобне" — нічого подібного, навіть якщо це звучить правдиво про подібні товари на ринку.
— НІКОЛИ не порівнюй товари між собою у термінах "цей кращий" / "цей преміальніший" / "цей дешевший і простіший" — у тебе немає цих даних.

ЗАБОРОНЕНІ ПРИКЛАДИ (ось так НЕ ПИШИ):
— "9D Glass — преміум-клас з максимальним покриттям" ✗ (вигадка, такого товару нема)
— "Doberman 20-в-1 — розширений захист камери і динаміків" ✗ (характеристика з повітря)
— "Це класичний вибір" / "Це для тих, хто цінує якість" ✗ (порожні маркетингові фрази)

ПРАВИЛЬНО (ось так ТРЕБА):
— "Doberman Glass Clear для iPhone 15 — $2.40, є в наявності"
— "У наявності: Doberman Glass Clear для iPhone Xr, X/Xs, Xs Max, 11, 12, 13, 14, 15, 16. Які моделі цікавлять?"
— Просто назви + ціни + наявність. Без характеристик. Без епітетів.

ЯКЩО клієнт прямо питає "а яке скло краще?" / "що порадите?" — відповідай чесно: "Не маю детального опису характеристик у системі. Можу уточнити у менеджера або підказати наявність і ціну — оберіть зручний варіант для Ваших клієнтів." НЕ вигадуй характеристик.

═══════════════════════════════
БЛОК 11. САМОПЕРЕВІРКА ПЕРЕД ВІДПРАВКОЮ:
═══════════════════════════════
1. Чи всі слова українські? (немає "нужно", "если", "тоже"?)
2. Чи природні граматичні конструкції? (немає "по питанню", "являється"?)
3. Чи відповідає тон ситуації? (не занадто формально, не занадто фамільярно)
4. Чи нема канцеляризмів? (прибрати "при умові", "у зв'язку з")
5. Чи правильно вжито "гурт"? (тільки як назва рівня ціни клієнта)
6. Чи звертання на "Ви" з великої літери? (НЕ змішувати з "ти"!)
7. Чи не названо КОНКРЕТНІ цифри залишків? (тільки рівні: "є / залишилось небагато / немає")
8. Чи узгоджений рід прикметників з іменниками? ("яка модель", не "який модель")
9. Чи це НЕ повторне привітання? (Якщо це не перша відповідь у діалозі — НЕ кажи "Привіт", "Я — Олег")
10. Чи всі названі товари РЕАЛЬНО є у системі? (НЕ вигадуй назв і характеристик!)

ТВІЙ ХАРАКТЕР:
— Привітний, але діловий. Не лебезиш, не перегинаєш з емодзі
— Говориш як людина, а не як робот — живо, природно, з гумором де доречно
— Пам'ятаєш ім'я клієнта і звертаєшся на ім'я або по імені-батькові
— Впевнений у своєму товарі, але не нав'язливий

ТВОЯ МЕТА: допомогти клієнту зробити замовлення швидко і зручно, збільшити суму замовлення м'якими пропозиціями.

═══════════════════════════════
КРИТИЧНІ ПРАВИЛА ПОВЕДІНКИ:
═══════════════════════════════

1. НІКОЛИ не вигадуй правила яких немає — немає мінімального замовлення, немає мінімальної кількості упаковок
2. Продавай БУДЬ-ЯКУ кількість — хоч 1 упаковку, хоч 100
3. Коли клієнт питає про ціну — ОДРАЗУ виконай JSON дію check_stock або list_products, щоб система показала реальну ціну з бази
4. НІКОЛИ не кажи "залежно від об'єму" — ціна фіксована по рівню клієнта
5. Коли клієнт хоче замовити — ОДРАЗУ виконай JSON create_order, не відмовляй і не переконуй брати більше

═══════════════════════════════
СЦЕНАРІЇ СПІЛКУВАННЯ:
═══════════════════════════════

1. ПЕРШИЙ КОНТАКТ:
— Привітайся тепло, представся
— Якщо клієнт є в базі — звернись на ім'я
— Запропонуй допомогу з замовленням

2. ПОКАЗ АСОРТИМЕНТУ:
— Коли питають про категорію або ціну — ОДРАЗУ виконай JSON дію
— Після списку від системи — коротко прокоментуй, запитай що цікавить
— Можеш запропонувати супутні товари, але ненав'язливо

3. РОБОТА З ЗАМОВЛЕННЯМ:
— Коли клієнт називає товари і кількість — ОДРАЗУ виконай JSON create_order
— Після підтвердження можеш м'яко запропонувати додати щось ще
— НЕ відмовляй в замовленні через малу кількість

4. ЦІНИ:
— Ціна визначається рівнем клієнта (гурт/дилер1/дилер2) — вона фіксована
— Не обговорюй ціни інших рівнів
— Знижки не даєш — кажеш "уточню у керівника"

5. БОРГ:
— Якщо є борг — згадай делікатно в кінці: "До речі, є невелика заборгованість — не забудьте врахувати"

6. ПІСЛЯ ЗАМОВЛЕННЯ:
— Підтверди суму, скажи що менеджер зв'яжеться
— Подякуй за замовлення

═══════════════════════════════
ТЕХНІЧНІ ПРАВИЛА (КРИТИЧНО ВАЖЛИВО):
═══════════════════════════════

Коли потрібна дія зі складом — відповідай ТІЛЬКИ JSON одним рядком, БЕЗ будь-якого тексту:

Питання про ціну/категорію → {"action":"list_products","category":"Захисне скло"}
Питання про конкретний товар → {"action":"check_stock","product":"назва"}
Замовлення → {"action":"create_order","items":[{"name":"точна назва","qty":1}]}
Борг → {"action":"check_debt"}

Категорії: навушники/наушники/headphones → Навушники
Кабелі/cable/провід/шнур → Кабелі  
Скло/glass/захист/9D/9Д → Захисне скло
Зарядка/charger/МЗП/адаптер → МЗП
Все/асортимент/прайс → all

ВАЖЛИВО — коли використовувати JSON:
— Клієнт питає "що по ціні на скло?" → JSON list_products
— Клієнт питає "скільки коштує 9D Glass?" → JSON check_stock  
— Клієнт каже "давай 2 упаковки скла" → JSON create_order
— БУДЬ-ЯКЕ питання про ціну, наявність, замовлення → JSON дія

ЗАБОРОНЕНО:
— Писати текст разом з JSON
— Вигадувати товари, ціни, правила яких немає в базі
— Відмовляти в замовленні через малу кількість
— Самостійно писати "замовлення прийнято" або "замовлення підтверджено" — це робить ТІЛЬКИ система після слова "підтверджую"
— Казати "залежно від об'єму" — ціна фіксована
— Коли клієнт каже "підтверджую" — НЕ відповідай нічого, система сама обробить це слово

Після отримання даних зі складу — відповідай як живий менеджер, природно і коротко."""

async def execute_action(action_data, db, user_id, client_rec):
    products = db.get("products") or []
    invoices = db.get("invoices") or []
    payments = db.get("payments") or []
    action = action_data.get("action")
    price_level = client_rec.get("priceLevel","d2") if client_rec else "d2"
    level_labels = {"gurt":"Гурт","d1":"Дилер 1","d2":"Дилер 2"}

    if action == "list_products":
        cat = action_data.get("category","all")
        filtered = products if cat=="all" else [p for p in products if cat.lower() in p.get("category","").lower()]
        in_stock = [(p, get_stock(p.get("id"), products, invoices)) for p in filtered]
        in_stock = [(p,s) for p,s in in_stock if s>0]
        if not in_stock:
            return f"На жаль, зараз немає товарів в наявності{' в категорії '+cat if cat!='all' else ''}."
        by_cat = {}
        for p,s in in_stock:
            c = p.get("category","Інше")
            by_cat.setdefault(c,[]).append((p,s))
        result = f"📦 Товари в наявності ({len(in_stock)} поз.):\n\n"
        for cat_name, items in by_cat.items():
            result += f"*{cat_name}*\n"
            for p,s in items[:15]:
                price = get_price(p, price_level)
                pcs = p.get("pcsPerPack") or 1
                price_str = f" — ${price:.2f}/уп ({pcs}шт)" if pcs > 1 and price > 0 else (f" — ${price:.2f}" if price > 0 else "")
                label = stock_level_label(s, pcs)
                result += f"{label} {p.get('name')}{price_str}\n"
            result += "\n"
        if client_rec:
            result += f"_Ціни: {level_labels.get(price_level,'Дилер 2')}_"
        return result.strip()

    elif action == "check_stock":
        query = action_data.get("product","").lower()
        matches = [p for p in products if query in p.get("name","").lower()]
        if not matches:
            return "Товар не знайдено."
        result = "📦 Результати:\n\n"
        for p in matches[:5]:
            s = get_stock(p.get("id"), products, invoices)
            price = get_price(p, price_level)
            pcs = p.get("pcsPerPack") or 1
            logger.info(f"check_stock: {p.get('name')}, pcsPerPack={pcs}, price_level={price_level}, price={price}, stock_units={s}")
            label = stock_level_label(s, pcs)
            if pcs > 1:
                price_str = f" — ${price:.2f}/уп ({pcs} шт в уп)" if price>0 else ""
            else:
                price_str = f" — ${price:.2f}/шт" if price>0 else ""
            result += f"{label}: {p.get('name')}{price_str}\n"
        return result

    elif action == "check_debt":
        if not client_rec:
            return "Для перевірки боргу потрібно авторизуватись. Напишіть свій номер телефону."
        cid = client_rec.get("id")
        sold = sum(inv.get("total",0) for inv in invoices if inv.get("clientId")==cid)
        paid = sum(p.get("amount",0) for p in payments if p.get("clientId")==cid and p.get("amount",0)>0)
        debt = sold-paid
        if debt>0.01:
            return f"💰 Ваш борг: ${debt:.2f}"
        return "✅ Боргів немає"

    elif action == "create_order":
        items_req = action_data.get("items",[])
        if not items_req:
            return "Вкажіть товари для замовлення."
        order_items = []
        not_found = []
        no_stock = []
        for req in items_req:
            name_q = req.get("name","").lower().strip()
            qty = int(req.get("qty",1))
            # Flexible search - find best match
            def score(p):
                pn = p.get("name","").lower()
                if name_q == pn: return 100
                if name_q in pn: return 80
                if pn in name_q: return 70
                # Word overlap score
                q_words = set(name_q.split())
                p_words = set(pn.split())
                overlap = len(q_words & p_words)
                return overlap * 10
            scored = [(score(p), p) for p in products if score(p) > 0]
            scored.sort(key=lambda x: -x[0])
            if not scored:
                not_found.append(req.get("name","?"))
                continue
            p = scored[0][1]
            stock = get_stock(p.get("id"), products, invoices)
            # Guard against negative stock (data corruption from prior bugs) —
            # treat anything <= 0 as "unavailable" from the bot's perspective.
            available = stock if stock > 0 else 0
            if available < qty:
                # Don't leak the raw remaining number; show adjusted qty instead.
                if available == 0:
                    no_stock.append(f"{p.get('name')} (немає в наявності)")
                    continue
                no_stock.append(f"{p.get('name')} (можемо зібрати {available}, просили {qty})")
                qty = available
            price = get_price(p, price_level)
            order_items.append({
                "productId": p.get("id"),
                "name": p.get("name"),
                "qty": qty,
                "price": price,
                "pcsPerPack": p.get("pcsPerPack") or 1,
                "sum": round(price*qty, 2)
            })
        if not order_items and not_found:
            return f"❌ Товари не знайдено: {', '.join(not_found[:3])}\n\nНапишіть точну назву товару зі складу."
        if not order_items:
            return "Не вдалося сформувати замовлення — перевірте наявність товарів."
        total = sum(i["sum"] for i in order_items)
        pending_orders[user_id] = {
            "client": client_rec,
            "items": order_items,
            "total": total,
            "price_level": price_level
        }
        result = f"📋 *Замовлення сформовано:*\n\n"
        for item in order_items:
            pcs = item.get("pcsPerPack", 1)
            unit = "/уп" if pcs > 1 else "/шт"
            price_str = f"${item['price']:.2f}{unit}" if item['price']>0 else "ціна не вст."
            result += f"• {item['name']} × {item['qty']} ({price_str}) = ${item['sum']:.2f}\n"
        if not_found:
            result += f"\n⚠️ Не знайдено: {', '.join(not_found)}"
        if no_stock:
            result += f"\n⚠️ Обмежена кількість: " + "; ".join(no_stock)
        result += f"\n\n💰 *Разом: ${total:.2f}*"
        result += f"\n_Рівень: {level_labels.get(price_level,'Дилер 2')}_"
        result += "\n\nНапишіть *підтверджую* для створення накладної або *скасувати*"
        return result

    return "Дію виконано."

async def save_invoice(user_id, db):
    global _db_cache_time
    order = pending_orders.get(user_id)
    if not order:
        logger.warning(f"save_invoice: no pending order for user {user_id}")
        return "Немає активного замовлення."
    client_rec = order["client"]
    if not client_rec:
        return "Клієнт не визначений."
    invoices = db.get("invoices") or []
    products = db.get("products") or []

    # Re-validate stock before saving — catches races with Mini App / sklad.html
    # sales that happened between create_order and confirmation.
    problems = []
    for item in order["items"]:
        pid = item["productId"]
        requested = item["qty"]
        current_stock = get_stock(pid, products, invoices)
        available = current_stock if current_stock > 0 else 0
        if available < requested:
            problems.append({
                "name": item["name"],
                "available": available,
                "requested": requested
            })
    if problems:
        # Abort: cancel the pending order, ask client to reorder
        del pending_orders[user_id]
        lines = []
        for pr in problems:
            if pr["available"] == 0:
                lines.append(f"• {pr['name']}: немає в наявності")
            else:
                lines.append(f"• {pr['name']}: доступно {pr['available']}, просили {pr['requested']}")
        return ("⚠️ Наявність змінилася. Замовлення скасовано:\n\n" +
                "\n".join(lines) +
                "\n\nБудь ласка, оформіть замовлення заново.")

    next_num = (db.get("nextInvNum") or 1)
    inv_id = int(time.time()*1000)
    items = [{
        "productId": i["productId"],
        "qty": i["qty"] * i.get("pcsPerPack", 1),
        "packs": i["qty"],
        "price": i["price"],
        "sum": i["sum"]
    } for i in order["items"]]
    from datetime import date
    inv = {
        "id": inv_id,
        "num": next_num,
        "clientId": client_rec.get("id"),
        "date": date.today().isoformat(),
        "items": items,
        "total": order["total"],
        "priceLevel": order["price_level"],
        "source": "telegram"
    }
    invoices.append(inv)
    logger.info(f"Saving invoice #{next_num} for {client_rec.get('name')}, total=${order['total']:.2f}, items={len(items)}")
    try:
        await fb_patch({"invoices": invoices, "nextInvNum": next_num+1})
        logger.info(f"Invoice #{next_num} saved successfully")
    except Exception as e:
        logger.error(f"Failed to save invoice: {e}", exc_info=True)
        return "Помилка збереження накладної. Спробуйте ще раз."
    _db_cache_time = 0  # invalidate cache
    del pending_orders[user_id]
    return f"✅ Накладна *#{str(next_num).zfill(4)}* створена!\n💰 Сума: ${order['total']:.2f}"

async def process_message(user_id, text, db, client_rec):
    if user_id not in conversations:
        conversations[user_id] = []

    # Check for order confirmation — use exact match after stripping punctuation
    # to avoid false positives like "так, а скільки?" or "ок, а коли?"
    text_clean = re.sub(r'[!?.,;:)(\s]+', ' ', text.lower()).strip()
    confirm_words = {"підтверджую", "підтверджую замовлення", "confirm",
                     "yes", "go", "давай", "оформляй", "оформлюй"}
    cancel_words = {"скасувати", "скасувати замовлення", "cancel",
                    "відміна", "стоп", "не треба", "не потрібно"}

    if text_clean in confirm_words:
        if user_id in pending_orders:
            logger.info(f"Order confirmed by user {user_id}")
            # Force fresh DB read so the re-validation in save_invoice
            # sees invoices created by Mini App / sklad.html in the last 30s.
            fresh_db = await load_db(force=True)
            result = await save_invoice(user_id, fresh_db)
            return result

    if text_clean in cancel_words:
        if user_id in pending_orders:
            del pending_orders[user_id]
            return "Замовлення скасовано."

    products = db.get("products") or []
    invoices = db.get("invoices") or []
    price_level = client_rec.get("priceLevel","d2") if client_rec else "d2"
    in_stock = [(p, get_stock(p.get("id"), products, invoices)) for p in products]
    in_stock = [(p,s) for p,s in in_stock if s>0]
    # ALL product names — compact, one per line
    stock_list = "\n".join([f"- {p.get('name')}" for p,s in in_stock])
    client_info = f"Клієнт: {client_rec.get('name')}, рівень: {price_level}" if client_rec else "Клієнт не ідентифікований"
    context = f"{client_info}\n\nТовари в наявності ({len(in_stock)} поз.):\n{stock_list}\n\nВАЖЛИВО: Якщо клієнт питає про товар — ЗАВЖДИ виконуй JSON дію, навіть якщо не бачиш точну назву в списку. НЕ кажи що товару немає — дай системі перевірити."

    conversations[user_id].append({"role":"user","content":text})
    # Trim to last 20 messages to prevent unbounded memory growth
    if len(conversations[user_id]) > 20:
        conversations[user_id] = conversations[user_id][-20:]
    try:
        response = await client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=1000,
            system=SYSTEM_PROMPT+"\n\n"+context,
            messages=conversations[user_id][-10:]
        )
        ai_text = response.content[0].text.strip()
    except Exception as e:
        logger.error(f"Claude API error: {e}", exc_info=True)
        conversations[user_id].pop()  # remove failed user message
        return "Вибачте, сталась помилка з'єднання. Спробуйте ще раз."
    conversations[user_id].append({"role":"assistant","content":ai_text})
    # Trim again after assistant reply
    if len(conversations[user_id]) > 20:
        conversations[user_id] = conversations[user_id][-20:]

    # Try to extract JSON action from AI response
    action_data = None
    # Method 1: entire response is JSON
    stripped = ai_text.strip()
    if stripped.startswith("{"):
        try:
            action_data = json.loads(stripped)
        except:
            pass
    # Method 2: JSON inside markdown code block
    if not action_data:
        code_match = re.search(r'```(?:json)?\s*(\{.+?\})\s*```', ai_text, re.DOTALL)
        if code_match:
            try:
                action_data = json.loads(code_match.group(1))
            except:
                pass
    # Method 3: find JSON with balanced braces
    if not action_data:
        for i, ch in enumerate(ai_text):
            if ch == '{':
                depth = 0
                for j in range(i, len(ai_text)):
                    if ai_text[j] == '{': depth += 1
                    elif ai_text[j] == '}': depth -= 1
                    if depth == 0:
                        try:
                            action_data = json.loads(ai_text[i:j+1])
                        except:
                            pass
                        break
                if action_data:
                    break

    if action_data and isinstance(action_data, dict) and "action" in action_data:
        try:
            result = await execute_action(action_data, db, user_id, client_rec)
            action_type = action_data.get("action")
            # Clear old pending order if user does anything other than create_order
            if action_type != "create_order" and user_id in pending_orders:
                del pending_orders[user_id]
            # For list/check actions — humanize via Claude
            if action_type in ("list_products", "check_stock", "check_debt"):
                try:
                    humanize = await client.messages.create(
                        model="claude-haiku-4-5-20251001",
                        max_tokens=500,
                        system="Ти — Олег, менеджер FlashSmart. Тобі система повернула дані зі складу. Перекажи їх клієнту як живий менеджер — коротко, по-діловому, українською. 🚫 КРИТИЧНО: НЕ ВИГАДУЙ назв товарів, моделей, характеристик яких немає у даних від системи. НЕ додавай маркетингових епітетів ('преміум', '9D', 'найкраще покриття', 'противдарне', 'гнучке', 'преміальний клас') — таких даних система НЕ повертала. Використовуй ТІЛЬКИ назву + ціну + наявність зі системи. НЕ порівнюй товари між собою. Якщо клієнт питає 'який кращий?' — відповідай 'детального опису у системі немає, можу уточнити у менеджера'. НІКОЛИ не починай з 'Привіт', 'Доброго дня', 'Я — Олег', 'Олег зі FlashSmart' — це продовження діалогу, ти вже привітався раніше. Одразу переходь до суті: 'Зі скла є...', 'У нас є...', 'Доступно...'. ВАЖЛИВО: 'Гурт' — це назва рівня ціни клієнта, а не місце зберігання. НЕ пиши 'у гурту'. ЗАВЖДИ звертайся на 'Ви' з великої літери, ніколи на 'ти'. КРИТИЧНО про залишки: НЕ передавай конкретні цифри (24 шт, 10 упаковок) — тільки словесні рівні: '>10' = 'є в наявності' / 'в асортименті', '1-10' = 'залишилось небагато' / 'обмежена кількість', '0' = 'немає'. Узгоджуй рід: 'модель' жін.→ 'яка модель' (не 'який'), 'товар' чол.→ 'який товар'.",
                        messages=[
                            {"role":"user","content":text},
                            {"role":"assistant","content":ai_text},
                            {"role":"user","content":f"[СИСТЕМА] Результат:\n{result}\n\n[СИСТЕМА] Перекажи ці дані клієнту як живий менеджер."}
                        ]
                    )
                    humanized = humanize.content[0].text.strip()
                    conversations[user_id][-1]["content"] = humanized
                    return humanized
                except Exception as e:
                    logger.error(f"Humanize error: {e}")
            # For create_order — return formatted result as-is
            conversations[user_id][-1]["content"] = result
            return result
        except Exception as e:
            logger.error(f"Action error: {e}", exc_info=True)

    return ai_text

async def safe_reply(message, text, reply_markup=None):
    """Send message with Markdown, fallback to plain text if parse fails."""
    try:
        await message.reply_text(text, parse_mode="Markdown", reply_markup=reply_markup)
    except Exception:
        clean = text.replace("*","").replace("_","").replace("`","")
        try:
            await message.reply_text(clean, reply_markup=reply_markup)
        except Exception as e:
            logger.error(f"Reply failed: {e}")
            await message.reply_text("Вибачте, сталась помилка форматування. Спробуйте ще раз.")

# Mini App URL
SHOP_URL = "https://flashsmart8.github.io/sklad-claudeai/shop.html"

def main_menu_keyboard():
    """Persistent bottom keyboard."""
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("🛒 Магазин", web_app=WebAppInfo(url=SHOP_URL))],
            [KeyboardButton("📦 Каталог"), KeyboardButton("💰 Борг")],
            [KeyboardButton("📋 Замовлення"), KeyboardButton("📞 Менеджер")],
        ],
        resize_keyboard=True,
        is_persistent=True
    )

def confirm_keyboard():
    """Order confirmation keyboard."""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Підтверджую", callback_data="confirm_order"),
            InlineKeyboardButton("❌ Скасувати", callback_data="cancel_order"),
        ]
    ])

def check_overdue_debt(client_rec, db):
    """Return reminder message if client has overdue or near-due debt, else None."""
    if not client_rec:
        return None
    client_id = client_rec.get("id")
    defer_days = client_rec.get("deferDays", 0) or 0
    invoices = [i for i in (db.get("invoices") or []) if i.get("clientId") == client_id]
    payments = [p for p in (db.get("payments") or []) if p.get("clientId") == client_id and (p.get("amount") or 0) > 0]
    returns_ = [r for r in (db.get("returns") or []) if r.get("clientId") == client_id and r.get("type") == "return"]
    
    # Calculate targeted and unattached payments (FIFO logic)
    targeted = {}
    unattached = 0
    for p in payments:
        inv_id = p.get("invoiceId")
        if inv_id:
            targeted[str(inv_id)] = targeted.get(str(inv_id), 0) + p.get("amount", 0)
        else:
            unattached += p.get("amount", 0)
    for r in returns_:
        inv_id = r.get("invoiceId")
        if inv_id:
            targeted[str(inv_id)] = targeted.get(str(inv_id), 0) + r.get("sum", 0)
        else:
            unattached += r.get("sum", 0)
    
    # Sort invoices by date
    invoices.sort(key=lambda x: x.get("date", ""))
    remaining = unattached
    today_date = datetime.now().date()
    overdue_invs = []
    near_due_invs = []  # within 2 days
    
    for inv in invoices:
        total = inv.get("total", 0)
        t = targeted.get(str(inv.get("id")), 0)
        paid = min(t, total)
        debt = total - paid
        if debt > 0 and remaining > 0:
            from_pool = min(remaining, debt)
            paid += from_pool
            debt -= from_pool
            remaining -= from_pool
        if debt > 0.01:
            try:
                inv_date = datetime.strptime(inv.get("date", ""), "%Y-%m-%d").date()
                due_date = inv_date + timedelta(days=defer_days)
                days_to_due = (due_date - today_date).days
                if days_to_due < 0:
                    overdue_invs.append((inv, debt, -days_to_due))
                elif days_to_due <= 2:
                    near_due_invs.append((inv, debt, days_to_due))
            except:
                pass
    
    if not overdue_invs and not near_due_invs:
        return None
    
    lines = []
    if overdue_invs:
        lines.append("⚠️ *УВАГА! У вас є прострочені накладні:*\n")
        for inv, debt, days_overdue in overdue_invs[:5]:
            num = str(inv.get("num", 0)).zfill(4)
            lines.append(f"📄 #{num} — борг *${debt:.2f}* (прострочено на {days_overdue} дн.)")
    if near_due_invs:
        if overdue_invs:
            lines.append("")
        lines.append("🔔 *Термін оплати наближається:*\n")
        for inv, debt, days_left in near_due_invs[:3]:
            num = str(inv.get("num", 0)).zfill(4)
            when = "сьогодні" if days_left == 0 else f"через {days_left} дн."
            lines.append(f"📄 #{num} — борг *${debt:.2f}* (термін {when})")
    lines.append("\n💬 Будь ласка, не забудьте внести оплату!")
    return "\n".join(lines)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    db = await load_db()
    client_rec = find_client_by_tg(db.get("clients",[]), user_id)
    if client_rec:
        await update.message.reply_text(
            f"Привіт, {client_rec.get('name')}! 👋\nЯ — Олег, AI менеджер FlashSmart.\nОберіть що потрібно 👇",
            reply_markup=main_menu_keyboard()
        )
        # Check for overdue debt and remind
        reminder = check_overdue_debt(client_rec, db)
        if reminder:
            await safe_reply(update.message, reminder)
    else:
        await update.message.reply_text(
            "Вітаємо! 👋\n\nЯ — AI менеджер FlashSmart.\n\n"
            "📱 Для доступу до каталогу надішліть свій номер телефону для ідентифікації."
        )

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    data = query.data
    
    try:
        if data == "confirm_order":
            # Force fresh DB read so re-validation in save_invoice sees
            # invoices created by Mini App / sklad.html in the last 30s.
            db = await load_db(force=True)
            if user_id in pending_orders:
                result = await save_invoice(user_id, db)
                await query.message.reply_text(result, parse_mode="Markdown")
            else:
                await query.message.reply_text("Немає активного замовлення.")
                
        elif data == "cancel_order":
            if user_id in pending_orders:
                del pending_orders[user_id]
                await query.message.reply_text("Замовлення скасовано.")
            else:
                await query.message.reply_text("Немає активного замовлення.")
                
    except Exception as e:
        logger.error(f"Callback error: {e}", exc_info=True)
        await query.message.reply_text("Вибачте, сталась помилка.")

async def parse_supplier_price_list(text: str, products: list) -> str:
    """Send forwarded supplier message to Claude AI and return comparison table."""
    my_prices_info = ""
    for p in products:
        pp = p.get("purchasePrice") or 0
        if pp > 0:
            my_prices_info += f"- {p.get('name')}: закупівля ${pp:.2f}\n"

    prompt = f"""Ти — помічник гуртового магазину мобільних аксесуарів.

Ось повідомлення постачальника:
\"\"\"
{text}
\"\"\"

Ось мої поточні закупівельні ціни з бази (може бути неповний список):
{my_prices_info if my_prices_info else "(немає даних)"}

Завдання: витягни з повідомлення постачальника всі товари з цінами та поверни ТІЛЬКИ такий текст (без жодних пояснень):

📋 *Прайс постачальника*

Для кожного товару один рядок у форматі:
• [назва товару] — [ціна за 1шт]$ (від N шт — X$, від M шт — Y$) | Моя закупівля: [моя ціна]$ або ❓

Якщо градація цін відсутня — просто [ціна]$.
Якщо мого товару немає в базі — пиши ❓.
Якщо ціна постачальника ВИЩА за мою — додай 🔴 на початку рядка.
Якщо НИЖЧА або рівна — додай 🟢.
Якщо моєї ціни немає (❓) — додай ⚪.

Повертай ТІЛЬКИ цей список, без жодного іншого тексту."""

    resp = await client.messages.create(
        model="claude-opus-4-5",
        max_tokens=1000,
        messages=[{"role": "user", "content": prompt}]
    )
    return resp.content[0].text.strip()


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global _db_cache_time
    user_id = update.effective_user.id

    # ── Обробка пересланих повідомлень (прайс постачальника) ──
    if update.message.forward_from or update.message.forward_from_chat or update.message.forward_date:
        if user_id != OWNER_ID:
            return  # тільки власник може пересилати прайси
        fwd_text = update.message.text or update.message.caption or ""
        if not fwd_text.strip():
            await update.message.reply_text("⚠️ У пересланому повідомленні немає тексту.")
            return
        await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")
        try:
            db = await load_db()
            products = db.get("products") or []
            result = await parse_supplier_price_list(fwd_text, products)
            await safe_reply(update.message, result)
        except Exception as e:
            logger.error(f"Supplier parse error: {e}", exc_info=True)
            await update.message.reply_text("❌ Помилка при розборі прайсу.")
        return

    text = update.message.text
    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")
    try:
        db = await load_db()
        clients = db.get("clients") or []
        client_rec = find_client_by_tg(clients, user_id)

        # Handle persistent keyboard buttons
        if text == "📦 Каталог":
            text = "покажи весь асортимент"
        elif text == "💰 Борг":
            text = "який у мене борг?"
        elif text == "📋 Замовлення":
            if client_rec:
                invs = [i for i in (db.get("invoices") or []) if i.get("clientId") == client_rec.get("id")]
                invs.sort(key=lambda x: x.get("num",0), reverse=True)
                if not invs:
                    await update.message.reply_text("У вас поки немає замовлень.")
                    return
                last5 = invs[:5]
                result = "📋 *Ваші останні замовлення:*\n\n"
                for inv in last5:
                    num = str(inv.get("num",0)).zfill(4)
                    d = inv.get("date","")
                    total = inv.get("total",0)
                    items_count = len(inv.get("items",[]))
                    src = "📱" if inv.get("source")=="miniapp" else "🤖" if inv.get("source")=="telegram" else "💻"
                    result += f"{src} #{num} від {d} — {items_count} поз. — *${total:.2f}*\n"
                await safe_reply(update.message, result)
                return
            else:
                await update.message.reply_text("Спочатку надішліть номер телефону для ідентифікації.")
                return
        elif text == "📞 Менеджер":
            # Get manager phone from Firebase config
            phone = "0982831328"  # default
            try:
                cfg = db.get("_config") or {}
                if cfg.get("managerPhone"):
                    phone = str(cfg["managerPhone"])
                    if len(phone) == 9 and phone[0] != '0':
                        phone = '0' + phone
            except: pass
            formatted = f"+38{phone}" if phone.startswith('0') else phone
            await update.message.reply_text(
                f"📞 Зв'язатися з менеджером:\n\n"
                f"Телефон: {formatted}\n"
                f"Viber/Telegram: {formatted}\n\n"
                f"Працюємо Пн-Пт 9:00-18:00"
            )
            return

        # Try to identify by phone number
        if not client_rec:
            # Check if user is in registration process
            if user_id in pending_registration:
                reg = pending_registration[user_id]
                if reg["step"] == "name":
                    # User sent their name — create client
                    name = text.strip()
                    if len(name) < 2:
                        await update.message.reply_text("Будь ласка, введіть повне ім'я (наприклад: Іванов Іван Іванович)")
                        return
                    new_client = {
                        "id": int(time.time()*1000),
                        "name": name,
                        "phone": reg["phone"],
                        "telegramId": user_id,
                        "priceLevel": "d2",
                        "deferDays": 0
                    }
                    clients.append(new_client)
                    await fb_patch({"clients": clients})
                    _db_cache_time = 0  # invalidate cache
                    del pending_registration[user_id]
                    await safe_reply(update.message,
                        f"✅ Реєстрацію завершено!\n\n"
                        f"*{name}*\n"
                        f"📱 {reg['phone']}\n\n"
                        f"Ласкаво просимо до FlashSmart! 🎉",
                        reply_markup=main_menu_keyboard()
                    )
                    # Notify owner about new client
                    try:
                        await context.bot.send_message(
                            chat_id=OWNER_ID,
                            text=f"🆕 Новий клієнт зареєструвався!\n\n👤 {name}\n📱 {reg['phone']}"
                        )
                    except: pass
                    return
            
            phone_match = re.search(r'[\+]?[\d\s\-\(\)]{10,}', text)
            if phone_match:
                phone = phone_match.group().strip()
                client_rec = find_client_by_phone(clients, phone)
                if client_rec:
                    client_rec["telegramId"] = user_id
                    await fb_patch({"clients": clients})
                    await safe_reply(update.message, 
                        f"✅ Ідентифіковано: *{client_rec.get('name')}*\nТепер можете робити замовлення!",
                        reply_markup=main_menu_keyboard()
                    )
                    return
                else:
                    # Phone not found — offer registration
                    pending_registration[user_id] = {"phone": phone, "step": "name"}
                    await update.message.reply_text(
                        f"Номер {phone} не знайдено в базі.\n\n"
                        f"Давайте зареєструємось! Напишіть ваше ім'я та прізвище:"
                    )
                    return
            # Not identified and no phone — block
            await update.message.reply_text(
                "Для доступу до каталогу та замовлень потрібна ідентифікація.\n\n"
                "📱 Надішліть свій номер телефону (той, який зареєстрований у нас).\n"
                "Якщо ви новий клієнт — надішліть свій номер і ми вас зареєструємо."
            )
            return

        response = await process_message(user_id, text, db, client_rec)
        markup = main_menu_keyboard()
        if user_id in pending_orders:
            markup = confirm_keyboard()
        await safe_reply(update.message, response, reply_markup=markup)

        if user_id != OWNER_ID:
            user = update.effective_user
            name = client_rec.get("name") if client_rec else f"{user.first_name} (@{user.username})"
            try:
                await context.bot.send_message(chat_id=OWNER_ID, text=f"💬 {name}:\n{text[:100]}")
            except:
                pass
    except Exception as e:
        logger.error(f"Error handling message from {user_id}: {e}", exc_info=True)
        try:
            await update.message.reply_text("Вибачте, сталась помилка. Спробуйте ще раз.")
        except:
            pass

async def clear_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conversations.pop(update.effective_user.id, None)
    pending_orders.pop(update.effective_user.id, None)
    pending_registration.pop(update.effective_user.id, None)
    await update.message.reply_text("Розмову очищено!")

async def _send_backup_file(chat_id, bot, caption_prefix=""):
    """Helper: generate backup JSON and send to chat. Returns True on success."""
    data = await fb_get()
    import io, json as jsonlib
    buf = io.BytesIO(jsonlib.dumps(data, ensure_ascii=False, indent=2).encode("utf-8"))
    from datetime import datetime as dt
    stamp = dt.now().strftime("%Y-%m-%d_%H%M")
    buf.name = f"sklad-backup-{stamp}.json"
    size_kb = len(buf.getvalue()) / 1024
    buf.seek(0)
    await bot.send_document(
        chat_id=chat_id,
        document=buf,
        filename=buf.name,
        caption=f"{caption_prefix}✅ Бекап FlashSmart\n📅 {stamp}\n📦 {size_kb:.1f} KB"
    )
    return True

async def backup_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id != OWNER_ID:
        return
    try:
        await update.message.reply_text("⏳ Готую бекап...")
        await _send_backup_file(update.effective_chat.id, context.bot)
    except Exception as e:
        logger.error(f"Backup error: {e}")
        await update.message.reply_text(f"❌ Помилка бекапу: {str(e)[:200]}")

async def daily_backup_loop(bot):
    """Runs in background. Sends daily backup to owner at 03:00 local time."""
    import asyncio
    from datetime import datetime as dt, timedelta as td
    while True:
        try:
            now = dt.now()
            # Next 03:00
            target = now.replace(hour=3, minute=0, second=0, microsecond=0)
            if target <= now:
                target += td(days=1)
            sleep_sec = (target - now).total_seconds()
            logger.info(f"Next auto-backup in {sleep_sec/3600:.1f}h at {target}")
            await asyncio.sleep(sleep_sec)
            if OWNER_ID:
                await _send_backup_file(OWNER_ID, bot, caption_prefix="🌙 Автобекап\n\n")
                logger.info("Daily auto-backup sent")
        except Exception as e:
            logger.error(f"Daily backup error: {e}")
            # On error, wait 1 hour before retry
            import asyncio
            await asyncio.sleep(3600)

async def post_init(application):
    """Runs once when bot starts. Launches background tasks."""
    import asyncio
    asyncio.create_task(daily_backup_loop(application.bot))

def main():
    app = Application.builder().token(TELEGRAM_TOKEN).post_init(post_init).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("clear", clear_history))
    app.add_handler(CommandHandler("backup", backup_cmd))
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(MessageHandler(filters.PHOTO & ~filters.COMMAND, handle_message))
    logger.info("Bot started!")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
