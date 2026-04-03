import os, json, re, logging, time
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
    token = await get_fb_token()
    url = f"{FB_URL}.json?auth={token}" if not path else f"{FB_URL}/{path}.json?auth={token}"
    async with httpx.AsyncClient(timeout=30) as http:
        r = await http.get(url)
        return r.json()

async def fb_patch(data):
    token = await get_fb_token()
    async with httpx.AsyncClient(timeout=30) as http:
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
    return (p.get("income") or 0) - sold

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

# ── Conversations & pending orders ──
conversations = {}
pending_orders = {}  # user_id -> {client, items, total}

SYSTEM_PROMPT = """Ти — Олег, досвідчений менеджер з гуртових продажів мобільних аксесуарів компанії FlashSmart. Працюєш з дилерами і оптовими клієнтами. Знаєш ринок аксесуарів до техніки Apple та Android вздовж і впоперек.

═══════════════════════════════
МОВНІ ПРАВИЛА (НАЙВИЩИЙ ПРІОРИТЕТ — ПОРУШЕННЯ НЕПРИПУСТИМІ):
═══════════════════════════════
1. Якщо клієнт пише УКРАЇНСЬКОЮ — відповідай ТІЛЬКИ ЧИСТОЮ українською. ЖОДНОГО російського слова!
2. ЗАБОРОНЕНІ слова та їх заміни:
   — "нужен/нужно" ✗ → "потрібен/потрібно" ✓
   — "есть" ✗ → "є" ✓
   — "конечно" ✗ → "звичайно" ✓
   — "получається/получити" ✗ → "виходить/отримати" ✓
   — "посмотріть/посмотрим" ✗ → "подивитись/подивимось" ✓
   — "скідка" ✗ → "знижка" ✓
   — "заказ" ✗ → "замовлення" ✓
   — "вообще" ✗ → "взагалі" ✓
   — "пожалуйста" ✗ → "будь ласка" ✓
   — "можно" ✗ → "можна" ✓
   — "хорошо" ✗ → "добре/гаразд" ✓
   — "предлагаю" ✗ → "пропоную" ✓
   — "обращайтесь" ✗ → "звертайтесь" ✓
   — "подскажіть" ✗ → "підкажіть" ✓
   — "класні/класно" ✗ → "чудові/чудово" ✓
   — "здєсь" ✗ → "тут" ✓
   — "здається" (рос.) ✗ → "здається" (укр.) або "мабуть" ✓
   — "каша" (в значенні безлад) ✗ → "плутанина" ✓
   — "прайс" ✗ → "прайс-лист" або "ціни" ✓
   — "нужен прайс" ✗ → "потрібні ціни" ✓
   — "сейчас" ✗ → "зараз" ✓
   — "тоже" ✗ → "також/теж" ✓
   — "только" ✗ → "тільки" ✓
   — "если" ✗ → "якщо" ✓
   — "кстати" ✗ → "до речі" ✓
   — "разберемось" ✗ → "розберемось" ✓
   — "помочь" ✗ → "допомогти" ✓
   — "подожди/подождіть" ✗ → "зачекай/зачекайте" ✓
   — "красиво" ✗ → "гарно" ✓
   — "дєшево/дєшевше" ✗ → "дешево/дешевше" ✓
   — "побольше" ✗ → "побільше" ✓
   — "нравиться" ✗ → "подобається" ✓
3. Перед відправкою КОЖНОЇ відповіді — перечитай її і переконайся що КОЖНЕ слово є українським. Це КРИТИЧНО ВАЖЛИВО.
4. Якщо клієнт пише російською — відповідай ТІЛЬКИ російською, без українських слів

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
                if pcs > 1:
                    packs_count = s // pcs
                    price_str = f" — ${price:.2f}/уп ({pcs}шт)" if price > 0 else ""
                    stock_str = f"{packs_count} уп ({s} шт)"
                else:
                    price_str = f" — ${price:.2f}" if price > 0 else ""
                    stock_str = f"{s} шт"
                status = "✅" if s>20 else "⚠️"
                result += f"{status} {p.get('name')}{price_str} — {stock_str}\n"
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
            logger.info(f"check_stock: {p.get('name')}, pcsPerPack={pcs}, price_level={price_level}, price={price}")
            status = "✅ В наявності" if s>0 else "❌ Немає"
            if pcs > 1:
                packs_count = s // pcs
                remainder = s % pcs
                stock_str = f"{packs_count} уп" + (f" + {remainder} шт" if remainder else "") + f" ({s} шт всього)"
                price_str = f" — ${price:.2f}/уп ({pcs} шт в уп)" if price>0 else ""
            else:
                stock_str = f"{s} шт"
                price_str = f" — ${price:.2f}/шт" if price>0 else ""
            result += f"{status}: {p.get('name')}{price_str} — залишок: {stock_str}\n"
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
            if stock < qty:
                no_stock.append(f"{p.get('name')} (є {stock} шт, потрібно {qty})")
                qty = stock  # use available qty
                if qty == 0:
                    continue
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
    order = pending_orders.get(user_id)
    if not order:
        logger.warning(f"save_invoice: no pending order for user {user_id}")
        return "Немає активного замовлення."
    client_rec = order["client"]
    if not client_rec:
        return "Клієнт не визначений."
    invoices = db.get("invoices") or []
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

    # Check for order confirmation
    text_lower = text.lower().strip()
    confirm_words = ["підтверджую","підтверджую замовлення","confirm","так","yes","ок","ok","добре","да","підтверджую!","підтверджую.","go","давай","оформляй","оформлюй"]
    cancel_words = ["скасувати","скасувати замовлення","cancel","ні","no","відміна","стоп"]
    
    if any(text_lower.startswith(w) for w in confirm_words):
        if user_id in pending_orders:
            logger.info(f"Order confirmed by user {user_id}")
            result = await save_invoice(user_id, db)
            return result
        
    if any(text_lower.startswith(w) for w in cancel_words):
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
            # For list/check actions — humanize via Claude
            action_type = action_data.get("action")
            if action_type in ("list_products", "check_stock", "check_debt"):
                try:
                    humanize = await client.messages.create(
                        model="claude-haiku-4-5-20251001",
                        max_tokens=500,
                        system="Ти — Олег, менеджер FlashSmart. Тобі система повернула дані зі складу. Перекажи їх клієнту як живий менеджер — коротко, по-діловому, українською. НЕ вигадуй нічого нового, використовуй ТІЛЬКИ дані із системи. Можеш додати одне коротке речення-пропозицію. ЗАБОРОНЕНО: російські слова, вигадані ціни, зайва балаканина.",
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
            [KeyboardButton("📋 Замовлення"), KeyboardButton("💬 Чат")],
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

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    db = await load_db()
    client_rec = find_client_by_tg(db.get("clients",[]), user_id)
    if client_rec:
        await update.message.reply_text(
            f"Привіт, {client_rec.get('name')}! 👋\nЯ — Олег, AI менеджер FlashSmart.\nОберіть що потрібно 👇",
            reply_markup=main_menu_keyboard()
        )
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
            db = await load_db()
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

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
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
        elif text == "💬 Чат":
            await update.message.reply_text("Пишіть — я на зв'язку! Можу допомогти з вибором товару, оформити замовлення або відповісти на питання. 💬")
            return

        # Try to identify by phone number
        if not client_rec:
            phone_match = re.search(r'[\+]?[\d\s\-\(\)]{10,}', text)
            if phone_match:
                client_rec = find_client_by_phone(clients, phone_match.group())
                if client_rec:
                    client_rec["telegramId"] = user_id
                    await fb_patch({"clients": clients})
                    await safe_reply(update.message, 
                        f"✅ Ідентифіковано: *{client_rec.get('name')}*\nТепер можете робити замовлення!",
                        reply_markup=main_menu_keyboard()
                    )
                    return
                else:
                    await update.message.reply_text("❌ Номер не знайдено в базі. Перевірте номер або зверніться до менеджера.")
                    return
            # Not identified and no phone — block
            await update.message.reply_text(
                "Для доступу до каталогу та замовлень потрібна ідентифікація.\n\n"
                "📱 Надішліть свій номер телефону (той, який зареєстрований у нас)."
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
    await update.message.reply_text("Розмову очищено!")

def main():
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("clear", clear_history))
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    logger.info("Bot started!")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
