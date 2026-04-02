import os, json, re, logging, time
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
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

async def load_db():
    data = await fb_get()
    if not data or not isinstance(data, dict):
        return {}
    for key in ["products","invoices","incomes","payments","clients","returns"]:
        if key in data and isinstance(data[key], dict):
            data[key] = list(data[key].values())
    logger.info(f"DB: products={len(data.get('products',[]))}, clients={len(data.get('clients',[]))}, invoices={len(data.get('invoices',[]))}")
    return data

# ── Helpers ──
def get_stock(pid, products, invoices):
    p = next((x for x in products if x.get("id") == pid), None)
    if not p: return 0
    sold = sum(item.get("qty",0) for inv in invoices for item in (inv.get("items") or []) if item.get("productId") == pid)
    return (p.get("income") or 0) - sold

def get_price(product, price_level):
    level_map = {"gurt":"priceGurt","d1":"priceD1","d2":"priceD2"}
    field = level_map.get(price_level, "priceD2")
    return product.get(field) or product.get("priceD2") or 0

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

ТВІЙ ХАРАКТЕР:
— Привітний, але діловий. Не лебезиш, не перегинаєш з емодзі
— Говориш як людина, а не як робот — живо, природно, з гумором де доречно
— Спілкуєшся ВИКЛЮЧНО мовою клієнта — якщо написав українською, відповідаєш тільки українською, якщо російською — тільки російською, якщо англійською — тільки англійською
— КАТЕГОРИЧНО ЗАБОРОНЕНО змішувати мови в одному повідомленні — жодного слова з іншої мови
— Визначаєш мову з першого повідомлення і не змінюєш її до кінця розмови якщо клієнт сам не переключився
— Пам'ятаєш ім'я клієнта і звертаєшся на ім'я або по імені-батькові
— Впевнений у своєму товарі, але не нав'язливий

ТВОЯ МЕТА: збільшити суму замовлення, зберегти клієнта і побудувати довгострокові відносини.

═══════════════════════════════
СЦЕНАРІЇ СПІЛКУВАННЯ:
═══════════════════════════════

1. ПЕРШИЙ КОНТАКТ:
— Привітайся тепло, представся
— Якщо клієнт є в базі — звернись на ім'я, згадай останнє замовлення якщо є
— Якщо новий — запитай звідки дізнався, яким напрямком займається
— Запропонуй розповісти про асортимент або одразу перейти до замовлення

2. ПОКАЗ АСОРТИМЕНТУ:
— Коли питають про категорію — виконай JSON дію, отримаєш список
— Після списку — прокоментуй топові позиції, поясни різницю між моделями
— Наприклад: "Doberman Glass 20в1 — це наш хіт, упаковка з 20 шт для iPhone 16, йде дуже добре у дрібних магазинах"
— Завжди пропонуй супутні товари: скло → кабелі → зарядки

3. РОБОТА З ЗАМОВЛЕННЯМ:
— Коли клієнт називає товари — виконай JSON create_order
— Після підтвердження суми — ЗАВЖДИ запропонуй добрати до круглої суми або до безкоштовної доставки
— Приклад: "Роман, у вас вийшло $340. Якщо добрати ще на $60 — можу зробити невелику знижку на наступне замовлення"
— Пропонуй супутні товари які логічно доповнюють замовлення

4. ДОЖИМ (м'який, ненав'язливий):
— "Подумаю" → "Звичайно, але зверніть увагу — ці позиції у нас зараз в обмеженій кількості"
— "Дорого" → "Розумію. Давайте подивимось що можна оптимізувати — можливо є аналог дешевше або варто взяти більший об'єм"
— Мовчить → через час: "Романе, ви зупинились на якомусь питанні? Можу допомогти"

5. ЗБІЛЬШЕННЯ ЧЕКА:
— Завжди пропонуй: "До цього замовлення добре йде..."
— Якщо бере скло — пропонуй кабелі тієї ж серії
— Якщо бере кабелі — пропонуй зарядки
— Якщо бере зарядки — пропонуй кабелі і скло

6. ЦІНИ І ЗНИЖКИ:
— Знаєш ціновий рівень клієнта і не обговорюєш ціни інших рівнів
— Знижки не даєш самовільно — кажеш "уточню у керівника"
— Якщо питають чому так дорого — пояснюєш якість і сервіс

7. БОРГ:
— Якщо є борг — згадуєш делікатно в кінці розмови: "До речі, за вами є невелика заборгованість $X — не забудьте при наступній оплаті"
— Не тисни і не соромиш

8. ПІСЛЯ ЗАМОВЛЕННЯ:
— Підтверди замовлення, назви суму, скажи що менеджер зв'яжеться
— Подякуй за співпрацю
— Запропонуй писати якщо будуть питання

═══════════════════════════════
ТЕХНІЧНІ ПРАВИЛА (КРИТИЧНО ВАЖЛИВО):
═══════════════════════════════

Коли потрібна дія зі складом — відповідай ТІЛЬКИ JSON одним рядком, БЕЗ будь-якого тексту:

Питання про категорію → {"action":"list_products","category":"Навушники"}
Пошук товару → {"action":"check_stock","product":"назва"}
Замовлення → {"action":"create_order","items":[{"name":"точна назва","qty":10}]}
Борг → {"action":"check_debt"}

Категорії: навушники/наушники/headphones → Навушники
Кабелі/cable/провід/шнур → Кабелі  
Скло/glass/захист → Захисне скло
Зарядка/charger/МЗП/адаптер → МЗП
Все/асортимент/прайс → all

ЗАБОРОНЕНО:
— Писати текст разом з JSON
— Вигадувати товари яких немає в базі
— Самостійно писати "замовлення прийнято" — це робить система
— Обіцяти знижки без підтвердження

Після отримання даних зі складу — відповідай як живий менеджер, природно і професійно."""

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
                price_str = f" — ${price:.2f}" if price > 0 else ""
                status = "✅" if s>20 else "⚠️"
                result += f"{status} {p.get('name')}{price_str} ({s} шт)\n"
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
            status = "✅ В наявності" if s>0 else "❌ Немає"
            price_str = f" — ${price:.2f}" if price>0 else ""
            result += f"{status}: {p.get('name')}{price_str} ({s} шт)\n"
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
            price_str = f"${item['price']:.2f}/шт" if item['price']>0 else "ціна не вст."
            result += f"• {item['name']} × {item['qty']} шт ({price_str}) = ${item['sum']:.2f}\n"
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
        return "Немає активного замовлення."
    client_rec = order["client"]
    if not client_rec:
        return "Клієнт не визначений."
    invoices = db.get("invoices") or []
    next_num = (db.get("nextInvNum") or 1)
    inv_id = int(time.time()*1000)
    items = [{
        "productId": i["productId"],
        "qty": i["qty"],
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
    await fb_patch({"invoices": invoices, "nextInvNum": next_num+1})
    del pending_orders[user_id]
    return f"✅ Накладна *#{str(next_num).zfill(4)}* створена!\n💰 Сума: ${order['total']:.2f}"

async def process_message(user_id, text, db, client_rec):
    if user_id not in conversations:
        conversations[user_id] = []

    # Check for order confirmation
    text_lower = text.lower().strip()
    if text_lower in ["підтверджую","підтверджую замовлення","confirm","так","yes","ок","ok","добре"]:
        if user_id in pending_orders:
            return await save_invoice(user_id, db)

    if text_lower in ["скасувати","скасувати замовлення","cancel","ні","no"]:
        if user_id in pending_orders:
            del pending_orders[user_id]
            return "Замовлення скасовано."

    products = db.get("products") or []
    invoices = db.get("invoices") or []
    price_level = client_rec.get("priceLevel","d2") if client_rec else "d2"
    in_stock = [(p, get_stock(p.get("id"), products, invoices)) for p in products]
    in_stock = [(p,s) for p,s in in_stock if s>0]
    stock_list = "\n".join([f"- {p.get('name')} ({p.get('category')}): {s} шт" for p,s in in_stock[:40]])
    client_info = f"Клієнт: {client_rec.get('name')}, рівень: {price_level}" if client_rec else "Клієнт не ідентифікований"
    context = f"{client_info}\n\nТовари в наявності ({len(in_stock)} поз.):\n{stock_list}"

    conversations[user_id].append({"role":"user","content":text})
    response = await client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=1000,
        system=SYSTEM_PROMPT+"\n\n"+context,
        messages=conversations[user_id][-10:]
    )
    ai_text = response.content[0].text.strip()
    conversations[user_id].append({"role":"assistant","content":ai_text})

    json_match = re.search(r'\{[^{}]+\}', ai_text)
    if json_match:
        try:
            action_data = json.loads(json_match.group())
            if "action" in action_data:
                result = await execute_action(action_data, db, user_id, client_rec)
                conversations[user_id][-1]["content"] = result
                return result
        except Exception as e:
            logger.error(f"JSON error: {e}")
    return ai_text

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    db = await load_db()
    client_rec = find_client_by_tg(db.get("clients",[]), user_id)
    if client_rec:
        await update.message.reply_text(f"Вітаємо, {client_rec.get('name')}! 👋\n\nЯ — AI менеджер FlashSmart. Чим можу допомогти?")
    else:
        await update.message.reply_text(
            "Вітаємо! 👋\n\nЯ — AI менеджер FlashSmart.\n\n"
            "Для оформлення замовлень, будь ласка, надішліть свій номер телефону для ідентифікації."
        )

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text = update.message.text
    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")
    try:
        db = await load_db()
        clients = db.get("clients") or []
        client_rec = find_client_by_tg(clients, user_id)

        # Try to identify by phone number
        if not client_rec:
            phone_match = re.search(r'[\+]?[\d\s\-\(\)]{10,}', text)
            if phone_match:
                client_rec = find_client_by_phone(clients, phone_match.group())
                if client_rec:
                    # Save telegram_id to client
                    client_rec["telegramId"] = user_id
                    await fb_patch({"clients": clients})
                    await update.message.reply_text(f"✅ Ідентифіковано: *{client_rec.get('name')}*\nТепер можете робити замовлення!", parse_mode="Markdown")
                    return

        response = await process_message(user_id, text, db, client_rec)
        await update.message.reply_text(response, parse_mode="Markdown")

        if user_id != OWNER_ID:
            user = update.effective_user
            name = client_rec.get("name") if client_rec else f"{user.first_name} (@{user.username})"
            try:
                await context.bot.send_message(chat_id=OWNER_ID, text=f"💬 {name}:\n{text[:100]}")
            except:
                pass
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        await update.message.reply_text("Вибачте, сталась помилка. Спробуйте ще раз.")

async def clear_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conversations.pop(update.effective_user.id, None)
    pending_orders.pop(update.effective_user.id, None)
    await update.message.reply_text("Розмову очищено!")

def main():
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("clear", clear_history))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    logger.info("Bot started!")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
