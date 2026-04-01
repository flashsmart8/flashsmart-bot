import os
import json
import re
import logging
import asyncio
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
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
    import time
    if _fb_token and time.time() < _fb_token_exp:
        return _fb_token
    async with httpx.AsyncClient() as http:
        r = await http.post(
            f"https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword?key={FB_API_KEY}",
            json={"email": FB_EMAIL, "password": FB_PASSWORD, "returnSecureToken": True}
        )
        data = r.json()
        if "idToken" not in data:
            logger.error(f"Firebase auth failed: {data}")
            raise Exception(f"Firebase auth error: {data.get('error', {}).get('message', 'Unknown')}")
        _fb_token = data["idToken"]
        _fb_token_exp = time.time() + 3000
        logger.info("Firebase auth OK")
    return _fb_token

async def load_db():
    token = await get_fb_token()
    async with httpx.AsyncClient(timeout=30) as http:
        r = await http.get(f"{FB_URL}.json?auth={token}")
        data = r.json()
        if not data or not isinstance(data, dict):
            logger.error(f"load_db: bad data type={type(data)}, val={str(data)[:200]}")
            return {}
        # Convert dict to list if needed (Firebase can return dict)
        for key in ["products", "invoices", "incomes", "payments", "clients", "returns"]:
            if key in data and isinstance(data[key], dict):
                data[key] = list(data[key].values())
        prods = data.get("products", []) or []
        invs = data.get("invoices", []) or []
        logger.info(f"load_db OK: products={len(prods)}, invoices={len(invs)}")
        return data

conversations = {}

SYSTEM_PROMPT = """Ти — AI менеджер FlashSmart. Категорії товарів ТІЛЬКИ: Захисне скло, Кабелі, МЗП, Навушники. Мова: українська.

ПРАВИЛО №1: Коли клієнт згадує товар або категорію — ЗАВЖДИ відповідай ТІЛЬКИ JSON:
Навушники/earbuds/headphones -> {"action": "list_products", "category": "Навушники"}
Кабелі/cable/дроти -> {"action": "list_products", "category": "Кабелі"}
Захисне скло/glass/скло -> {"action": "list_products", "category": "Захисне скло"}
МЗП/зарядка/charger/адаптер -> {"action": "list_products", "category": "МЗП"}
Що є/наявність/асортимент -> {"action": "list_products", "category": "all"}
Конкретна назва товару -> {"action": "check_stock", "product": "назва"}

ПРАВИЛО №2: Ніколи не стверджуй що товару немає — завжди спочатку перевір через JSON.
ПРАВИЛО №3: JSON відповідь — БЕЗ будь-якого іншого тексту.

Після отримання даних про товар — спілкуйся живо, пропонуй, уточнюй кількість і ціновий рівень."""

def get_stock(pid, products, invoices):
    p = next((x for x in products if x.get("id") == pid), None)
    if not p:
        return 0
    sold = sum(
        item.get("qty", 0)
        for inv in invoices
        for item in (inv.get("items") or [])
        if item.get("productId") == pid
    )
    return (p.get("income") or 0) - sold

async def execute_action(action_data, db):
    products = db.get("products") or []
    clients = db.get("clients") or []
    invoices = db.get("invoices") or []
    payments = db.get("payments") or []
    action = action_data.get("action")

    if action == "list_products":
        cat = action_data.get("category", "all")
        if cat == "all":
            filtered = products
        else:
            filtered = [p for p in products if cat.lower() in p.get("category", "").lower()]

        in_stock = [(p, get_stock(p.get("id"), products, invoices)) for p in filtered]
        in_stock = [(p, s) for p, s in in_stock if s > 0]

        if not in_stock:
            return f"На жаль, зараз немає товарів в наявності{' в категорії ' + cat if cat != 'all' else ''}.\n\nКатегорій з товарами: {len(set(p.get('category') for p in products if get_stock(p.get('id'), products, invoices) > 0))}"

        by_cat = {}
        for p, s in in_stock:
            c = p.get("category", "Інше")
            if c not in by_cat:
                by_cat[c] = []
            by_cat[c].append((p, s))

        result = f"📦 Товари в наявності ({len(in_stock)} поз.):\n\n"
        for cat_name, items in by_cat.items():
            result += f"*{cat_name}*\n"
            for p, s in items[:15]:
                status = "✅" if s > 20 else "⚠️"
                result += f"{status} {p.get('name')} — {s} шт\n"
            result += "\n"
        return result.strip()

    elif action == "check_stock":
        query = action_data.get("product", "").lower()
        matches = [p for p in products if query in p.get("name", "").lower()]
        if not matches:
            return f"Товар не знайдено."
        result = "📦 Результати:\n\n"
        for p in matches[:5]:
            s = get_stock(p.get("id"), products, invoices)
            status = "✅ В наявності" if s > 0 else "❌ Немає"
            result += f"{status}: {p.get('name')} — {s} шт\n"
        return result

    elif action == "check_debt":
        client_id = action_data.get("client_id")
        c = next((x for x in clients if x.get("id") == client_id), None)
        if not c:
            return "Клієнта не знайдено."
        sold = sum(inv.get("total", 0) for inv in invoices if inv.get("clientId") == client_id)
        paid = sum(p.get("amount", 0) for p in payments if p.get("clientId") == client_id and p.get("amount", 0) > 0)
        debt = sold - paid
        if debt > 0.01:
            return f"Борг {c.get('name')}: ${debt:.2f}"
        return f"✅ {c.get('name')} — боргів немає"

    return "Дію виконано."

async def process_message(user_id, message, db):
    if user_id not in conversations:
        conversations[user_id] = []

    products = db.get("products") or []
    invoices = db.get("invoices") or []

    in_stock = [(p, get_stock(p.get("id"), products, invoices)) for p in products]
    in_stock = [(p, s) for p, s in in_stock if s > 0]
    stock_list = "\n".join([f"- {p.get('name')} ({p.get('category')}): {s} шт" for p, s in in_stock[:40]])
    context = f"Товари в наявності ({len(in_stock)} поз.):\n{stock_list}" if in_stock else "Склад порожній."

    conversations[user_id].append({"role": "user", "content": message})

    response = await client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=800,
        system=SYSTEM_PROMPT + "\n\n" + context,
        messages=conversations[user_id][-10:]
    )

    ai_text = response.content[0].text.strip()
    conversations[user_id].append({"role": "assistant", "content": ai_text})

    json_match = re.search(r'\{[^{}]+\}', ai_text)
    if json_match:
        try:
            action_data = json.loads(json_match.group())
            if "action" in action_data:
                result = await execute_action(action_data, db)
                conversations[user_id][-1]["content"] = result
                return result
        except Exception as e:
            logger.error(f"JSON parse error: {e}")

    return ai_text

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Вітаємо! 👋\n\nЯ — AI менеджер FlashSmart.\n"
        "Допоможу підібрати товар, перевірити наявність та оформити замовлення.\n\n"
        "Чим можу допомогти?"
    )

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text = update.message.text
    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")
    try:
        db = await load_db()
        response = await process_message(user_id, text, db)
        await update.message.reply_text(response, parse_mode="Markdown")
        if user_id != OWNER_ID:
            user = update.effective_user
            try:
                await context.bot.send_message(
                    chat_id=OWNER_ID,
                    text=f"💬 {user.first_name} (@{user.username}):\n{text[:100]}"
                )
            except:
                pass
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        await update.message.reply_text("Вибачте, сталась помилка. Спробуйте ще раз.")

async def clear_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conversations.pop(update.effective_user.id, None)
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
