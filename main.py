import os
import json
import logging
import asyncio
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes
import httpx
from anthropic import AsyncAnthropic

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Config from environment
TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
ANTHROPIC_KEY = os.environ["ANTHROPIC_API_KEY"]
FB_URL = os.environ["FIREBASE_URL"]
FB_EMAIL = os.environ["FIREBASE_EMAIL"]
FB_PASSWORD = os.environ["FIREBASE_PASSWORD"]
FB_API_KEY = os.environ["FIREBASE_API_KEY"]
OWNER_ID = int(os.environ.get("OWNER_TELEGRAM_ID", "0"))

client = AsyncAnthropic(api_key=ANTHROPIC_KEY)

# Firebase auth
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
        _fb_token = data["idToken"]
        _fb_token_exp = time.time() + 3000
    return _fb_token

async def fb_get(path):
    token = await get_fb_token()
    async with httpx.AsyncClient() as http:
        r = await http.get(f"{FB_URL}/{path}.json?auth={token}")
        return r.json()

async def fb_patch(path, data):
    token = await get_fb_token()
    async with httpx.AsyncClient() as http:
        r = await http.patch(
            f"{FB_URL}/{path}.json?auth={token}",
            json=data
        )
        return r.json()

async def fb_put(path, data):
    token = await get_fb_token()
    async with httpx.AsyncClient() as http:
        r = await http.put(
            f"{FB_URL}/{path}.json?auth={token}",
            json=data
        )
        return r.json()

# Load all DB data
async def load_db():
    return await fb_get("sklad")

# Conversation history per user
conversations = {}

SYSTEM_PROMPT = """Ти — AI менеджер з гуртового продажу мобільних аксесуарів (захисне скло, кабелі, МЗП, навушники).
Твоє завдання: спілкуватися з клієнтами українською мовою, виявляти потребу, пропонувати товари, оформлювати замовлення.

Правила:
1. Будь ввічливим, професійним, але живим — не роботизованим
2. Завжди перевіряй наявність товару перед пропозицією
3. Пропонуй альтернативи якщо товар закінчився
4. Уточнюй кількість і ціновий рівень (Гурт/Дилер 1/Дилер 2)
5. Перед оформленням накладної — підтверди замовлення
6. Якщо клієнт має борг — делікатно нагадай

Коли потрібно виконати дію (перевірити залишки, оформити накладну) — відповідай у форматі JSON:
{"action": "check_stock", "product": "назва"}
{"action": "create_invoice", "client_id": 1, "items": [{"product_id": 1, "qty": 10, "price": 2.50}]}
{"action": "check_debt", "client_id": 1}
{"action": "list_products", "category": "Захисне скло"}

Інакше відповідай звичайним текстом."""

async def process_ai_response(user_id, message, db):
    if user_id not in conversations:
        conversations[user_id] = []
    
    conversations[user_id].append({"role": "user", "content": message})
    
    # Build context about DB
    products = db.get("products", [])
    clients = db.get("clients", [])
    incomes = db.get("incomes", [])
    invoices = db.get("invoices", [])
    
    # Calculate stock
    def get_stock(pid):
        p = next((x for x in products if x.get("id") == pid), None)
        if not p:
            return 0
        sold = sum(
            item.get("qty", 0)
            for inv in invoices
            for item in inv.get("items", [])
            if item.get("productId") == pid
        )
        return p.get("income", 0) - sold
    
    # Stock summary for context
    stock_summary = []
    for p in products[:30]:  # limit for context
        stock = get_stock(p.get("id"))
        if stock > 0:
            stock_summary.append(f"{p.get('name')}: {stock} шт")
    
    context = f"""
Поточні залишки (топ товари):
{chr(10).join(stock_summary[:20])}

Клієнти: {[c.get('name') for c in clients]}
"""
    
    messages = conversations[user_id][-10:]  # last 10 messages
    
    response = await client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1000,
        system=SYSTEM_PROMPT + "\n\nДані бази:\n" + context,
        messages=messages
    )
    
    ai_text = response.content[0].text
    conversations[user_id].append({"role": "assistant", "content": ai_text})
    
    # Check if AI wants to perform action
    try:
        if ai_text.strip().startswith("{"):
            action_data = json.loads(ai_text)
            return await execute_action(action_data, db, products, clients, invoices, get_stock)
    except:
        pass
    
    return ai_text

async def execute_action(action_data, db, products, clients, invoices, get_stock):
    action = action_data.get("action")
    
    if action == "check_stock":
        query = action_data.get("product", "").lower()
        matches = [p for p in products if query in p.get("name", "").lower()]
        if not matches:
            return "Товар не знайдено в каталозі."
        result = "📦 Залишки:\n"
        for p in matches[:5]:
            stock = get_stock(p.get("id"))
            status = "✅" if stock > 10 else ("⚠️" if stock > 0 else "❌")
            result += f"{status} {p.get('name')}: {stock} шт\n"
        return result
    
    elif action == "list_products":
        cat = action_data.get("category", "")
        filtered = [p for p in products if cat.lower() in p.get("category", "").lower()]
        if not filtered:
            filtered = products[:20]
        result = f"📋 Товари ({cat or 'всі'}):\n"
        for p in filtered[:15]:
            stock = get_stock(p.get("id"))
            if stock > 0:
                result += f"• {p.get('name')} — {stock} шт\n"
        return result
    
    elif action == "check_debt":
        client_id = action_data.get("client_id")
        c = next((x for x in clients if x.get("id") == client_id), None)
        if not c:
            return "Клієнта не знайдено."
        sold = sum(inv.get("total", 0) for inv in invoices if inv.get("clientId") == client_id)
        payments = db.get("payments", [])
        paid = sum(p.get("amount", 0) for p in payments if p.get("clientId") == client_id and p.get("amount", 0) > 0)
        debt = sold - paid
        if debt > 0.01:
            return f"💰 Борг {c.get('name')}: ${debt:.2f}"
        return f"✅ {c.get('name')} — боргів немає"
    
    elif action == "create_invoice":
        return "📝 Для оформлення накладної підтвердіть замовлення — я передам дані менеджеру."
    
    return "Дію виконано."

async def notify_owner(app, message):
    if OWNER_ID:
        try:
            await app.bot.send_message(chat_id=OWNER_ID, text=message)
        except:
            pass

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    db = await load_db()
    clients = db.get("clients", []) if db else []
    
    # Check if client exists
    client_match = next((c for c in clients if str(c.get("phone", "")) in (user.username or "")), None)
    
    welcome = f"Вітаємо! 👋\n\nЯ — AI менеджер FlashSmart.\nДопоможу підібрати товар, перевірити наявність та оформити замовлення.\n\nЧим можу допомогти?"
    await update.message.reply_text(welcome)

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text = update.message.text
    
    # Show typing
    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")
    
    try:
        db = await load_db()
        response = await process_ai_response(user_id, text, db or {})
        await update.message.reply_text(response)
        
        # Notify owner about new message
        if user_id != OWNER_ID:
            user = update.effective_user
            await notify_owner(context.application, f"💬 Повідомлення від {user.first_name} (@{user.username}):\n{text[:100]}")
    
    except Exception as e:
        logger.error(f"Error: {e}")
        await update.message.reply_text("Вибачте, сталась помилка. Спробуйте ще раз.")

async def clear_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    conversations.pop(user_id, None)
    await update.message.reply_text("Розмову очищено. Починаємо спочатку!")

def main():
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("clear", clear_history))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    
    logger.info("Bot started!")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
