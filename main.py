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
    return _fb_token

async def fb_get(path):
    token = await get_fb_token()
    async with httpx.AsyncClient() as http:
        r = await http.get(f"{FB_URL}/{path}.json?auth={token}")
        return r.json()

async def load_db():
    return await fb_get(".")

conversations = {}

SYSTEM_PROMPT = """Ти — AI менеджер з продажу мобільних аксесуарів компанії FlashSmart (захисне скло, кабелі, МЗП, навушники).
Спілкуйся виключно українською мовою. Будь живим, дружнім і професійним.

ВАЖЛИВО: Коли потрібно виконати дію — відповідай ТІЛЬКИ JSON без жодного тексту до чи після:
{"action": "list_products", "category": "all"}
{"action": "list_products", "category": "Захисне скло"}
{"action": "check_stock", "product": "назва товару"}
{"action": "check_debt", "client_id": 1}

В інших випадках — звичайний текст без JSON.

Твої можливості:
- Показати наявні товари по категоріях
- Перевірити залишок конкретного товару
- Допомогти підібрати товар під потребу клієнта
- Перевірити борг клієнта
- Розповісти про ціни (запитай у якому ціновому рівні — Гурт, Дилер 1, Дилер 2)"""

def get_stock(pid, products, invoices):
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

async def execute_action(action_data, db):
    products = db.get("products", []) or []
    clients = db.get("clients", []) or []
    invoices = db.get("invoices", []) or []
    payments = db.get("payments", []) or []
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
            return "На жаль, зараз немає товарів в наявності в цій категорії."
        
        # Group by category
        by_cat = {}
        for p, s in in_stock:
            c = p.get("category", "Інше")
            if c not in by_cat:
                by_cat[c] = []
            by_cat[c].append((p, s))
        
        result = "📦 Товари в наявності:\n\n"
        for cat_name, items in by_cat.items():
            result += f"*{cat_name}*\n"
            for p, s in items[:10]:
                status = "✅" if s > 20 else "⚠️"
                result += f"{status} {p.get('name')} — {s} шт\n"
            result += "\n"
        return result.strip()

    elif action == "check_stock":
        query = action_data.get("product", "").lower()
        matches = [p for p in products if query in p.get("name", "").lower()]
        if not matches:
            return f"Товар '{query}' не знайдено в каталозі."
        result = "📦 Результати пошуку:\n\n"
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
            return f"💰 Борг {c.get('name')}: ${debt:.2f}"
        return f"✅ {c.get('name')} — боргів немає"

    return "Дію виконано."

async def process_message(user_id, message, db):
    if user_id not in conversations:
        conversations[user_id] = []

    products = db.get("products", []) or []
    invoices = db.get("invoices", []) or []

    in_stock = [p for p in products if get_stock(p.get("id"), products, invoices) > 0]
    stock_list = "\n".join([f"- {p.get('name')}: {get_stock(p.get('id'), products, invoices)} шт" for p in in_stock[:30]])

    context = f"Товари в наявності ({len(in_stock)} позицій):\n{stock_list}"

    conversations[user_id].append({"role": "user", "content": message})

    response = await client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=800,
        system=SYSTEM_PROMPT + "\n\n" + context,
        messages=conversations[user_id][-10:]
    )

    ai_text = response.content[0].text.strip()
    conversations[user_id].append({"role": "assistant", "content": ai_text})

    # Check if response contains JSON action
    json_match = re.search(r'\{[^{}]+\}', ai_text)
    if json_match:
        try:
            action_data = json.loads(json_match.group())
            result = await execute_action(action_data, db)
            conversations[user_id][-1]["content"] = result
            return result
        except:
            pass

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
        db = await load_db() or {}
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
