import asyncio
import logging
import os
from datetime import datetime, timedelta

import aiosqlite
import pandas as pd
import matplotlib.pyplot as plt
from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    Message,
    ReplyKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardRemove,
    FSInputFile,
)

# =========================
# SOZLAMALAR
# =========================
TOKEN = os.getenv("8681921784:AAEeP2ekwGEGqOJ0QmBopFg7EBnW3Ok4cCY")
DB_NAME = "finance_v4.db"
EXPORT_FOLDER = "exports"
CHART_FOLDER = "charts"
ADMIN_ID = 1165988187  # BU YERGA O'Z TELEGRAM ID INGIZNI YOZING

os.makedirs(EXPORT_FOLDER, exist_ok=True)
os.makedirs(CHART_FOLDER, exist_ok=True)

logging.basicConfig(level=logging.INFO)
bot = Bot(token=TOKEN)
dp = Dispatcher()

DEFAULT_INCOME_CATEGORIES = ["💼 Oylik", "🛒 Savdo", "💸 Qo‘shimcha daromad"]
DEFAULT_EXPENSE_CATEGORIES = ["🍽 Oziq-ovqat", "🚕 Transport", "🏠 Uy", "💊 Sog‘liq", "🎯 Boshqa"]

# =========================
# KLAVIATURALAR
# =========================
main_menu = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="➕ Daromad qo‘shish"), KeyboardButton(text="➖ Xarajat qo‘shish")],
        [KeyboardButton(text="📅 Sana bo‘yicha ko‘rish"), KeyboardButton(text="📊 Oylik hisobot")],
        [KeyboardButton(text="📁 Excel export"), KeyboardButton(text="📈 Diagramma")],
        [KeyboardButton(text="👤 Profil"), KeyboardButton(text="💰 Budjet limiti")],
        [KeyboardButton(text="🗂 Kategoriya qo‘shish"), KeyboardButton(text="🧾 Kategoriyalarim")],
        [KeyboardButton(text="ℹ️ Yordam")],
    ],
    resize_keyboard=True,
)

admin_menu = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="👥 Foydalanuvchilar soni"), KeyboardButton(text="📊 Umumiy statistika")],
        [KeyboardButton(text="🏆 Top foydalanuvchilar"), KeyboardButton(text="🔎 User qidirish")],
        [KeyboardButton(text="📁 Barcha ma’lumotlarni export")],
        [KeyboardButton(text="⬅️ Oddiy menyu")],
    ],
    resize_keyboard=True,
)

cancel_menu = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text="⬅️ Bekor qilish")]],
    resize_keyboard=True,
)

category_type_menu = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📈 Daromad kategoriyasi")],
        [KeyboardButton(text="📉 Xarajat kategoriyasi")],
        [KeyboardButton(text="⬅️ Bekor qilish")],
    ],
    resize_keyboard=True,
)

# =========================
# HOLATLAR
# =========================
class FinanceState(StatesGroup):
    choosing_income_category = State()
    entering_income_amount = State()
    choosing_expense_category = State()
    entering_expense_amount = State()
    entering_date_for_view = State()
    entering_budget_limit = State()
    choosing_custom_category_type = State()
    entering_custom_category_name = State()
    entering_user_search = State()

# =========================
# YORDAMCHI FUNKSIYALAR
# =========================
def normalize_amount(text: str) -> float:
    return float(text.replace(" ", "").replace(",", ""))


def month_key() -> str:
    return datetime.now().strftime("%Y-%m")


def today_key() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def is_admin(user_id: int) -> bool:
    return user_id == ADMIN_ID


async def build_category_keyboard(user_id: int, tx_type: str) -> ReplyKeyboardMarkup:
    user_categories = await get_categories(user_id, tx_type)
    keyboard = [[KeyboardButton(text=cat)] for cat in user_categories]
    keyboard.append([KeyboardButton(text="⬅️ Bekor qilish")])
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

# =========================
# BAZA
# =========================
async def init_db():
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                full_name TEXT,
                username TEXT,
                created_at TEXT
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                type TEXT NOT NULL,
                category TEXT NOT NULL,
                amount REAL NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS budgets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                year_month TEXT NOT NULL,
                limit_amount REAL NOT NULL,
                UNIQUE(user_id, year_month)
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS categories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                type TEXT NOT NULL,
                name TEXT NOT NULL,
                UNIQUE(user_id, type, name)
            )
            """
        )
        await db.commit()


async def register_user(message: Message):
    full_name = f"{message.from_user.first_name or ''} {message.from_user.last_name or ''}".strip()
    username = message.from_user.username or ""

    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            """
            INSERT OR IGNORE INTO users (user_id, full_name, username, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (message.from_user.id, full_name, username, datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        )
        await db.commit()

    await ensure_default_categories(message.from_user.id)


async def ensure_default_categories(user_id: int):
    async with aiosqlite.connect(DB_NAME) as db:
        for name in DEFAULT_INCOME_CATEGORIES:
            await db.execute(
                "INSERT OR IGNORE INTO categories (user_id, type, name) VALUES (?, ?, ?)",
                (user_id, "income", name),
            )
        for name in DEFAULT_EXPENSE_CATEGORIES:
            await db.execute(
                "INSERT OR IGNORE INTO categories (user_id, type, name) VALUES (?, ?, ?)",
                (user_id, "expense", name),
            )
        await db.commit()


async def get_categories(user_id: int, tx_type: str):
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "SELECT name FROM categories WHERE user_id = ? AND type = ? ORDER BY name",
            (user_id, tx_type),
        )
        rows = await cursor.fetchall()
    return [row[0] for row in rows]


async def add_category(user_id: int, tx_type: str, name: str):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "INSERT OR IGNORE INTO categories (user_id, type, name) VALUES (?, ?, ?)",
            (user_id, tx_type, name),
        )
        await db.commit()


async def add_transaction(user_id: int, tx_type: str, category: str, amount: float):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            """
            INSERT INTO transactions (user_id, type, category, amount, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (user_id, tx_type, category, amount, datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        )
        await db.commit()


async def get_monthly_summary(user_id: int, year_month: str):
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            """
            SELECT type, COALESCE(SUM(amount), 0)
            FROM transactions
            WHERE user_id = ? AND substr(created_at, 1, 7) = ?
            GROUP BY type
            """,
            (user_id, year_month),
        )
        rows = await cursor.fetchall()

    income = 0
    expense = 0
    for tx_type, total in rows:
        if tx_type == "income":
            income = total
        elif tx_type == "expense":
            expense = total
    return income, expense, income - expense


async def get_transactions_by_date(user_id: int, date_str: str):
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            """
            SELECT type, category, amount, created_at
            FROM transactions
            WHERE user_id = ? AND substr(created_at, 1, 10) = ?
            ORDER BY created_at DESC
            """,
            (user_id, date_str),
        )
        rows = await cursor.fetchall()
    return rows


async def get_all_transactions(user_id: int):
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            """
            SELECT id, type, category, amount, created_at
            FROM transactions
            WHERE user_id = ?
            ORDER BY created_at DESC
            """,
            (user_id,),
        )
        rows = await cursor.fetchall()
    return rows


async def get_monthly_category_data(user_id: int, year_month: str):
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            """
            SELECT category, COALESCE(SUM(amount), 0)
            FROM transactions
            WHERE user_id = ?
              AND type = 'expense'
              AND substr(created_at, 1, 7) = ?
            GROUP BY category
            ORDER BY SUM(amount) DESC
            """,
            (user_id, year_month),
        )
        rows = await cursor.fetchall()
    return rows


async def set_budget_limit(user_id: int, year_month: str, limit_amount: float):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            """
            INSERT INTO budgets (user_id, year_month, limit_amount)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id, year_month)
            DO UPDATE SET limit_amount = excluded.limit_amount
            """,
            (user_id, year_month, limit_amount),
        )
        await db.commit()


async def get_budget_limit(user_id: int, year_month: str):
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "SELECT limit_amount FROM budgets WHERE user_id = ? AND year_month = ?",
            (user_id, year_month),
        )
        row = await cursor.fetchone()
    return row[0] if row else None


async def get_profile_info(user_id: int):
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "SELECT full_name, username, created_at FROM users WHERE user_id = ?",
            (user_id,),
        )
        user = await cursor.fetchone()

        cursor = await db.execute(
            "SELECT COUNT(*) FROM transactions WHERE user_id = ?",
            (user_id,),
        )
        tx_count = (await cursor.fetchone())[0]

    return user, tx_count


async def get_total_users_count():
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute("SELECT COUNT(*) FROM users")
        total = (await cursor.fetchone())[0]
    return total


async def get_today_new_users_count():
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "SELECT COUNT(*) FROM users WHERE substr(created_at, 1, 10) = ?",
            (today_key(),),
        )
        total = (await cursor.fetchone())[0]
    return total


async def get_active_users_count(year_month: str):
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "SELECT COUNT(DISTINCT user_id) FROM transactions WHERE substr(created_at, 1, 7) = ?",
            (year_month,),
        )
        total = (await cursor.fetchone())[0]
    return total


async def get_global_stats(year_month: str):
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute("SELECT COUNT(*) FROM transactions")
        tx_count = (await cursor.fetchone())[0]

        cursor = await db.execute(
            "SELECT COALESCE(SUM(amount), 0) FROM transactions WHERE type = 'income' AND substr(created_at, 1, 7) = ?",
            (year_month,),
        )
        income = (await cursor.fetchone())[0]

        cursor = await db.execute(
            "SELECT COALESCE(SUM(amount), 0) FROM transactions WHERE type = 'expense' AND substr(created_at, 1, 7) = ?",
            (year_month,),
        )
        expense = (await cursor.fetchone())[0]

    return tx_count, income, expense, income - expense


async def get_top_users(limit: int = 10):
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            """
            SELECT u.user_id, u.full_name, u.username, COUNT(t.id) AS tx_count, COALESCE(SUM(t.amount), 0) AS total_amount
            FROM users u
            LEFT JOIN transactions t ON u.user_id = t.user_id
            GROUP BY u.user_id, u.full_name, u.username
            ORDER BY tx_count DESC, total_amount DESC
            LIMIT ?
            """,
            (limit,),
        )
        rows = await cursor.fetchall()
    return rows


async def search_users(keyword: str):
    async with aiosqlite.connect(DB_NAME) as db:
        if keyword.isdigit():
            cursor = await db.execute(
                "SELECT user_id, full_name, username, created_at FROM users WHERE CAST(user_id AS TEXT) LIKE ? LIMIT 10",
                (f"%{keyword}%",),
            )
        else:
            kw = f"%{keyword.lower()}%"
            cursor = await db.execute(
                """
                SELECT user_id, full_name, username, created_at
                FROM users
                WHERE lower(full_name) LIKE ? OR lower(username) LIKE ?
                ORDER BY created_at DESC
                LIMIT 10
                """,
                (kw, kw),
            )
        rows = await cursor.fetchall()
    return rows


async def check_budget_warning(user_id: int):
    ym = month_key()
    limit_amount = await get_budget_limit(user_id, ym)
    if limit_amount is None:
        return None

    _, expense, _ = await get_monthly_summary(user_id, ym)
    if expense > limit_amount:
        over = expense - limit_amount
        return (
            f"⚠️ Diqqat! Siz bu oy budjet limitidan oshdingiz.\n\n"
            f"💸 Limit: {limit_amount:,.0f} so'm\n"
            f"📉 Xarajat: {expense:,.0f} so'm\n"
            f"❗ Oshgan qism: {over:,.0f} so'm"
        )
    return None

# =========================
# EXCEL
# =========================
def create_excel_file(user_id: int, rows: list):
    if not rows:
        return None

    df = pd.DataFrame(rows, columns=["ID", "Turi", "Kategoriya", "Summa", "Sana"])
    df["Oy"] = df["Sana"].astype(str).str.slice(0, 7)
    df["Kun"] = df["Sana"].astype(str).str.slice(0, 10)

    file_path = os.path.join(EXPORT_FOLDER, f"finance_v4_{user_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")

    with pd.ExcelWriter(file_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Transactions", index=False)

        summary = pd.DataFrame(
            {
                "Ko'rsatkich": ["Jami daromad", "Jami xarajat", "Qoldiq"],
                "Qiymat": [
                    df[df["Turi"] == "income"]["Summa"].sum(),
                    df[df["Turi"] == "expense"]["Summa"].sum(),
                    df[df["Turi"] == "income"]["Summa"].sum() - df[df["Turi"] == "expense"]["Summa"].sum(),
                ],
            }
        )
        summary.to_excel(writer, sheet_name="Summary", index=False)

        expense_only = df[df["Turi"] == "expense"]
        if not expense_only.empty:
            category_summary = (
                expense_only.groupby("Kategoriya", as_index=False)["Summa"]
                .sum()
                .sort_values("Summa", ascending=False)
            )
            category_summary.to_excel(writer, sheet_name="ExpenseByCategory", index=False)

        monthly_summary = df.groupby(["Oy", "Turi"], as_index=False)["Summa"].sum()
        monthly_summary.to_excel(writer, sheet_name="MonthlyData", index=False)

        daily_summary = df.groupby(["Kun", "Turi"], as_index=False)["Summa"].sum()
        daily_summary.to_excel(writer, sheet_name="DailyData", index=False)

    return file_path


async def create_all_data_excel():
    async with aiosqlite.connect(DB_NAME) as db:
        users_cursor = await db.execute("SELECT user_id, full_name, username, created_at FROM users ORDER BY created_at DESC")
        users_rows = await users_cursor.fetchall()

        tx_cursor = await db.execute("SELECT id, user_id, type, category, amount, created_at FROM transactions ORDER BY created_at DESC")
        tx_rows = await tx_cursor.fetchall()

        budgets_cursor = await db.execute("SELECT user_id, year_month, limit_amount FROM budgets ORDER BY year_month DESC")
        budgets_rows = await budgets_cursor.fetchall()

    file_path = os.path.join(EXPORT_FOLDER, f"admin_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
    with pd.ExcelWriter(file_path, engine="openpyxl") as writer:
        pd.DataFrame(users_rows, columns=["user_id", "full_name", "username", "created_at"]).to_excel(writer, sheet_name="Users", index=False)
        pd.DataFrame(tx_rows, columns=["id", "user_id", "type", "category", "amount", "created_at"]).to_excel(writer, sheet_name="Transactions", index=False)
        pd.DataFrame(budgets_rows, columns=["user_id", "year_month", "limit_amount"]).to_excel(writer, sheet_name="Budgets", index=False)
    return file_path

# =========================
# DIAGRAMMALAR
# =========================
def create_chart(user_id: int, income: float, expense: float, category_rows: list):
    paths = []

    file_path = os.path.join(CHART_FOLDER, f"chart_{user_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
    plt.figure(figsize=(8, 5))
    plt.bar(["Daromad", "Xarajat"], [income, expense])
    plt.title("Oylik daromad va xarajat")
    plt.ylabel("So'm")
    plt.tight_layout()
    plt.savefig(file_path)
    plt.close()
    paths.append(file_path)

    if category_rows:
        file_path_2 = os.path.join(CHART_FOLDER, f"categories_{user_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
        plt.figure(figsize=(8, 5))
        categories = [x[0] for x in category_rows]
        amounts = [x[1] for x in category_rows]
        plt.pie(amounts, labels=categories, autopct="%1.1f%%")
        plt.title("Kategoriya bo'yicha xarajatlar")
        plt.tight_layout()
        plt.savefig(file_path_2)
        plt.close()
        paths.append(file_path_2)

    return paths

# =========================
# OY OXIRI HISOBOTI
# =========================
async def send_monthly_report_to_user(user_id: int):
    ym = month_key()
    income, expense, balance = await get_monthly_summary(user_id, ym)
    category_rows = await get_monthly_category_data(user_id, ym)
    all_rows = await get_all_transactions(user_id)
    budget_limit = await get_budget_limit(user_id, ym)

    text = (
        f"📦 Oy yakuni bo'yicha hisobot\n\n"
        f"📅 Oy: {ym}\n"
        f"💰 Jami daromad: {income:,.0f} so'm\n"
        f"💸 Jami xarajat: {expense:,.0f} so'm\n"
        f"📌 Qoldiq: {balance:,.0f} so'm"
    )

    if budget_limit is not None:
        text += f"\n💰 Budjet limiti: {budget_limit:,.0f} so'm"

    await bot.send_message(user_id, text)

    warning_text = await check_budget_warning(user_id)
    if warning_text:
        await bot.send_message(user_id, warning_text)

    excel_path = create_excel_file(user_id, all_rows)
    if excel_path:
        await bot.send_document(user_id, FSInputFile(excel_path), caption="📁 Excel hisobot")

    chart_paths = create_chart(user_id, income, expense, category_rows)
    for chart_path in chart_paths:
        await bot.send_photo(user_id, FSInputFile(chart_path))


async def monthly_scheduler():
    sent_month = None
    while True:
        now = datetime.now()
        tomorrow = now + timedelta(days=1)
        is_last_day = tomorrow.month != now.month
        current_marker = now.strftime("%Y-%m")

        if is_last_day and now.hour == 21 and sent_month != current_marker:
            async with aiosqlite.connect(DB_NAME) as db:
                cursor = await db.execute("SELECT DISTINCT user_id FROM users")
                users = await cursor.fetchall()

            for row in users:
                user_id = row[0]
                try:
                    await send_monthly_report_to_user(user_id)
                except Exception as e:
                    logging.error(f"User {user_id} ga hisobot yuborishda xato: {e}")

            sent_month = current_marker

        if now.hour != 21:
            sent_month = None

        await asyncio.sleep(60)

# =========================
# HANDLERLAR
# =========================
@dp.message(CommandStart())
async def start_handler(message: Message):
    await register_user(message)
    text = (
        "Assalomu alaykum!\n\n"
        "Men sizning moliyaviy botingizman.\n"
        "Daromad, xarajat, budjet va hisobotlarni yuritishda yordam beraman."
    )
    if is_admin(message.from_user.id):
        text += "\n\n🔐 Siz admin sifatida ham kirdingiz. /admin ni bossangiz admin menyu ochiladi."
    await message.answer(text, reply_markup=main_menu)


@dp.message(Command("admin"))
async def admin_panel_handler(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("⛔ Siz admin emassiz.")
        return
    await message.answer("🔐 Admin panel ochildi.", reply_markup=admin_menu)


@dp.message(F.text == "⬅️ Oddiy menyu")
async def back_to_main_menu(message: Message):
    await message.answer("Oddiy menyuga qaytdingiz.", reply_markup=main_menu)


@dp.message(Command("help"))
@dp.message(F.text == "ℹ️ Yordam")
async def help_handler(message: Message):
    await message.answer(
        "Men quyidagilarni qila olaman:\n\n"
        "1) Daromad qo‘shish\n"
        "2) Xarajat qo‘shish\n"
        "3) Sana bo‘yicha ko‘rish\n"
        "4) Oylik hisobot\n"
        "5) Excel export\n"
        "6) Diagramma chiqarish\n"
        "7) Oy oxirida avtomatik hisobot yuborish\n"
        "8) Profilni ko‘rsatish\n"
        "9) Budjet limitini saqlash\n"
        "10) O‘z kategoriyangizni qo‘shish\n"
        "11) Admin panel (/admin)"
    )


@dp.message(F.text == "👥 Foydalanuvchilar soni")
async def admin_users_count_handler(message: Message):
    if not is_admin(message.from_user.id):
        return
    total = await get_total_users_count()
    today_new = await get_today_new_users_count()
    active = await get_active_users_count(month_key())
    await message.answer(
        f"👥 Foydalanuvchilar statistikasi\n\n"
        f"Jami foydalanuvchilar: {total}\n"
        f"Bugun yangi qo‘shilganlar: {today_new}\n"
        f"Joriy oy aktiv userlar: {active}"
    )


@dp.message(F.text == "📊 Umumiy statistika")
async def admin_global_stats_handler(message: Message):
    if not is_admin(message.from_user.id):
        return
    ym = month_key()
    total_users = await get_total_users_count()
    active_users = await get_active_users_count(ym)
    tx_count, income, expense, balance = await get_global_stats(ym)
    await message.answer(
        f"📊 Umumiy statistika\n\n"
        f"Oy: {ym}\n"
        f"Jami userlar: {total_users}\n"
        f"Aktiv userlar: {active_users}\n"
        f"Jami tranzaksiyalar: {tx_count}\n"
        f"Joriy oy daromad: {income:,.0f} so'm\n"
        f"Joriy oy xarajat: {expense:,.0f} so'm\n"
        f"Joriy oy balans: {balance:,.0f} so'm"
    )


@dp.message(F.text == "🏆 Top foydalanuvchilar")
async def admin_top_users_handler(message: Message):
    if not is_admin(message.from_user.id):
        return
    rows = await get_top_users(10)
    if not rows:
        await message.answer("Hali foydalanuvchilar yo‘q.")
        return

    text = "🏆 Top foydalanuvchilar\n\n"
    for i, row in enumerate(rows, start=1):
        user_id, full_name, username, tx_count, total_amount = row
        uname = f"@{username}" if username else "yo‘q"
        text += (
            f"{i}) {full_name or 'Noma’lum'} | {uname}\n"
            f"ID: {user_id}\n"
            f"Tranzaksiya: {tx_count}\n"
            f"Jami summa: {total_amount:,.0f} so'm\n\n"
        )
    await message.answer(text)


@dp.message(F.text == "🔎 User qidirish")
async def admin_search_start(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    await state.set_state(FinanceState.entering_user_search)
    await message.answer("Foydalanuvchi ism, username yoki ID sini kiriting:", reply_markup=cancel_menu)


@dp.message(FinanceState.entering_user_search)
async def admin_search_result(message: Message, state: FSMContext):
    if message.text == "⬅️ Bekor qilish":
        await state.clear()
        await message.answer("Bekor qilindi.", reply_markup=admin_menu)
        return

    rows = await search_users(message.text.strip())
    if not rows:
        await message.answer("Hech narsa topilmadi.", reply_markup=admin_menu)
        await state.clear()
        return

    text = "🔎 Qidiruv natijalari\n\n"
    for row in rows:
        user_id, full_name, username, created_at = row
        uname = f"@{username}" if username else "yo‘q"
        text += (
            f"ID: {user_id}\n"
            f"Ism: {full_name}\n"
            f"Username: {uname}\n"
            f"Ro‘yxatdan o‘tgan: {created_at}\n\n"
        )
    await message.answer(text, reply_markup=admin_menu)
    await state.clear()


@dp.message(F.text == "📁 Barcha ma’lumotlarni export")
async def admin_export_all_handler(message: Message):
    if not is_admin(message.from_user.id):
        return
    file_path = await create_all_data_excel()
    await message.answer_document(FSInputFile(file_path), caption="✅ Admin eksport tayyor.")


@dp.message(F.text == "👤 Profil")
async def profile_handler(message: Message):
    await register_user(message)
    user, tx_count = await get_profile_info(message.from_user.id)
    ym = month_key()
    budget_limit = await get_budget_limit(message.from_user.id, ym)
    income, expense, balance = await get_monthly_summary(message.from_user.id, ym)

    full_name = user[0] if user else "-"
    username = f"@{user[1]}" if user and user[1] else "yo'q"
    created_at = user[2] if user else "-"
    budget_text = f"{budget_limit:,.0f} so'm" if budget_limit is not None else "o‘rnatilmagan"

    await message.answer(
        f"👤 Profil\n\n"
        f"Ism: {full_name}\n"
        f"Username: {username}\n"
        f"Ro‘yxatdan o‘tgan sana: {created_at}\n"
        f"Jami tranzaksiya soni: {tx_count}\n\n"
        f"📅 Joriy oy: {ym}\n"
        f"💰 Daromad: {income:,.0f} so'm\n"
        f"💸 Xarajat: {expense:,.0f} so'm\n"
        f"📌 Qoldiq: {balance:,.0f} so'm\n"
        f"🎯 Budjet limiti: {budget_text}"
    )


@dp.message(F.text == "💰 Budjet limiti")
async def budget_start(message: Message, state: FSMContext):
    await state.set_state(FinanceState.entering_budget_limit)
    await message.answer(
        "Joriy oy uchun budjet limitini kiriting.\nMasalan: 3000000",
        reply_markup=cancel_menu,
    )


@dp.message(FinanceState.entering_budget_limit)
async def budget_save(message: Message, state: FSMContext):
    if message.text == "⬅️ Bekor qilish":
        await state.clear()
        await message.answer("Bekor qilindi.", reply_markup=main_menu)
        return

    try:
        amount = normalize_amount(message.text)
        await set_budget_limit(message.from_user.id, month_key(), amount)
        await message.answer(
            f"✅ Joriy oy uchun budjet limiti saqlandi: {amount:,.0f} so'm",
            reply_markup=main_menu,
        )
        await state.clear()
    except ValueError:
        await message.answer("❌ Iltimos, limitni son ko‘rinishida kiriting.")


@dp.message(F.text == "🗂 Kategoriya qo‘shish")
async def custom_category_start(message: Message, state: FSMContext):
    await state.set_state(FinanceState.choosing_custom_category_type)
    await message.answer("Qaysi turga kategoriya qo‘shmoqchisiz?", reply_markup=category_type_menu)


@dp.message(FinanceState.choosing_custom_category_type)
async def custom_category_type_chosen(message: Message, state: FSMContext):
    if message.text == "⬅️ Bekor qilish":
        await state.clear()
        await message.answer("Bekor qilindi.", reply_markup=main_menu)
        return

    if message.text == "📈 Daromad kategoriyasi":
        tx_type = "income"
    elif message.text == "📉 Xarajat kategoriyasi":
        tx_type = "expense"
    else:
        await message.answer("❌ Iltimos, tugmalardan birini tanlang.")
        return

    await state.update_data(tx_type=tx_type)
    await state.set_state(FinanceState.entering_custom_category_name)
    await message.answer("Yangi kategoriya nomini yozing:", reply_markup=cancel_menu)


@dp.message(FinanceState.entering_custom_category_name)
async def custom_category_name_entered(message: Message, state: FSMContext):
    if message.text == "⬅️ Bekor qilish":
        await state.clear()
        await message.answer("Bekor qilindi.", reply_markup=main_menu)
        return

    data = await state.get_data()
    tx_type = data["tx_type"]
    name = message.text.strip()

    if len(name) < 2:
        await message.answer("❌ Kategoriya nomi juda qisqa.")
        return

    await add_category(message.from_user.id, tx_type, name)
    type_name = "daromad" if tx_type == "income" else "xarajat"
    await message.answer(
        f"✅ Yangi {type_name} kategoriyasi qo‘shildi: {name}",
        reply_markup=main_menu,
    )
    await state.clear()


@dp.message(F.text == "🧾 Kategoriyalarim")
async def my_categories_handler(message: Message):
    incomes = await get_categories(message.from_user.id, "income")
    expenses = await get_categories(message.from_user.id, "expense")

    income_text = "\n".join([f"• {x}" for x in incomes]) if incomes else "Yo‘q"
    expense_text = "\n".join([f"• {x}" for x in expenses]) if expenses else "Yo‘q"

    await message.answer(
        f"🧾 Sizning kategoriyalaringiz\n\n"
        f"📈 Daromad kategoriyalari:\n{income_text}\n\n"
        f"📉 Xarajat kategoriyalari:\n{expense_text}"
    )


@dp.message(F.text == "➕ Daromad qo‘shish")
async def income_category_start(message: Message, state: FSMContext):
    await register_user(message)
    keyboard = await build_category_keyboard(message.from_user.id, "income")
    await state.set_state(FinanceState.choosing_income_category)
    await message.answer("Daromad kategoriyasini tanlang:", reply_markup=keyboard)


@dp.message(FinanceState.choosing_income_category)
async def income_category_chosen(message: Message, state: FSMContext):
    if message.text == "⬅️ Bekor qilish":
        await state.clear()
        await message.answer("Bekor qilindi.", reply_markup=main_menu)
        return

    await state.update_data(category=message.text)
    await state.set_state(FinanceState.entering_income_amount)
    await message.answer("Endi summani kiriting: masalan 250000", reply_markup=ReplyKeyboardRemove())


@dp.message(FinanceState.entering_income_amount)
async def income_amount_entered(message: Message, state: FSMContext):
    try:
        amount = normalize_amount(message.text)
        data = await state.get_data()
        category = data["category"]
        await add_transaction(message.from_user.id, "income", category, amount)
        await message.answer(
            f"✅ Daromad saqlandi\nKategoriya: {category}\nSumma: {amount:,.0f} so'm",
            reply_markup=main_menu,
        )
        await state.clear()
    except ValueError:
        await message.answer("❌ Iltimos, summani faqat son ko‘rinishida kiriting.")


@dp.message(F.text == "➖ Xarajat qo‘shish")
async def expense_category_start(message: Message, state: FSMContext):
    await register_user(message)
    keyboard = await build_category_keyboard(message.from_user.id, "expense")
    await state.set_state(FinanceState.choosing_expense_category)
    await message.answer("Xarajat kategoriyasini tanlang:", reply_markup=keyboard)


@dp.message(FinanceState.choosing_expense_category)
async def expense_category_chosen(message: Message, state: FSMContext):
    if message.text == "⬅️ Bekor qilish":
        await state.clear()
        await message.answer("Bekor qilindi.", reply_markup=main_menu)
        return

    await state.update_data(category=message.text)
    await state.set_state(FinanceState.entering_expense_amount)
    await message.answer("Endi summani kiriting: masalan 80000", reply_markup=ReplyKeyboardRemove())


@dp.message(FinanceState.entering_expense_amount)
async def expense_amount_entered(message: Message, state: FSMContext):
    try:
        amount = normalize_amount(message.text)
        data = await state.get_data()
        category = data["category"]
        await add_transaction(message.from_user.id, "expense", category, amount)
        await message.answer(
            f"✅ Xarajat saqlandi\nKategoriya: {category}\nSumma: {amount:,.0f} so'm",
            reply_markup=main_menu,
        )
        await state.clear()

        warning_text = await check_budget_warning(message.from_user.id)
        if warning_text:
            await message.answer(warning_text)
    except ValueError:
        await message.answer("❌ Iltimos, summani faqat son ko‘rinishida kiriting.")


@dp.message(F.text == "📅 Sana bo‘yicha ko‘rish")
async def date_view_start(message: Message, state: FSMContext):
    await state.set_state(FinanceState.entering_date_for_view)
    await message.answer("Sanani kiriting: YYYY-MM-DD\nMasalan: 2026-04-21", reply_markup=cancel_menu)


@dp.message(FinanceState.entering_date_for_view)
async def date_view_result(message: Message, state: FSMContext):
    if message.text == "⬅️ Bekor qilish":
        await state.clear()
        await message.answer("Bekor qilindi.", reply_markup=main_menu)
        return

    date_str = message.text.strip()
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        await message.answer("❌ Sana noto‘g‘ri. To‘g‘ri format: YYYY-MM-DD")
        return

    rows = await get_transactions_by_date(message.from_user.id, date_str)
    if not rows:
        await message.answer("Bu sana bo‘yicha ma’lumot topilmadi.", reply_markup=main_menu)
        await state.clear()
        return

    text = f"📅 {date_str} bo‘yicha yozuvlar:\n\n"
    for tx_type, category, amount, created_at in rows:
        icon = "💰" if tx_type == "income" else "💸"
        text += f"{icon} {category} — {amount:,.0f} so'm ({created_at[11:16]})\n"

    await message.answer(text, reply_markup=main_menu)
    await state.clear()


@dp.message(F.text == "📊 Oylik hisobot")
async def monthly_report_handler(message: Message):
    ym = month_key()
    income, expense, balance = await get_monthly_summary(message.from_user.id, ym)
    budget_limit = await get_budget_limit(message.from_user.id, ym)

    text = (
        f"📊 Oylik hisobot\n\n"
        f"📅 Oy: {ym}\n"
        f"💰 Jami daromad: {income:,.0f} so'm\n"
        f"💸 Jami xarajat: {expense:,.0f} so'm\n"
        f"📌 Qoldiq: {balance:,.0f} so'm"
    )

    if budget_limit is not None:
        text += f"\n🎯 Budjet limiti: {budget_limit:,.0f} so'm"

    await message.answer(text)

    warning_text = await check_budget_warning(message.from_user.id)
    if warning_text:
        await message.answer(warning_text)


@dp.message(F.text == "📁 Excel export")
async def excel_export_handler(message: Message):
    rows = await get_all_transactions(message.from_user.id)
    file_path = create_excel_file(message.from_user.id, rows)

    if not file_path:
        await message.answer("Export qilish uchun hali ma’lumot yo‘q.")
        return

    await message.answer_document(
        FSInputFile(file_path),
        caption="✅ Sizning Excel hisobotingiz tayyor. Bu fayl Power BI uchun ham mos.",
    )


@dp.message(F.text == "📈 Diagramma")
async def chart_handler(message: Message):
    ym = month_key()
    income, expense, _ = await get_monthly_summary(message.from_user.id, ym)
    category_rows = await get_monthly_category_data(message.from_user.id, ym)

    if income == 0 and expense == 0:
        await message.answer("Diagramma chizish uchun hali ma’lumot yo‘q.")
        return

    chart_paths = create_chart(message.from_user.id, income, expense, category_rows)
    for path in chart_paths:
        await message.answer_photo(FSInputFile(path))


@dp.message(Command("powerbi"))
async def powerbi_info_handler(message: Message):
    await message.answer(
        "Power BI ulash uchun hozirgi Excel struktura tayyor.\n\n"
        "Power BI ga eng qulay sheetlar:\n"
        "1) Transactions\n"
        "2) Summary\n"
        "3) ExpenseByCategory\n"
        "4) MonthlyData\n"
        "5) DailyData"
    )

# =========================
# MAIN
# =========================
async def main():
    await init_db()
    scheduler_task = asyncio.create_task(monthly_scheduler())
    polling_task = asyncio.create_task(dp.start_polling(bot))
    await asyncio.gather(scheduler_task, polling_task)


if __name__ == "__main__":
    asyncio.run(main())
