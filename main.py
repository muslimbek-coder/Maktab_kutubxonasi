import asyncio, logging, os, secrets, time, hashlib
from types import SimpleNamespace
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from html import escape

from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart, Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from dotenv import load_dotenv
from fastapi import FastAPI, Depends, Header, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

load_dotenv()
TOKEN = os.getenv("BOT_TOKEN")
HOLD = 48 * 3600  # band qilish muddati: 2 kun
TZ = timezone(timedelta(hours=5))  # Toshkent vaqti
log = logging.getLogger(__name__)

# ---------- Ma'lumotlar bazasi (PostgreSQL: Neon / Supabase) ----------
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise SystemExit("DATABASE_URL topilmadi. Neon/Supabase ulanish manzilini Environment Variables ga qo'shing.")

pool = ConnectionPool(
    DATABASE_URL, min_size=1, max_size=5, open=True,
    kwargs={"row_factory": dict_row, "autocommit": True, "prepare_threshold": None},
    check=ConnectionPool.check_connection,  # uzilgan ulanishlarni avtomatik yangilaydi
)

SCHEMA = """
create table if not exists schools(id serial primary key, vil text, tum text, name text, unique(vil,tum,name));
create table if not exists librarians(id serial primary key, school_id int, name text, fam text, login text unique, pw text, token text, telegram_tg_id bigint);
create table if not exists books(id serial primary key, school_id int, title text, author text, genre text not null default '', total int);
create table if not exists students(tg_id bigint primary key, school_id int, name text, pending int, want int, role text, state text, requested_days int, cls text);
create table if not exists res(id serial primary key, school_id int, book_id int, tg_id bigint, name text, created bigint, until bigint, st text, days int, due bigint, cls text);
create table if not exists pupils(id serial primary key, school_id int, name text, cls text, unique(school_id,name,cls));
create table if not exists parent_children(parent_tg bigint, student_tg bigint, primary key(parent_tg, student_tg));
create table if not exists parent_codes(code text primary key, student_tg bigint, school_id int, expires bigint);
create table if not exists bot_users(tg_id bigint primary key, language text);
create table if not exists library_codes(code text primary key, librarian_id int not null, purpose text not null, expires bigint not null);
create table if not exists librarian_book_drafts(tg_id bigint primary key, librarian_id int not null, school_id int not null, current_step text not null, title text, author text, genre text);
alter table librarians add column if not exists telegram_tg_id bigint;
alter table schools add column if not exists telegram_group_id bigint;
alter table schools add column if not exists telegram_group_title text;
alter table books add column if not exists genre text not null default '';
"""
with pool.connection() as _c:
    _c.execute(SCHEMA)

def _sql(s):
    return s.replace("?", "%s")  # sqlite uslubidagi ? ni postgres uslubiga o'tkazish

def q(sql, a=()):
    with pool.connection() as conn:
        return SimpleNamespace(rowcount=conn.execute(_sql(sql), a).rowcount)

def rows(sql, a=()):
    with pool.connection() as conn:
        return [dict(r) for r in conn.execute(_sql(sql), a).fetchall()]

def one(sql, a=()):
    with pool.connection() as conn:
        r = conn.execute(_sql(sql), a).fetchone()
        return dict(r) if r else None

def many(sql, items):
    items = list(items)
    if not items:
        return
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(_sql(sql), items)

def hashpw(p, salt):
    return hashlib.pbkdf2_hmac("sha256", p.encode(), salt.encode(), 100_000).hex()

def fmt(t):
    return datetime.fromtimestamp(t, TZ).strftime("%d.%m %H:%M")

def avail(b):
    n = one("select count(*) c from res where book_id=? and (st='olindi' or (st='band' and until>?))",
            (b["id"], int(time.time())))["c"]
    return b["total"] - n

BOT_TEXT = {
    "uz": {
        "choose_language": "Tilni tanlang / Выберите язык / Choose a language:",
        "region": "Viloyatingizni tanlang:", "district": "Tumaningizni tanlang:",
        "school": "Maktabingizni tanlang:", "no_schools": "Hozircha kutubxonalar ro'yxatdan o'tmagan.",
        "role": "Siz kim bo'lasiz?", "student": "O'quvchi", "parent": "Ota-ona",
        "librarian": "Kutubxonachi", "student_menu": "O'quvchi bo'limi:",
        "parent_menu": "Ota-ona bo'limi:", "books_button": "📚 Kitob tanlash",
        "parent_code_button": "🔗 Ota-onaga ulanish kodi", "parent_add": "➕ Farzand qo'shish",
        "parent_list": "👨‍👩‍👧 Farzandlarim va kitoblari", "librarian_menu": "📚 Kutubxonachi bo'limi",
        "add_book_button": "➕ Yangi kitob qo'shish", "book_missing": "Bu kutubxonada hozircha kitob yo'q.",
        "book_list": "Kitobni tanlang. Band qilish 2 kun saqlanadi:",
        "not_started": "Avval /start buyrug'ini bosing.", "no_libraries": "Kutubxonachi hisobiga kiring: /librarian KOD",
        "use_private": "Bu buyruqni bot bilan shaxsiy chatda yuboring.",
        "code": "Kod xato, eskirgan yoki avval ishlatilgan. Saytdan yangi kod yarating.",
        "linked": "✅ Kutubxonachi hisobi ulandi. Kitob qo'shish uchun /addbook buyrug'ini yuboring.",
        "already_linked": "Bu Telegram hisobi boshqa kutubxonachiga ulangan.",
        "need_link": "Avval sayt akkauntingizdan bir martalik kod olib, botga /librarian KOD yuboring.",
        "link_help": "Guruhga ulash uchun saytdagi “Telegram guruhini ulash” havolasini oching, botni guruhga qo'shib admin qiling (xabar yuborish huquqi bilan). So'ng havolani oching yoki guruhda /linklibrary KOD yuboring.",
        "group_only": "Bu buyruqni kutubxona guruhida yuboring.",
        "admin_only": "Guruhni ulash uchun guruh administratori bo'lishingiz kerak.",
        "group_linked": "✅ Guruh kutubxonaga ulandi. Yangi kitoblar shu guruhga e'lon qilinadi.",
        "group_code": "Guruh kodi xato yoki muddati o'tgan. Saytdan yangisini yarating.",
        "book_title": "Kitob nomini yuboring:",
        "book_author": "Muallifini yuboring (noma'lum bo'lsa, - belgisi yuboring):",
        "book_genre": "Janrini yuboring (noma'lum bo'lsa, - belgisi yuboring):",
        "book_total": "Nechta nusxa bor? 1 dan 1000 gacha son kiriting:",
        "book_count_error": "Nusxalar sonini 1 dan 1000 gacha butun son bilan kiriting.",
        "book_added": "✅ “{title}” kitobi qo'shildi. Ulangan guruhga e'lon yuborildi.",
        "book_added_no_group": "✅ “{title}” kitobi qo'shildi. Guruh e'loni uchun avval guruhni saytdan ulang.",
        "book_added_no_delivery": "✅ “{title}” kitobi qo'shildi, ammo guruhga e'lon yetmadi. Bot guruhda yozish huquqiga ega ekanini tekshiring.",
        "book_title_error": "Kitob nomi 1–200 belgi bo'lishi kerak. Qayta yuboring:",
        "book_author_error": "Muallif nomi 100 belgidan oshmasin. Qayta yuboring:",
        "book_genre_error": "Janr 60 belgidan oshmasin. Qayta yuboring:",
        "no_book_link": "Bu kutubxona guruhiga ulanmagan.",
        "reserve": "📲 Botda oldindan band qilish",
        "copies": "Nusxa", "available": "Mavjud", "author_label": "Muallif",
        "genre_label": "Janr", "school_label": "Kutubxona",
        "student_label": "O'quvchi",
        "reserve_days": "Kitobni necha kunga olmoqchisiz? 1 dan 60 gacha raqam yozing:",
        "identity": "Ism-familiyangiz va sinfingizni yozing (masalan: Aziza Karimova, 7-A):",
        "days_error": "Iltimos, 1 dan 60 gacha raqam yozing.",
        "profile_error": "Ism-familiya va sinfni vergul bilan yozing (masalan: Aziza Karimova, 7-A):",
        "bad_book": "Kitob topilmadi. /start ni bosing.",
        "too_many": "Bir vaqtda 2 tadan ortiq kitob band qila olmaysiz.",
        "unavailable": "Afsuski, bu kitob hozir qolmagan.",
        "reserved": "✅ “{title}” band qilindi.\n{until} gacha olib keting.\nOlgandan keyin {days} kun ichida qaytaring.",
        "reservation_list_empty": "Band qilingan kitobingiz yo'q.",
        "not_found": "Maktab yoki hudud topilmadi. /start ni bosing.",
        "parent_code_prompt": "Farzandingiz yuborgan 8 belgili kodni kiriting:",
        "parent_code_bad": "Kod noto'g'ri yoki muddati o'tgan. Farzandingizdan yangi kod oling.",
        "parent_code_self": "O'zingizni farzand sifatida ulay olmaysiz.",
        "parent_linked": "✅ Farzandingiz ulandi.",
        "child_name": "Farzandingiz ro'yxatda tanilishi uchun ism-familiyasi va sinfini yozing (masalan: Aziza Karimova, 7-A):",
        "parent_no_children": "Hali farzand ulanmagan. “Farzand qo'shish” tugmasini bosing.",
        "parent_choose_child": "Kitoblar tarixini ko'rish uchun farzandingizni tanlang:",
        "not_linked_child": "Bu o'quvchi sizga ulanmagan.",
        "no_child_history": "Bu farzandingizda hali kitob buyurtmalari yo'q.",
        "child_history": "📚 Farzandingizning kitoblar tarixi:",
        "newer": "⬅️ Yangiroq", "older": "Avvalgilari ➡️",
        "parent_code_issued": "🔗 Ota-onangizga shu kodni yuboring: <code>{code}</code>\nKod 15 daqiqa ichida bir marta ishlatiladi.",
        "no_reservations": "Hozircha band qilingan kitob yo'q.",
        "book_expired": "⌛ “{title}” bandining muddati tugadi. Kerak bo'lsa, qaytadan band qiling.",
        "added_to_group": "📚 Guruhga kutubxona boti qo'shildi. Kutubxonachi guruhni ulash uchun /linklibrary KOD buyrug'ini yuborsin.",
        "language_saved": "✅ Til saqlandi.",
        "draft_cancelled": "Kitob qo'shish bekor qilindi.",
        "reserved_status": "Band qilingan", "borrowed_status": "O'qiyapti",
        "returned_status": "Qaytarilgan", "cancelled_status": "Bekor qilingan",
        "expired_status": "Band muddati tugagan",
        "took_notice": "📗 “{title}” kitobi olingani belgilandi.\nQaytarish muddati: {due}",
        "returned_notice": "🙏 “{title}” kitobi qaytarildi. Rahmat!",
        "cancelled_notice": "❌ “{title}” kitobining bandi bekor qilindi.",
        "expired_notice": "⌛ “{title}” bandining muddati tugadi. Kerak bo'lsa, qaytadan band qiling.",
        "child_prefix": "Farzandingiz: ",
    },
    "ru": {
        "choose_language": "Выберите язык / Tilni tanlang / Choose a language:",
        "region": "Выберите область:", "district": "Выберите район:", "school": "Выберите школу:",
        "no_schools": "Библиотеки пока не зарегистрированы.", "role": "Кто вы?",
        "student": "Ученик", "parent": "Родитель", "librarian": "Библиотекарь",
        "student_menu": "Раздел ученика:", "parent_menu": "Раздел родителя:",
        "books_button": "📚 Выбрать книгу", "parent_code_button": "🔗 Код для родителя",
        "parent_add": "➕ Добавить ребёнка", "parent_list": "👨‍👩‍👧 Мои дети и книги",
        "librarian_menu": "📚 Раздел библиотекаря", "add_book_button": "➕ Добавить книгу",
        "book_missing": "В этой библиотеке пока нет книг.", "book_list": "Выберите книгу. Бронь действует 2 дня:",
        "not_started": "Сначала отправьте /start.", "no_libraries": "Войдите как библиотекарь: /librarian КОД",
        "use_private": "Отправьте эту команду в личном чате с ботом.",
        "code": "Код неверен, просрочен или уже использован. Создайте новый на сайте.",
        "linked": "✅ Аккаунт библиотекаря подключён. Чтобы добавить книгу, отправьте /addbook.",
        "already_linked": "Этот Telegram-аккаунт уже подключён к другому библиотекарю.",
        "need_link": "Сначала создайте одноразовый код в своём аккаунте на сайте и отправьте /librarian КОД.",
        "link_help": "Откройте ссылку «Подключить Telegram-группу» на сайте, добавьте бота в группу администратором с правом отправки сообщений, затем откройте ссылку. Или отправьте в группе /linklibrary КОД.",
        "group_only": "Отправьте эту команду в библиотечной группе.",
        "admin_only": "Для подключения группы нужно быть её администратором.",
        "group_linked": "✅ Группа подключена к библиотеке. Новые книги будут опубликованы здесь.",
        "group_code": "Код группы неверен или просрочен. Создайте новый на сайте.",
        "book_title": "Отправьте название книги:",
        "book_author": "Отправьте автора (если неизвестен, отправьте дефис -):",
        "book_genre": "Отправьте жанр (если неизвестен, отправьте дефис -):",
        "book_total": "Сколько экземпляров? Введите число от 1 до 1000:",
        "book_count_error": "Введите целое число экземпляров от 1 до 1000.",
        "book_added": "✅ Книга «{title}» добавлена. Объявление отправлено в подключённую группу.",
        "book_added_no_group": "✅ Книга «{title}» добавлена. Сначала подключите группу на сайте, чтобы публиковать объявления.",
        "book_added_no_delivery": "✅ Книга «{title}» добавлена, но объявление не доставлено. Проверьте право бота писать в группу.",
        "book_title_error": "Название должно содержать от 1 до 200 символов. Повторите:",
        "book_author_error": "Имя автора должно быть не длиннее 100 символов. Повторите:",
        "book_genre_error": "Жанр должен быть не длиннее 60 символов. Повторите:",
        "no_book_link": "К этой библиотеке не подключена группа.",
        "reserve": "📲 Забронировать через бота", "copies": "Экземпляры",
        "available": "В наличии", "author_label": "Автор", "genre_label": "Жанр",
        "school_label": "Библиотека", "reserve_days": "На сколько дней взять книгу? Введите число от 1 до 60:",
        "student_label": "Ученик",
        "identity": "Введите имя, фамилию и класс (например: Азиза Каримова, 7-А):",
        "days_error": "Введите число от 1 до 60.", "profile_error": "Введите имя и класс через запятую (например: Азиза Каримова, 7-А):",
        "bad_book": "Книга не найдена. Отправьте /start.", "too_many": "Нельзя одновременно бронировать больше двух книг.",
        "unavailable": "К сожалению, этой книги сейчас нет.", "reserved": "✅ Книга «{title}» забронирована.\nЗаберите до {until}.\nПосле получения верните в течение {days} дн.",
        "reservation_list_empty": "У вас нет забронированных книг.", "not_found": "Школа или регион не найдены. Отправьте /start.",
        "parent_code_prompt": "Введите 8-значный код от ребёнка:", "parent_code_bad": "Код неверен или просрочен. Попросите ребёнка создать новый.",
        "parent_code_self": "Нельзя подключить себя как ребёнка.", "parent_linked": "✅ Ребёнок подключён.",
        "child_name": "Введите имя, фамилию и класс ребёнка (например: Азиза Каримова, 7-А):",
        "parent_no_children": "Дети ещё не подключены. Нажмите «Добавить ребёнка».",
        "parent_choose_child": "Выберите ребёнка, чтобы посмотреть историю книг:",
        "not_linked_child": "Этот ученик не подключён к вам.", "no_child_history": "У ребёнка пока нет истории книг.",
        "child_history": "📚 История книг ребёнка:", "newer": "⬅️ Новее", "older": "Старее ➡️",
        "parent_code_issued": "🔗 Отправьте родителю этот код: <code>{code}</code>\nОн действует 15 минут и используется один раз.",
        "no_reservations": "Нет активных броней.", "book_expired": "⌛ Срок брони книги «{title}» истёк. При необходимости забронируйте снова.",
        "added_to_group": "📚 В группу добавлен библиотечный бот. Библиотекарь может подключить её командой /linklibrary КОД.",
        "language_saved": "✅ Язык сохранён.",
        "draft_cancelled": "Добавление книги отменено.",
        "reserved_status": "Забронирована", "borrowed_status": "Читает",
        "returned_status": "Возвращена", "cancelled_status": "Отменена",
        "expired_status": "Срок брони истёк", "not_found": "Не найдено.",
        "took_notice": "📗 Отмечено получение книги «{title}».\nВерните до: {due}",
        "returned_notice": "🙏 Книга «{title}» возвращена. Спасибо!",
        "cancelled_notice": "❌ Бронь книги «{title}» отменена.",
        "expired_notice": "⌛ Срок брони книги «{title}» истёк. При необходимости забронируйте снова.",
        "child_prefix": "Ваш ребёнок: ",
    },
    "en": {
        "choose_language": "Choose a language / Tilni tanlang / Выберите язык:",
        "region": "Choose your region:", "district": "Choose your district:",
        "school": "Choose your school:", "no_schools": "No libraries have registered yet.",
        "role": "Who are you?", "student": "Student", "parent": "Parent",
        "librarian": "Librarian", "student_menu": "Student menu:", "parent_menu": "Parent menu:",
        "books_button": "📚 Browse books", "parent_code_button": "🔗 Parent linking code",
        "parent_add": "➕ Add a child", "parent_list": "👨‍👩‍👧 My children and books",
        "librarian_menu": "📚 Librarian menu", "add_book_button": "➕ Add a book",
        "book_missing": "There are no books in this library yet.",
        "book_list": "Choose a book. Reservations are held for 2 days:",
        "not_started": "Please send /start first.", "no_libraries": "Sign in as a librarian: /librarian CODE",
        "use_private": "Send this command in your private chat with the bot.",
        "code": "The code is invalid, expired, or already used. Generate a new one on the website.",
        "linked": "✅ Librarian account linked. Send /addbook to add a book.",
        "already_linked": "This Telegram account is linked to another librarian.",
        "need_link": "Generate a one-time code in your website account, then send /librarian CODE.",
        "link_help": "Open the “Connect Telegram group” link on the website, add the bot as an administrator with permission to post, then open the link. Or send /linklibrary CODE in the group.",
        "group_only": "Send this command in the library group.",
        "admin_only": "You must be a group administrator to connect it.",
        "group_linked": "✅ Group connected to the library. New books will be announced here.",
        "group_code": "The group code is invalid or expired. Generate a new one on the website.",
        "book_title": "Send the book title:",         "book_author": "Send the author (send a dash - if unknown):",
        "book_genre": "Send the genre (send a dash - if unknown):",
        "book_total": "How many copies? Enter a number from 1 to 1000:",
        "book_count_error": "Enter a whole number of copies from 1 to 1000.",
        "book_added": "✅ “{title}” was added. An announcement was sent to the connected group.",
        "book_added_no_group": "✅ “{title}” was added. Connect a group on the website to publish announcements.",
        "book_added_no_delivery": "✅ “{title}” was added, but the group announcement was not delivered. Check that the bot can post in the group.",
        "book_title_error": "The title must be 1–200 characters. Try again:",
        "book_author_error": "The author must be no longer than 100 characters. Try again:",
        "book_genre_error": "The genre must be no longer than 60 characters. Try again:",
        "no_book_link": "No group is connected to this library.",
        "reserve": "📲 Reserve in the bot", "copies": "Copies", "available": "Available",
        "author_label": "Author", "genre_label": "Genre", "school_label": "Library",
        "student_label": "Student",
        "reserve_days": "How many days would you like the book? Enter a number from 1 to 60:",
        "identity": "Enter your full name and class (for example: Aziza Karimova, 7-A):",
        "days_error": "Enter a number from 1 to 60.",
        "profile_error": "Enter your name and class separated by a comma (for example: Aziza Karimova, 7-A):",
        "bad_book": "Book not found. Send /start.", "too_many": "You cannot have more than two active reservations.",
        "unavailable": "Sorry, this book is currently unavailable.",
        "reserved": "✅ “{title}” reserved.\nPick it up by {until}.\nReturn it within {days} days after pickup.",
        "reservation_list_empty": "You have no active reservations.", "not_found": "School or region not found. Send /start.",
        "parent_code_prompt": "Enter the 8-character code from your child:",
        "parent_code_bad": "The code is invalid or expired. Ask your child for a new one.",
        "parent_code_self": "You cannot link yourself as your child.", "parent_linked": "✅ Child linked.",
        "child_name": "Enter your child's full name and class (for example: Aziza Karimova, 7-A):",
        "parent_no_children": "No children linked yet. Press “Add a child”.",
        "parent_choose_child": "Choose a child to view their book history:",
        "not_linked_child": "This student is not linked to you.",
        "no_child_history": "This child has no book history yet.",
        "child_history": "📚 Child's book history:", "newer": "⬅️ Newer", "older": "Older ➡️",
        "parent_code_issued": "🔗 Send this code to the parent: <code>{code}</code>\nIt expires in 15 minutes and can only be used once.",
        "no_reservations": "No active reservations.",
        "book_expired": "⌛ The reservation for “{title}” expired. Reserve again if needed.",
        "added_to_group": "📚 The library bot was added to this group. The librarian can connect it with /linklibrary CODE.",
        "language_saved": "✅ Language saved.",
        "draft_cancelled": "Book entry cancelled.",
        "reserved_status": "Reserved", "borrowed_status": "Borrowed",
        "returned_status": "Returned", "cancelled_status": "Cancelled",
        "expired_status": "Reservation expired", "not_found": "Not found.",
        "took_notice": "📗 Pickup of “{title}” was recorded.\nReturn by: {due}",
        "returned_notice": "🙏 “{title}” was returned. Thank you!",
        "cancelled_notice": "❌ The reservation for “{title}” was cancelled.",
        "expired_notice": "⌛ The reservation for “{title}” expired. Reserve again if needed.",
        "child_prefix": "Your child: ",
    },
}

def user_language(tg_id):
    u = one("select language from bot_users where tg_id=?", (tg_id,))
    return u["language"] if u and u["language"] in BOT_TEXT else "uz"

def text(tg_id, key, **values):
    return BOT_TEXT[user_language(tg_id)][key].format(**values)

# ---------- Telegram bot ----------
bot = Bot(TOKEN) if TOKEN else None
dp = Dispatcher()

def kb(btns):
    return InlineKeyboardMarkup(inline_keyboard=btns)

def actor_id(m: Message):
    return m.chat.id if m.chat.type == "private" else m.from_user.id

async def notify(tg, text):
    if not bot:
        return
    try:
        await bot.send_message(tg, text)
    except Exception as e:
        log.warning("Telegram xabari yuborilmadi (tg_id=%s): %s", tg, e)

async def show_viloyatlar(m: Message):
    tg_id = actor_id(m)
    viloyatlar = rows("select distinct vil from schools order by vil")
    if not viloyatlar:
        return await m.answer(text(tg_id, "no_schools"))
    await m.answer(text(tg_id, "region"), reply_markup=kb(
        [[InlineKeyboardButton(text=x["vil"], callback_data=f"v:{i}")]
         for i, x in enumerate(viloyatlar)]))

async def show_tumanlar(m: Message, vil: str, vil_index: int):
    tg_id = actor_id(m)
    tumanlar = rows("select distinct tum from schools where vil=? order by tum", (vil,))
    await m.answer(text(tg_id, "district"), reply_markup=kb(
        [[InlineKeyboardButton(text=x["tum"], callback_data=f"d:{vil_index}:{i}")]
         for i, x in enumerate(tumanlar)]))

async def show_maktablar(m: Message, vil: str, tum: str):
    tg_id = actor_id(m)
    maktablar = rows("select id, name from schools where vil=? and tum=? order by name", (vil, tum))
    await m.answer(text(tg_id, "school"), reply_markup=kb(
        [[InlineKeyboardButton(text=x["name"], callback_data=f"s:{x['id']}")]
         for x in maktablar]))

async def show_roles(m: Message):
    tg_id = actor_id(m)
    await m.answer(text(tg_id, "role"), reply_markup=kb([
        [InlineKeyboardButton(text=text(tg_id, "student"), callback_data="role:student")],
        [InlineKeyboardButton(text=text(tg_id, "parent"), callback_data="role:parent")],
        [InlineKeyboardButton(text=text(tg_id, "librarian"), callback_data="role:librarian")],
    ]))

async def show_student_menu(m: Message):
    tg_id = actor_id(m)
    await m.answer(text(tg_id, "student_menu"), reply_markup=kb([
        [InlineKeyboardButton(text=text(tg_id, "books_button"), callback_data="student:books")],
        [InlineKeyboardButton(text=text(tg_id, "parent_code_button"), callback_data="student:code")],
    ]))

async def show_parent_menu(m: Message):
    tg_id = actor_id(m)
    await m.answer(text(tg_id, "parent_menu"), reply_markup=kb([
        [InlineKeyboardButton(text=text(tg_id, "parent_add"), callback_data="parent:add")],
        [InlineKeyboardButton(text=text(tg_id, "parent_list"), callback_data="parent:list")],
    ]))

async def show_books(m: Message, school_id):
    tg_id = actor_id(m)
    books = rows("select * from books where school_id=?", (school_id,))
    if not books:
        return await m.answer(text(tg_id, "book_missing"))
    available_label = text(tg_id, "available").lower()
    await m.answer(text(tg_id, "book_list"), reply_markup=kb(
        [[InlineKeyboardButton(
            text=f"{b['title'][:40]} ({avail(b)}/{b['total']} {available_label})",
            callback_data=f"b:{b['id']}")] for b in books]))

async def reserve(m: Message, tg, book_id, days, name, cls):
    st = one("select * from students where tg_id=?", (tg,))
    b = one("select * from books where id=?", (book_id,))
    if not st or not b or b["school_id"] != st["school_id"]:
        return await m.answer(text(tg, "bad_book"))
    now = int(time.time())
    if one("select count(*) c from res where tg_id=? and st='band' and until>?", (tg, now))["c"] >= 2:
        return await m.answer(text(tg, "too_many"))
    if avail(b) < 1:
        return await m.answer(text(tg, "unavailable"))
    q("insert into res(school_id,book_id,tg_id,name,created,until,st,days,cls) values(?,?,?,?,?,?,'band',?,?)",
      (b["school_id"], b["id"], tg, name, now, now + HOLD, days, cls))
    q("update students set name=?, cls=?, state=null, want=null, requested_days=null where tg_id=?",
      (name, cls, tg))
    await m.answer(text(tg, "reserved", title=b["title"], until=fmt(now + HOLD), days=days))
    parent_notices = {
        "uz": f"📚 Farzandingiz {name} ({cls}) “{b['title']}” kitobini band qildi.",
        "ru": f"📚 Ваш ребёнок {name} ({cls}) забронировал книгу «{b['title']}».",
        "en": f"📚 Your child {name} ({cls}) reserved “{b['title']}”.",
    }
    await notify_parents(tg, parent_notices)

async def ask_days(m: Message, tg, book_id):
    q("update students set want=?, state='days', requested_days=null where tg_id=?", (book_id, tg))
    await m.answer(text(tg, "reserve_days"))

async def notify_parents(student_tg, content):
    parents = rows("select parent_tg from parent_children where student_tg=?", (student_tg,))
    for parent in parents:
        parent_id = parent["parent_tg"]
        message = (content.get(user_language(parent_id), content.get("uz"))
                   if isinstance(content, dict) else content)
        await notify(parent_id, message)

async def show_librarian_menu(m: Message):
    tg_id = actor_id(m)
    await m.answer(text(tg_id, "librarian_menu"), reply_markup=kb([
        [InlineKeyboardButton(text=text(tg_id, "add_book_button"), callback_data="librarian:addbook")],
    ]))

async def publish_book(book):
    school = one("select name, telegram_group_id from schools where id=?", (book["school_id"],))
    if not bot or not school or not school["telegram_group_id"]:
        return False
    try:
        bot_info = await bot.get_me()
        link = f"https://t.me/{bot_info.username}?start=library"
        available = max(avail(book), 0)
        safe_title = escape(book["title"])
        safe_author = escape(book.get("author") or "—")
        safe_genre = escape(book.get("genre") or "—")
        safe_school = escape(school["name"])
        message = (
            "📚 <b>YANGI KITOB KUTUBXONADA!</b>\n"
            "🇺🇿 O'zbekcha\n"
            f"📖 <b>{safe_title}</b>\n✍️ Muallif: {safe_author}\n"
            f"🎭 Janr: {safe_genre}\n📦 Mavjud: {available}/{book['total']} nusxa\n"
            f"🏫 {safe_school}\n\n"
            "📚 <b>НОВАЯ КНИГА В БИБЛИОТЕКЕ!</b>\n"
            f"📖 <b>{safe_title}</b>\n✍️ Автор: {safe_author}\n"
            f"🎭 Жанр: {safe_genre}\n📦 В наличии: {available}/{book['total']} экз.\n"
            f"🏫 {safe_school}\n\n"
            "📚 <b>NEW BOOK AT THE LIBRARY!</b>\n"
            f"📖 <b>{safe_title}</b>\n✍️ Author: {safe_author}\n"
            f"🎭 Genre: {safe_genre}\n📦 Available: {available}/{book['total']} copies\n"
            f"🏫 {safe_school}\n\n"
            "📲 Bot orqali oldindan band qiling / Бронируйте в боте / Reserve in the bot"
        )
        await bot.send_message(
            school["telegram_group_id"], message, parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="📲 Band qilish / Бронь / Reserve", url=link)
            ]]))
        return True
    except Exception:
        log.exception("Yangi kitob e'loni yuborilmadi (book_id=%s)", book["id"])
        return False

async def publish_imported_books(school_id, books):
    if not books:
        return True
    school = one("select name, telegram_group_id from schools where id=?", (school_id,))
    if not bot or not school or not school["telegram_group_id"]:
        return False
    try:
        bot_info = await bot.get_me()
        link = f"https://t.me/{bot_info.username}?start=library"
        lines = []
        for b in books[:8]:
            lines.append(
                f"• <b>{escape(b['title'][:120])}</b> — "
                f"{escape((b.get('author') or '—')[:80])}; "
                f"{escape((b.get('genre') or '—')[:50])}; "
                f"{max(avail(b), 0)}/{b['total']}"
            )
        if len(books) > 8:
            lines.append(f"… va yana {len(books) - 8} ta kitob")
        message = (
            "📚 <b>KUTUBXONAGA YANGI KITOBLAR / НОВЫЕ КНИГИ / NEW BOOKS</b>\n"
            f"🏫 {escape(school['name'])}\n\n" + "\n".join(lines) +
            "\n\n📲 Botda band qiling / Бронируйте в боте / Reserve in the bot"
        )
        await bot.send_message(
            school["telegram_group_id"], message, parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="📲 Band qilish / Бронь / Reserve", url=link)
            ]]))
        return True
    except Exception:
        log.exception("Import qilingan kitoblar e'loni yuborilmadi (school_id=%s)", school_id)
        return False

async def link_library_group(m: Message, code):
    tg_id = m.from_user.id
    if m.chat.type not in ("group", "supergroup"):
        return await m.answer(text(tg_id, "group_only"))
    librarian = one("select id, school_id from librarians where telegram_tg_id=?", (tg_id,))
    if not librarian:
        return await m.answer(text(tg_id, "need_link"))
    entry = one(
        "select c.librarian_id, l.school_id from library_codes c "
        "join librarians l on l.id=c.librarian_id "
        "where c.code=? and c.purpose='group' and c.expires>?",
        (code.upper(), int(time.time())))
    if (not entry or entry["librarian_id"] != librarian["id"]
            or entry["school_id"] != librarian["school_id"]):
        return await m.answer(text(tg_id, "group_code"))
    if not bot:
        return await m.answer(text(tg_id, "no_book_link"))
    member = await bot.get_chat_member(m.chat.id, tg_id)
    if member.status not in ("administrator", "creator"):
        return await m.answer(text(tg_id, "admin_only"))
    consumed = one(
        "delete from library_codes where code=? and purpose='group' and expires>? "
        "returning librarian_id", (code.upper(), int(time.time())))
    if not consumed or consumed["librarian_id"] != librarian["id"]:
        return await m.answer(text(tg_id, "group_code"))
    q("update schools set telegram_group_id=?, telegram_group_title=? where id=?",
      (m.chat.id, m.chat.title or "", librarian["school_id"]))
    await m.answer(text(tg_id, "group_linked"))

@dp.message(Command("librarian"))
async def link_librarian_code(m: Message, code: str):
    if m.chat.type != "private":
        return await m.answer(text(m.from_user.id, "use_private"))
    code = code.strip().upper()
    entry = one(
        "select librarian_id from library_codes "
        "where code=? and purpose='access' and expires>?", (code, int(time.time())))
    if not entry:
        return await m.answer(text(m.from_user.id, "code"))
    other = one("select id from librarians where telegram_tg_id=? and id<>?",
                (m.from_user.id, entry["librarian_id"]))
    if other:
        return await m.answer(text(m.from_user.id, "already_linked"))
    consumed = one(
        "delete from library_codes where code=? and purpose='access' and expires>? "
        "returning librarian_id", (code, int(time.time())))
    if not consumed:
        return await m.answer(text(m.from_user.id, "code"))
    q("update librarians set telegram_tg_id=? where id=?",
      (m.from_user.id, consumed["librarian_id"]))
    q("insert into bot_users(tg_id) values(?) on conflict(tg_id) do nothing", (m.from_user.id,))
    await m.answer(text(m.from_user.id, "linked"))
    await show_librarian_menu(m)

@dp.message(Command("librarian"))
async def link_librarian(m: Message):
    parts = (m.text or "").split(maxsplit=1)
    code = parts[1].strip() if len(parts) > 1 else ""
    await link_librarian_code(m, code)

@dp.message(Command("linklibrary"))
async def link_group_command(m: Message):
    parts = (m.text or "").split(maxsplit=1)
    code = parts[1].strip() if len(parts) > 1 else ""
    if not code:
        return await m.answer(text(m.from_user.id, "link_help"))
    await link_library_group(m, code)

async def begin_add_book(m: Message, tg_id: int | None = None):
    if m.chat.type != "private":
        return await m.answer(text(m.from_user.id, "use_private"))
    if tg_id is None:
        tg_id = actor_id(m)
    librarian = one("select id, school_id from librarians where telegram_tg_id=?",
                    (tg_id,))
    if not librarian:
        return await m.answer(text(tg_id, "no_libraries"))
    q(
        "insert into librarian_book_drafts(tg_id,librarian_id,school_id,current_step) "
        "values(?,?,?,'title') on conflict(tg_id) do update set "
        "librarian_id=excluded.librarian_id, school_id=excluded.school_id, "
        "current_step='title', title=null, author=null, genre=null",
        (tg_id, librarian["id"], librarian["school_id"]))
    await m.answer(text(tg_id, "book_title"))

@dp.message(Command("addbook"))
async def add_book_command(m: Message):
        await begin_add_book(m)

@dp.message(Command("cancel"))
async def cancel_book_draft(m: Message):
    if m.chat.type != "private":
        return await m.answer(text(m.from_user.id, "use_private"))
    q("delete from librarian_book_drafts where tg_id=?", (m.from_user.id,))
    await m.answer(text(m.from_user.id, "draft_cancelled"))

@dp.callback_query(F.data == "librarian:addbook")
async def add_book_button(c: CallbackQuery):
    await c.answer()
    await begin_add_book(c.message, c.from_user.id)

def language_keyboard():
    return kb([
        [InlineKeyboardButton(text="🇺🇿 O'zbekcha", callback_data="language:uz")],
        [InlineKeyboardButton(text="🇷🇺 Русский", callback_data="language:ru")],
        [InlineKeyboardButton(text="🇬🇧 English", callback_data="language:en")],
    ])

async def start_for_user(m: Message):
    preferences = one("select language from bot_users where tg_id=?", (m.from_user.id,))
    if not preferences or not preferences["language"]:
        await m.answer(BOT_TEXT["uz"]["choose_language"], reply_markup=language_keyboard())
        return
    librarian = one("select id from librarians where telegram_tg_id=?", (m.from_user.id,))
    if librarian:
        return await show_librarian_menu(m)
    await show_viloyatlar(m)

@dp.message(CommandStart())
async def start(m: Message):
    parts = (m.text or "").split(maxsplit=1)
    payload = parts[1].strip() if len(parts) > 1 else ""
    if m.chat.type in ("group", "supergroup"):
        if payload:
            return await link_library_group(m, payload)
        return await m.answer(text(m.from_user.id, "added_to_group"))
    q("insert into bot_users(tg_id) values(?) on conflict(tg_id) do nothing", (m.from_user.id,))
    if payload.lower().startswith("librarian_"):
        return await link_librarian_code(m, payload.split("_", 1)[1])
    await start_for_user(m)

@dp.message(Command("maktab"))
async def change_school(m: Message):
    await show_viloyatlar(m)

@dp.message(Command("language"))
async def change_language(m: Message):
    await m.answer(BOT_TEXT[user_language(m.from_user.id)]["choose_language"],
                   reply_markup=language_keyboard())

@dp.callback_query(F.data.startswith("language:"))
async def pick_language(c: CallbackQuery):
    selected = c.data.split(":", 1)[1]
    if selected not in BOT_TEXT:
        await c.answer()
        return
    q("insert into bot_users(tg_id,language) values(?,?) "
      "on conflict(tg_id) do update set language=excluded.language",
      (c.from_user.id, selected))
    await c.answer(BOT_TEXT[selected]["language_saved"])
    if one("select id from librarians where telegram_tg_id=?", (c.from_user.id,)):
        await show_librarian_menu(c.message)
    else:
        await show_viloyatlar(c.message)

@dp.message(Command("band"))
async def mine(m: Message):
    r = rows("select r.*, b.title from res r join books b on b.id=r.book_id where r.tg_id=? and r.st='band' and r.until>?",
             (m.from_user.id, int(time.time())))
    await m.answer("\n".join(f"📖 {x['title']} — {fmt(x['until'])}" for x in r)
                   or text(m.from_user.id, "reservation_list_empty"))

@dp.callback_query(F.data.startswith("v:"))
async def pick_viloyat(c: CallbackQuery):
    vil_index = int(c.data[2:])
    viloyatlar = rows("select distinct vil from schools order by vil")
    await c.answer()
    if vil_index < 0 or vil_index >= len(viloyatlar):
        return await c.message.answer(text(c.from_user.id, "not_found"))
    await show_tumanlar(c.message, viloyatlar[vil_index]["vil"], vil_index)

@dp.callback_query(F.data.startswith("d:"))
async def pick_tuman(c: CallbackQuery):
    _, vil_index, tum_index = c.data.split(":")
    viloyatlar = rows("select distinct vil from schools order by vil")
    await c.answer()
    vi, ti = int(vil_index), int(tum_index)
    if vi < 0 or vi >= len(viloyatlar):
        return await c.message.answer(text(c.from_user.id, "not_found"))
    vil = viloyatlar[vi]["vil"]
    tumanlar = rows("select distinct tum from schools where vil=? order by tum", (vil,))
    if ti < 0 or ti >= len(tumanlar):
        return await c.message.answer(text(c.from_user.id, "not_found"))
    await show_maktablar(c.message, vil, tumanlar[ti]["tum"])

@dp.callback_query(F.data.startswith("s:"))
async def pick_school(c: CallbackQuery):
    sid = int(c.data[2:])
    if not one("select 1 from schools where id=?", (sid,)):
        await c.answer(text(c.from_user.id, "not_found"), show_alert=True)
        return
    q("insert into students(tg_id,school_id) values(?,?) on conflict(tg_id) do update set school_id=excluded.school_id",
      (c.from_user.id, sid))
    await c.answer()
    q("update students set state=null, pending=null, want=null, requested_days=null where tg_id=?",
      (c.from_user.id,))
    await show_roles(c.message)

@dp.callback_query(F.data.startswith("role:"))
async def pick_role(c: CallbackQuery):
    role = c.data.split(":", 1)[1]
    st = one("select * from students where tg_id=?", (c.from_user.id,))
    await c.answer()
    if role == "librarian":
        if one("select id from librarians where telegram_tg_id=?", (c.from_user.id,)):
            return await show_librarian_menu(c.message)
        return await c.message.answer(text(c.from_user.id, "need_link"))
    if role not in ("student", "parent") or not st:
        return await c.message.answer(text(c.from_user.id, "not_started"))
    q("update students set role=?, state=null, want=null, requested_days=null where tg_id=?",
      (role, c.from_user.id))
    if role == "student":
        await show_student_menu(c.message)
    else:
        await show_parent_menu(c.message)

@dp.callback_query(F.data == "student:books")
async def student_books(c: CallbackQuery):
    st = one("select * from students where tg_id=? and role='student'", (c.from_user.id,))
    await c.answer()
    if not st:
        return await c.message.answer(text(c.from_user.id, "not_started"))
    await show_books(c.message, st["school_id"])

@dp.callback_query(F.data == "student:code")
async def student_code(c: CallbackQuery):
    st = one("select * from students where tg_id=? and role='student'", (c.from_user.id,))
    await c.answer()
    if not st:
        return await c.message.answer(text(c.from_user.id, "not_started"))
    if not st["name"]:
        q("update students set state='child_profile' where tg_id=?", (c.from_user.id,))
        return await c.message.answer(text(c.from_user.id, "child_name"))
    await issue_parent_code(c.message, c.from_user.id, st["school_id"])

async def issue_parent_code(m: Message, student_tg, school_id):
    code = secrets.token_hex(4).upper()
    expires = int(time.time()) + 900
    q("delete from parent_codes where student_tg=?", (student_tg,))
    q("insert into parent_codes(code,student_tg,school_id,expires) values(?,?,?,?)",
      (code, student_tg, school_id, expires))
    await m.answer(text(student_tg, "parent_code_issued", code=code), parse_mode="HTML")

@dp.callback_query(F.data == "parent:add")
async def parent_add(c: CallbackQuery):
    st = one("select 1 from students where tg_id=? and role='parent'", (c.from_user.id,))
    await c.answer()
    if not st:
        return await c.message.answer(text(c.from_user.id, "not_started"))
    q("update students set state='link_code' where tg_id=?", (c.from_user.id,))
    await c.message.answer(text(c.from_user.id, "parent_code_prompt"))

@dp.callback_query(F.data == "parent:list")
async def parent_list(c: CallbackQuery):
    st = one("select 1 from students where tg_id=? and role='parent'", (c.from_user.id,))
    await c.answer()
    if not st:
        return await c.message.answer(text(c.from_user.id, "not_started"))
    children = rows(
        "select s.tg_id, s.name, s.cls, sc.name school from parent_children pc "
        "join students s on s.tg_id=pc.student_tg join schools sc on sc.id=s.school_id "
        "where pc.parent_tg=? order by s.name, s.tg_id", (c.from_user.id,))
    if not children:
        return await c.message.answer(text(c.from_user.id, "parent_no_children"))
    await c.message.answer(text(c.from_user.id, "parent_choose_child"),
                           reply_markup=kb([
                               [InlineKeyboardButton(
                                   text=(f"{x['name'] or text(c.from_user.id, 'student_label')}"
                                         f"{' (' + x['cls'] + ')' if x['cls'] else ''} — {x['school']}")[:64],
                                   callback_data=f"child:{x['tg_id']}")]
                               for x in children]))

@dp.callback_query(F.data.startswith("child:"))
async def child_history(c: CallbackQuery):
    parts = c.data.split(":")
    child_tg = int(parts[1])
    offset = max(int(parts[2]), 0) if len(parts) > 2 else 0
    linked = one("select 1 from parent_children where parent_tg=? and student_tg=?",
                 (c.from_user.id, child_tg))
    await c.answer()
    if not linked:
        return await c.message.answer(text(c.from_user.id, "not_linked_child"))
    total = one("select count(*) c from res where tg_id=?", (child_tg,))["c"]
    history = rows(
        "select r.*, b.title from res r join books b on b.id=r.book_id "
        "where r.tg_id=? order by r.id desc limit 10 offset ?", (child_tg, offset))
    if not history:
        return await c.message.answer(text(c.from_user.id, "no_child_history"))
    status_key = {"band": "reserved_status", "olindi": "borrowed_status", "qaytarildi": "returned_status",
                  "bekor": "cancelled_status", "muddati": "expired_status"}
    message = text(c.from_user.id, "child_history") + "\n\n" + "\n".join(
        f"• {x['title'][:120]} — {BOT_TEXT[user_language(c.from_user.id)].get(status_key.get(x['st'], ''), x['st'])}"
        f"{' (' + x['name'] + ', ' + x['cls'] + ')' if x['cls'] else ''}"
        f"\n  {fmt(x['created'])}" for x in history)
    pages = []
    if offset:
        pages.append(InlineKeyboardButton(text=text(c.from_user.id, "newer"), callback_data=f"child:{child_tg}:{max(0, offset - 10)}"))
    if offset + len(history) < total:
        pages.append(InlineKeyboardButton(text=text(c.from_user.id, "older"), callback_data=f"child:{child_tg}:{offset + 10}"))
    await c.message.answer(message, reply_markup=kb([pages]) if pages else None)

@dp.callback_query(F.data.startswith("b:"))
async def pick_book(c: CallbackQuery):
    await c.answer()
    st = one("select * from students where tg_id=? and role='student'", (c.from_user.id,))
    if not st:
        return await c.message.answer(text(c.from_user.id, "not_started"))
    await ask_days(c.message, c.from_user.id, int(c.data[2:]))

@dp.message(F.text & ~F.text.startswith("/"))
async def got_text(m: Message):
    code = (m.text or "").strip().upper()
    if (m.chat.type == "private" and len(code) == 16
            and all(char in "0123456789ABCDEF" for char in code)):
        return await link_librarian_code(m, code)
    draft = one("select * from librarian_book_drafts where tg_id=?", (m.from_user.id,))
    if draft and m.chat.type == "private":
        value = m.text.strip()
        if draft["current_step"] == "title":
            if not 1 <= len(value) <= 200:
                return await m.answer(text(m.from_user.id, "book_title_error"))
            q("update librarian_book_drafts set title=?, current_step='author' where tg_id=?",
              (value, m.from_user.id))
            return await m.answer(text(m.from_user.id, "book_author"))
        if draft["current_step"] == "author":
            value = "" if value in ("-", "—") else value
            if len(value) > 100:
                return await m.answer(text(m.from_user.id, "book_author_error"))
            q("update librarian_book_drafts set author=?, current_step='genre' where tg_id=?",
              (value, m.from_user.id))
            return await m.answer(text(m.from_user.id, "book_genre"))
        if draft["current_step"] == "genre":
            value = "" if value in ("-", "—") else value
            if len(value) > 60:
                return await m.answer(text(m.from_user.id, "book_genre_error"))
            q("update librarian_book_drafts set genre=?, current_step='total' where tg_id=?",
              (value, m.from_user.id))
            return await m.answer(text(m.from_user.id, "book_total"))
        if draft["current_step"] == "total":
            if not value.isdigit() or not 1 <= int(value) <= 1000:
                return await m.answer(text(m.from_user.id, "book_count_error"))
            book = one(
                "insert into books(school_id,title,author,genre,total) values(?,?,?,?,?) returning *",
                (draft["school_id"], draft["title"], draft["author"] or "",
                 draft["genre"] or "", int(value)))
            q("delete from librarian_book_drafts where tg_id=?", (m.from_user.id,))
            published = await publish_book(book)
            school = one("select telegram_group_id from schools where id=?",
                         (draft["school_id"],))
            announce_key = "book_added" if published else (
                "book_added_no_group" if not school or not school["telegram_group_id"]
                else "book_added_no_delivery")
            await m.answer(text(m.from_user.id, announce_key, title=book["title"]))
            return await show_librarian_menu(m)
    st = one("select * from students where tg_id=?", (m.from_user.id,))
    if not st:
        return await show_viloyatlar(m)
    if st["state"] == "link_code" and st["role"] == "parent":
        code = m.text.strip().upper()
        entry = one("select * from parent_codes where code=? and expires>?", (code, int(time.time())))
        if not entry:
            return await m.answer(text(m.from_user.id, "parent_code_bad"))
        if entry["student_tg"] == m.from_user.id:
            return await m.answer(text(m.from_user.id, "parent_code_self"))
        if not q("delete from parent_codes where code=? and expires>?",
                 (code, int(time.time()))).rowcount:
            return await m.answer(text(m.from_user.id, "parent_code_bad"))
        q("insert into parent_children(parent_tg,student_tg) values(?,?) on conflict do nothing",
          (m.from_user.id, entry["student_tg"]))
        q("update students set state=null where tg_id=?", (m.from_user.id,))
        await m.answer(text(m.from_user.id, "parent_linked"))
        await show_parent_menu(m)
        return
    if st["state"] == "child_profile" and st["role"] == "student":
        parts = [part.strip() for part in m.text.split(",", 1)]
        if len(parts) != 2 or len(parts[0]) < 3 or not parts[1] or len(parts[0]) > 60 or len(parts[1]) > 10:
            return await m.answer(text(m.from_user.id, "profile_error"))
        q("update students set name=?, cls=?, state=null where tg_id=?",
          (parts[0], parts[1], m.from_user.id))
        return await issue_parent_code(m, m.from_user.id, st["school_id"])
    if st["state"] == "days" and st["role"] == "student":
        t = m.text.strip()
        if not t.isdigit() or not 1 <= int(t) <= 60:
            return await m.answer(text(m.from_user.id, "days_error"))
        q("update students set requested_days=?, state='identity' where tg_id=?",
          (int(t), m.from_user.id))
        return await m.answer(text(m.from_user.id, "identity"))
    if st["state"] == "identity" and st["role"] == "student":
        parts = [part.strip() for part in m.text.split(",", 1)]
        if len(parts) != 2 or len(parts[0]) < 3 or not parts[1] or len(parts[0]) > 60 or len(parts[1]) > 10:
            return await m.answer(text(m.from_user.id, "profile_error"))
        return await reserve(m, m.from_user.id, st["want"], st["requested_days"], parts[0], parts[1])
    if st["role"] == "parent":
        return await show_parent_menu(m)
    if st["role"] == "student":
        return await show_student_menu(m)
    await show_roles(m)

async def expirer():
    while True:
        now = int(time.time())
        for x in rows("select r.*, b.title from res r join books b on b.id=r.book_id where r.st='band' and r.until<=?", (now,)):
            q("update res set st='muddati' where id=?", (x["id"],))
            await notify(x["tg_id"], text(x["tg_id"], "expired_notice", title=x["title"]))
        await asyncio.sleep(300)

# ---------- Veb sayt API ----------
@asynccontextmanager
async def lifespan(app):
    tasks = [asyncio.create_task(expirer())]
    if bot:
        tasks.append(asyncio.create_task(dp.start_polling(bot)))
    else:
        print("DIQQAT: .env faylida BOT_TOKEN yo'q, bot ishlamaydi.")
    yield
    for t in tasks:
        t.cancel()
    pool.close()

app = FastAPI(lifespan=lifespan)

class Reg(BaseModel):
    name: str; fam: str; vil: str; tum: str; maktab: str; login: str; parol: str

class Login(BaseModel):
    login: str; parol: str

class BookIn(BaseModel):
    title: str; author: str = ""; genre: str = ""; total: int = 1

class Lend(BaseModel):
    name: str; book_id: int; days: int = 14

def me(authorization: str = Header("")):
    u = one("select * from librarians where token=?", (authorization.replace("Bearer ", ""),)) if authorization else None
    if not u:
        raise HTTPException(401, "Kirish kerak")
    return u

@app.api_route("/", methods=["GET", "HEAD"])
def index():
    return FileResponse("static/index.html")

@app.post("/api/register")
def register(d: Reg):
    if not all(x.strip() for x in (d.name, d.fam, d.vil, d.tum, d.maktab, d.login)) or len(d.parol) < 4:
        raise HTTPException(400, "Barcha maydonlarni to'ldiring (parol kamida 4 belgi)")
    if one("select 1 from librarians where login=?", (d.login,)):
        raise HTTPException(400, "Bu login band")
    key = (d.vil, d.tum.strip(), d.maktab.strip())
    q("insert into schools(vil,tum,name) values(?,?,?) on conflict do nothing", key)
    s = one("select id from schools where vil=? and tum=? and name=?", key)
    tok = secrets.token_hex(16)
    q("insert into librarians(school_id,name,fam,login,pw,token) values(?,?,?,?,?,?)",
      (s["id"], d.name.strip(), d.fam.strip(), d.login, hashpw(d.parol, d.login), tok))
    return {"token": tok}

@app.post("/api/login")
def login(d: Login):
    u = one("select * from librarians where login=? and pw=?", (d.login, hashpw(d.parol, d.login)))
    if not u:
        raise HTTPException(400, "Login yoki parol xato")
    tok = secrets.token_hex(16)
    q("update librarians set token=? where id=?", (tok, u["id"]))
    return {"token": tok}

@app.get("/api/me")
def get_me(u=Depends(me)):
    return {"name": u["name"], "fam": u["fam"], "school": one("select * from schools where id=?", (u["school_id"],))}

@app.get("/api/telegram/status")
def get_telegram_status(u=Depends(me)):
    school = one("select telegram_group_id, telegram_group_title from schools where id=?",
                 (u["school_id"],))
    return {
        "linked": bool(u.get("telegram_tg_id")),
        "group_linked": bool(school and school["telegram_group_id"]),
        "group_title": school["telegram_group_title"] if school else None,
    }

@app.post("/api/telegram/access-code")
async def create_librarian_code(u=Depends(me)):
    if not bot:
        raise HTTPException(503, "Telegram bot sozlanmagan")
    bot_info = await bot.get_me()
    if not bot_info.username:
        raise HTTPException(503, "Telegram bot username sozlanmagan")
    now = int(time.time())
    q("delete from library_codes where expires<=?", (now,))
    q("delete from library_codes where librarian_id=? and purpose='access'", (u["id"],))
    code = secrets.token_hex(8).upper()
    q("insert into library_codes(code,librarian_id,purpose,expires) values(?,?,'access',?)",
      (code, u["id"], now + 600))
    return {"code": code, "expires_in": 600,
                        "bot_url": f"https://t.me/{bot_info.username}?start=librarian_{code}"}

@app.post("/api/telegram/group-link")
async def create_group_link(u=Depends(me)):
    if not bot:
        raise HTTPException(503, "Telegram bot sozlanmagan")
    bot_info = await bot.get_me()
    if not bot_info.username:
        raise HTTPException(503, "Telegram bot username sozlanmagan")
    now = int(time.time())
    q("delete from library_codes where expires<=?", (now,))
    q("delete from library_codes where librarian_id=? and purpose='group'", (u["id"],))
    code = secrets.token_hex(8).upper()
    q("insert into library_codes(code,librarian_id,purpose,expires) values(?,?,'group',?)",
      (code, u["id"], now + 1800))
    return {
        "code": code,
        "expires_in": 1800,
        "url": f"https://t.me/{bot_info.username}?startgroup={code}",
    }

@app.get("/api/books")
def get_books(u=Depends(me)):
    return [dict(b, avail=avail(b)) for b in rows("select * from books where school_id=?", (u["school_id"],))]

@app.post("/api/books")
async def add_book(d: BookIn, u=Depends(me)):
    if not d.title.strip():
        raise HTTPException(400, "Kitob nomini yozing")
    if len(d.title.strip()) > 200 or len(d.author.strip()) > 100 or len(d.genre.strip()) > 60:
        raise HTTPException(400, "Nomi 200, muallif 100, janr 60 belgidan oshmasin")
    book = one(
        "insert into books(school_id,title,author,genre,total) values(?,?,?,?,?) returning *",
        (u["school_id"], d.title.strip(), d.author.strip(), d.genre.strip(),
         min(max(d.total, 1), 1000)))
    announced = await publish_book(book)
    return {"ok": True, "announced": announced}

@app.get("/api/reservations")
def get_res(u=Depends(me)):
    out = rows("select r.*, b.title from res r join books b on b.id=r.book_id where r.school_id=? order by r.id desc", (u["school_id"],))
    now = int(time.time())
    for x in out:
        if x["st"] == "band" and x["until"] <= now:
            x["st"] = "muddati"
        x["late"] = bool(x["st"] == "olindi" and x.get("due") and x["due"] < now)
    return out

@app.post("/api/reservations/{rid}/{action}")
async def do_action(rid: int, action: str, u=Depends(me)):
    new = {"took": "olindi", "returned": "qaytarildi", "cancel": "bekor"}.get(action)
    r = one("select r.*, b.title from res r join books b on b.id=r.book_id where r.id=? and r.school_id=?", (rid, u["school_id"]))
    if not new or not r:
        raise HTTPException(404, "Topilmadi")
    due = int(time.time()) + (r.get("days") or 14) * 86400
    q("update res set st=?, due=? where id=?", (new, due if new == "olindi" else r.get("due"), rid))
    notice_key = {
        "olindi": "took_notice", "qaytarildi": "returned_notice",
        "bekor": "cancelled_notice",
    }[new]
    notice_values = {"title": r["title"], "due": fmt(due)}
    localized_notices = {
        language: BOT_TEXT[language][notice_key].format(**notice_values)
        for language in BOT_TEXT
    }
    msg = localized_notices[user_language(r["tg_id"])]
    await notify(r["tg_id"], msg)
    if r["tg_id"]:
        await notify_parents(r["tg_id"], {
            language: BOT_TEXT[language]["child_prefix"] + localized_notices[language]
            for language in BOT_TEXT
        })
    return {"ok": True}

@app.post("/api/lend")
def lend(d: Lend, u=Depends(me)):
    b = one("select * from books where id=? and school_id=?", (d.book_id, u["school_id"]))
    if not b or not d.name.strip():
        raise HTTPException(400, "O'quvchi va kitobni tanlang")
    if avail(b) < 1:
        raise HTTPException(400, "Bu kitob hozir qolmagan")
    now = int(time.time())
    days = min(max(d.days, 1), 60)
    q("insert into res(school_id,book_id,tg_id,name,created,until,st,days,due) values(?,?,?,?,?,?,'olindi',?,?)",
      (u["school_id"], b["id"], 0, d.name.strip(), now, now, days, now + days * 86400))
    return {"ok": True}


# ---------- O'quvchilar ro'yxati (Excel import) ----------
class PupilIn(BaseModel):
    name: str; cls: str

class PupilsIn(BaseModel):
    items: list[PupilIn]

@app.get("/api/pupils")
def get_pupils(u=Depends(me)):
    return rows("select id, name, cls from pupils where school_id=? order by cls, name", (u["school_id"],))

@app.post("/api/pupils/import")
def import_pupils(d: PupilsIn, u=Depends(me)):
    sid = u["school_id"]
    items = []
    for p in d.items[:5000]:
        nm, c = p.name.strip()[:60], p.cls.strip()[:10]
        if nm and c:
            items.append((sid, nm, c))
    before = one("select count(*) c from pupils where school_id=?", (sid,))["c"]
    many("insert into pupils(school_id,name,cls) values(?,?,?) on conflict do nothing", items)
    after = one("select count(*) c from pupils where school_id=?", (sid,))["c"]
    return {"added": after - before}

@app.delete("/api/pupils/{pid}")
def del_pupil(pid: int, u=Depends(me)):
    q("delete from pupils where id=? and school_id=?", (pid, u["school_id"]))
    return {"ok": True}


# ---------- Kitoblarni Excel orqali import ----------
class BookRow(BaseModel):
    title: str; author: str = ""; genre: str = ""; total: int = 1

class BooksIn(BaseModel):
    items: list[BookRow]

@app.post("/api/books/import")
async def import_books(d: BooksIn, u=Depends(me)):
    sid, skipped = u["school_id"], 0
    have = {(r["title"].lower(), (r["author"] or "").lower())
            for r in rows("select title, author from books where school_id=?", (sid,))}
    to_add = []
    announcements = []
    for b in d.items[:5000]:
        ti, au, genre = b.title.strip()[:200], b.author.strip()[:100], b.genre.strip()[:60]
        if not ti:
            continue
        key = (ti.lower(), au.lower())
        if key in have:
            skipped += 1
            continue
        have.add(key)
        total = min(max(b.total, 1), 1000)
        to_add.append((sid, ti, au, genre, total))
        announcements.append({"id": -1, "school_id": sid, "title": ti,
                              "author": au, "genre": genre, "total": total})
    many("insert into books(school_id,title,author,genre,total) values(?,?,?,?,?)", to_add)
    announced = await publish_imported_books(sid, announcements)
    return {"added": len(to_add), "skipped": skipped, "announced": announced}
