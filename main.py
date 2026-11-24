import asyncio, base64, binascii, logging, os, secrets, time, hashlib, uuid
from pathlib import Path
from types import SimpleNamespace
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from html import escape

from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart, Command
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton, FSInputFile,
)
from dotenv import load_dotenv
from fastapi import FastAPI, Depends, File, Header, HTTPException, UploadFile
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
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
ADMIN_RESET_KEY = os.getenv("ADMIN_RESET_KEY", "")

pool = ConnectionPool(
    DATABASE_URL, min_size=1, max_size=5, open=True,
    kwargs={"row_factory": dict_row, "autocommit": True, "prepare_threshold": None},
    check=ConnectionPool.check_connection,  # uzilgan ulanishlarni avtomatik yangilaydi
)

SCHEMA = """
create table if not exists schools(id serial primary key, vil text, tum text, name text, unique(vil,tum,name));
create table if not exists librarians(id serial primary key, school_id int, name text, fam text, login text unique, pw text, token text, telegram_tg_id bigint);
create table if not exists books(id serial primary key, school_id int, title text, author text, genre text not null default '', total int, description text not null default '', cover_url text not null default '', barcode text not null default '', pdf_url text not null default '');
create table if not exists students(tg_id bigint primary key, school_id int, name text, pending int, want int, role text, state text, requested_days int, cls text, notify_parents boolean not null default true);
create table if not exists res(id serial primary key, school_id int, book_id int, tg_id bigint, name text, created bigint, until bigint, st text, days int, due bigint, cls text, reminder_pre_sent bigint, reminder_overdue_sent bigint, reminder_pre_parent_sent bigint, reminder_overdue_parent_sent bigint);
create table if not exists pupils(id serial primary key, school_id int, name text, cls text, unique(school_id,name,cls));
create table if not exists parent_children(parent_tg bigint, student_tg bigint, primary key(parent_tg, student_tg));
create table if not exists parent_codes(code text primary key, student_tg bigint, school_id int, expires bigint);
create table if not exists bot_users(tg_id bigint primary key, language text);
create table if not exists library_codes(code text primary key, librarian_id int not null, purpose text not null, expires bigint not null);
create table if not exists library_groups(school_id int not null, telegram_group_id bigint not null, telegram_group_title text, primary key(school_id, telegram_group_id));
create table if not exists librarian_book_drafts(tg_id bigint primary key, librarian_id int not null, school_id int not null, current_step text not null, title text, author text, genre text, description text, cover_url text, barcode text, pdf_url text);
create table if not exists librarian_news_drafts(tg_id bigint primary key, librarian_id int not null, school_id int not null, current_step text not null, message text, image_path text);
create table if not exists news_announcements(id serial primary key, school_id int not null, librarian_id int not null, message text not null, photo_path text, scheduled_at bigint, status text not null default 'queued', created bigint not null, updated bigint not null, sent_at bigint);
create table if not exists news_announcement_groups(announcement_id int not null, group_id bigint not null, status text not null default 'pending', primary key(announcement_id,group_id));
create table if not exists telegram_notifications(id serial primary key, school_id int not null, recipient_tg bigint not null, message text not null, kind text not null, parse_mode text, button_text text, button_url text, status text not null default 'failed', attempts int not null default 1, last_error text not null default '', created bigint not null, updated bigint not null, sent_at bigint);
create index if not exists telegram_notifications_school_status_idx on telegram_notifications(school_id, status, created desc);
alter table telegram_notifications add column if not exists photo_path text;
alter table telegram_notifications add column if not exists news_announcement_id int;
alter table librarian_news_drafts add column if not exists image_path text;
create index if not exists news_announcements_due_idx on news_announcements(status, scheduled_at);
alter table librarians add column if not exists telegram_tg_id bigint;
alter table students add column if not exists notify_parents boolean not null default true;
alter table schools add column if not exists telegram_group_id bigint;
alter table schools add column if not exists telegram_group_title text;
alter table books add column if not exists genre text not null default '';
alter table books add column if not exists description text not null default '';
alter table books add column if not exists cover_url text not null default '';
alter table books add column if not exists barcode text not null default '';
alter table books add column if not exists pdf_url text not null default '';
alter table res add column if not exists reminder_pre_sent bigint;
alter table res add column if not exists reminder_overdue_sent bigint;
alter table res add column if not exists reminder_pre_parent_sent bigint;
alter table res add column if not exists reminder_overdue_parent_sent bigint;
alter table librarian_book_drafts add column if not exists description text;
alter table librarian_book_drafts add column if not exists cover_url text;
alter table librarian_book_drafts add column if not exists barcode text;
alter table librarian_book_drafts add column if not exists pdf_url text;
create unique index if not exists books_school_barcode_unique
on books(school_id, barcode) where barcode <> '';
insert into library_groups(school_id, telegram_group_id, telegram_group_title)
select id, telegram_group_id, telegram_group_title from schools
where telegram_group_id is not null on conflict do nothing;
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
        "student_menu_button": "🎓 O'quvchi menyusi",
        "parent_menu_button": "👨‍👩‍👧 Ota-ona menyusi",
        "librarian_menu_button": "📚 Kutubxonachi menyusi",
        "quick_menu_hint": "Bo'limga qaytish uchun pastdagi tugmani bosing.",
        "student_profile_button": "👤 Ism va sinfni kiritish",
        "parent_reminder_button": "👨‍👩‍👧 Ota-onaga eslatma",
        "parent_reminder_enabled": "✅ Ota-onaga eslatmalar yoqilgan",
        "parent_reminder_disabled": "🔕 Ota-onaga eslatmalar o'chirilgan",
        "parent_reminder_changed": "Ota-onaga eslatma sozlamasi o'zgartirildi.",
        "profile_prompt": "Ism-familiyangiz va sinfingizni vergul bilan yozing (masalan: Aziza Karimova, 7-A):",
        "profile_saved": "✅ Ism-familiya va sinf saqlandi.",
        "parent_code_button": "🔗 Ota-onaga ulanish kodi", "parent_add": "➕ Farzand qo'shish",
        "parent_list": "👨‍👩‍👧 Farzandlarim va kitoblari", "librarian_menu": "📚 Kutubxonachi bo'limi",
        "add_book_button": "➕ Yangi kitob qo'shish", "book_missing": "Bu kutubxonada hozircha kitob yo'q.",
        "book_list": "Kitobni tanlang. Band qilish 2 kun saqlanadi:",
        "not_started": "Avval /start buyrug'ini bosing.", "no_libraries": "Kutubxonachi hisobiga kiring: /librarian KOD",
        "use_private": "Bu buyruqni bot bilan shaxsiy chatda yuboring.",
        "code": "Kod xato, eskirgan yoki avval ishlatilgan. Saytdan yangi kod yarating.",
        "linked": "✅ Kutubxonachi hisobi ulandi. Kitob qo'shish uchun /addbook, yangilik e'lon qilish uchun /news yuboring.",
        "already_linked": "Bu Telegram hisobi boshqa kutubxonachiga ulangan.",
        "need_link": "Avval sayt akkauntingizdan bir martalik kod olib, botga /librarian KOD yuboring.",
        "link_help": "Guruhga ulash uchun saytdagi “Telegram guruhini ulash” havolasini oching, botni guruhga qo'shib admin qiling (xabar yuborish huquqi bilan). So'ng havolani oching yoki guruhda /linklibrary KOD yuboring.",
        "group_only": "Bu buyruqni kutubxona guruhida yuboring.",
        "admin_only": "Guruhni ulash uchun guruh administratori bo'lishingiz kerak.",
        "group_linked": "✅ Guruh kutubxonaga ulandi. Kitoblar va yangiliklar shu guruhga e'lon qilinadi.",
        "group_linked_books": "✅ Guruh kutubxonaga ulandi. Avvalgi kitoblar e'loni: {sent}/{total} ta yuborildi.",
        "group_linked_books_failed": "✅ Guruh kutubxonaga ulandi. Avvalgi kitoblar e'loni: {sent}/{total} ta yuborildi, {failed} tasi yetkazilmadi. Yetkazilmaganlarni saytdagi Telegram bo'limidan qayta yuboring.",
        "group_code": "Guruh kodi xato yoki muddati o'tgan. Saytdan yangisini yarating.",
        "book_title": "Kitob nomini yuboring:",
        "book_author": "Muallifini yuboring (noma'lum bo'lsa, - belgisi yuboring):",
        "book_genre": "Janrini yuboring (noma'lum bo'lsa, - belgisi yuboring):",
        "book_description": "Kitob haqida qisqacha ma'lumot yuboring (bo'sh qoldirish uchun -):",
        "book_barcode": "Kitob shtrix-kodini/QR matnini yuboring (bo'lmasa -):",
        "book_cover": "Muqova rasmi URL manzilini yuboring (bo'lmasa -):",
        "book_pdf": "Kitobning PDF faylini yuboring (bo'lmasa -):",
        "cover_link": "🖼 Muqovani ko'rish",
        "book_description_error": "Tavsif 1000 belgidan oshmasin. Qayta yuboring:",
        "book_barcode_error": "Kod 80 belgidan oshmasin. Qayta yuboring:",
        "book_cover_error": "Muqova havolasi http:// yoki https:// bilan boshlanishi kerak. Qayta yuboring:",
        "barcode_taken": "Bu shtrix-kod boshqa kitobda ishlatilgan.",
        "book_total": "Nechta nusxa bor? 1 dan 1000 gacha son kiriting:",
        "book_count_error": "Nusxalar sonini 1 dan 1000 gacha butun son bilan kiriting.",
        "book_added": "✅ “{title}” kitobi qo'shildi. PDF: {pdf_status}. Ulangan guruhga e'lon yuborildi.",
        "book_added_no_group": "✅ “{title}” kitobi qo'shildi. PDF: {pdf_status}. Guruh e'loni uchun avval guruhni saytdan ulang.",
        "book_added_no_delivery": "✅ “{title}” kitobi qo'shildi. PDF: {pdf_status}, ammo guruhga e'lon yetmadi. Bot guruhda yozish huquqiga ega ekanini tekshiring.",
        "pdf_yes": "mavjud",
        "pdf_no": "mavjud emas",
        "pdf_reading": "📖 Kitob PDF'i. Mutolaa yoqimli bo'lsin! To'liq o'qish uchun kitobni olib keting",
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
        "due_reminder": "⏰ “{title}” kitobini ertaga ({due}) qaytarish vaqti keladi.",
        "overdue_reminder": "⚠️ “{title}” kitobini qaytarish muddati o'tdi. Iltimos, kutubxonaga qaytaring.",
        "site_lend_notice": "📗 Kutubxonachidan “{title}” kitobini oldingiz.\nQaytarish muddati: {due}",
        "my_results": "📚 Natijam",
        "reading_now": "📖 Hozir o'qiyotgan kitoblaringiz:",
        "no_reading": "Hozircha o'qiyotgan kitobingiz yo'q.",
        "book_history": "📕 Qaytarilgan kitoblaringiz:",
        "no_book_history": "Hozircha qaytarilgan kitoblaringiz yo'q.",
        "no_results": "Hozircha olingan kitoblaringiz yo'q.",
        "reading_status": "O'qiyapti",
        "lent_by_site": "Kitob berildi, Telegramga xabar yuborildi.",
        "lent_unlinked": "Kitob berildi, lekin shu ism va sinf bilan botda ro'yxatdan o'tgan o'quvchi topilmadi.",
        "lent_ambiguous": "Kitob berildi, lekin shu ism va sinf bir nechta Telegram akkauntiga mos. Xabar yuborilmadi.",
        "lent_delivery_failed": "Kitob berildi, lekin Telegramga xabar yuborilmadi.",
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
        "books_button": "📚 Выбрать книгу",
        "student_menu_button": "🎓 Меню ученика",
        "parent_menu_button": "👨‍👩‍👧 Меню родителя",
        "librarian_menu_button": "📚 Меню библиотекаря",
        "quick_menu_hint": "Нажмите кнопку ниже, чтобы открыть раздел.",
        "student_profile_button": "👤 Ввести имя и класс",
        "parent_reminder_button": "👨‍👩‍👧 Напоминания родителю",
        "parent_reminder_enabled": "✅ Напоминания родителю включены",
        "parent_reminder_disabled": "🔕 Напоминания родителю выключены",
        "parent_reminder_changed": "Настройка напоминаний для родителя изменена.",
        "profile_prompt": "Введите имя, фамилию и класс через запятую (например: Aziza Karimova, 7-A):",
        "profile_saved": "✅ Имя и класс сохранены.",
        "parent_code_button": "🔗 Код для родителя",
        "parent_add": "➕ Добавить ребёнка", "parent_list": "👨‍👩‍👧 Мои дети и книги",
        "librarian_menu": "📚 Раздел библиотекаря", "add_book_button": "➕ Добавить книгу",
        "book_missing": "В этой библиотеке пока нет книг.", "book_list": "Выберите книгу. Бронь действует 2 дня:",
        "not_started": "Сначала отправьте /start.", "no_libraries": "Войдите как библиотекарь: /librarian КОД",
        "use_private": "Отправьте эту команду в личном чате с ботом.",
        "code": "Код неверен, просрочен или уже использован. Создайте новый на сайте.",
        "linked": "✅ Аккаунт библиотекаря подключён. Книга: /addbook, новость: /news.",
        "already_linked": "Этот Telegram-аккаунт уже подключён к другому библиотекарю.",
        "need_link": "Сначала создайте одноразовый код в своём аккаунте на сайте и отправьте /librarian КОД.",
        "link_help": "Откройте ссылку «Подключить Telegram-группу» на сайте, добавьте бота в группу администратором с правом отправки сообщений, затем откройте ссылку. Или отправьте в группе /linklibrary КОД.",
        "group_only": "Отправьте эту команду в библиотечной группе.",
        "admin_only": "Для подключения группы нужно быть её администратором.",
        "group_linked": "✅ Группа подключена к библиотеке. Книги и новости будут публиковаться здесь.",
        "group_linked_books": "✅ Группа подключена. Объявления о существующих книгах отправлены: {sent}/{total}.",
        "group_linked_books_failed": "✅ Группа подключена. Объявления о существующих книгах отправлены: {sent}/{total}; не доставлено: {failed}. Повторите отправку в разделе Telegram на сайте.",
        "group_code": "Код группы неверен или просрочен. Создайте новый на сайте.",
        "book_title": "Отправьте название книги:",
        "book_author": "Отправьте автора (если неизвестен, отправьте дефис -):",
        "book_genre": "Отправьте жанр (если неизвестен, отправьте дефис -):",
        "book_description": "Кратко опишите книгу (или отправьте -):",
        "book_barcode": "Отправьте штрих-код/QR-код книги (или -):",
        "book_cover": "Отправьте URL обложки (или -):",
        "book_pdf": "Отправьте PDF-файл книги (или -):",
        "cover_link": "🖼 Посмотреть обложку",
        "book_description_error": "Описание должно быть не длиннее 1000 символов. Повторите:",
        "book_barcode_error": "Код не должен быть длиннее 80 символов. Повторите:",
        "book_cover_error": "Ссылка на обложку должна начинаться с http:// или https://. Повторите:",
        "barcode_taken": "Этот штрих-код уже используется другой книгой.",
        "book_total": "Сколько экземпляров? Введите число от 1 до 1000:",
        "book_count_error": "Введите целое число экземпляров от 1 до 1000.",
        "book_added": "✅ Книга «{title}» добавлена. PDF: {pdf_status}. Объявление отправлено в подключённую группу.",
        "book_added_no_group": "✅ Книга «{title}» добавлена. PDF: {pdf_status}. Сначала подключите группу на сайте.",
        "book_added_no_delivery": "✅ Книга «{title}» добавлена. PDF: {pdf_status}, но объявление не доставлено. Проверьте право бота писать в группу.",
        "pdf_yes": "есть",
        "pdf_no": "нет",
        "pdf_reading": "📖 PDF книги. Приятного чтения!",
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
        "due_reminder": "⏰ Книгу «{title}» нужно вернуть завтра ({due}).",
        "overdue_reminder": "⚠️ Срок возврата книги «{title}» истёк. Пожалуйста, верните её в библиотеку.",
        "site_lend_notice": "📗 Вы получили у библиотекаря книгу «{title}».\nВернуть до: {due}",
        "my_results": "📚 Мои результаты",
        "reading_now": "📖 Сейчас вы читаете:",
        "no_reading": "Сейчас у вас нет книг на руках.",
        "book_history": "📕 Возвращённые вами книги:",
        "no_book_history": "Пока нет возвращённых вами книг.",
        "no_results": "Пока вы не получали книги.",
        "reading_status": "Читаете",
        "lent_by_site": "Книга выдана, уведомление отправлено в Telegram.",
        "lent_unlinked": "Книга выдана, но ученик с таким именем и классом не найден в боте.",
        "lent_ambiguous": "Книга выдана, но имя и класс подходят нескольким аккаунтам. Уведомление не отправлено.",
        "lent_delivery_failed": "Книга выдана, но уведомление в Telegram не отправлено.",
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
        "books_button": "📚 Browse books",
        "student_menu_button": "🎓 Student menu",
        "parent_menu_button": "👨‍👩‍👧 Parent menu",
        "librarian_menu_button": "📚 Librarian menu",
        "quick_menu_hint": "Use the button below to reopen your section.",
        "student_profile_button": "👤 Enter name and class",
        "parent_reminder_button": "👨‍👩‍👧 Parent reminders",
        "parent_reminder_enabled": "✅ Parent reminders are on",
        "parent_reminder_disabled": "🔕 Parent reminders are off",
        "parent_reminder_changed": "Parent reminder setting updated.",
        "profile_prompt": "Enter your full name and class separated by a comma (for example: Aziza Karimova, 7-A):",
        "profile_saved": "✅ Name and class saved.",
        "parent_code_button": "🔗 Parent linking code",
        "parent_add": "➕ Add a child", "parent_list": "👨‍👩‍👧 My children and books",
        "librarian_menu": "📚 Librarian menu", "add_book_button": "➕ Add a book",
        "book_missing": "There are no books in this library yet.",
        "book_list": "Choose a book. Reservations are held for 2 days:",
        "not_started": "Please send /start first.", "no_libraries": "Sign in as a librarian: /librarian CODE",
        "use_private": "Send this command in your private chat with the bot.",
        "code": "The code is invalid, expired, or already used. Generate a new one on the website.",
        "linked": "✅ Librarian account linked. Send /addbook for a book or /news for an announcement.",
        "already_linked": "This Telegram account is linked to another librarian.",
        "need_link": "Generate a one-time code in your website account, then send /librarian CODE.",
        "link_help": "Open the “Connect Telegram group” link on the website, add the bot as an administrator with permission to post, then open the link. Or send /linklibrary CODE in the group.",
        "group_only": "Send this command in the library group.",
        "admin_only": "You must be a group administrator to connect it.",
        "group_linked": "✅ Group connected to the library. Books and news will be announced here.",
        "group_linked_books": "✅ Group connected. Announcements for existing books sent: {sent}/{total}.",
        "group_linked_books_failed": "✅ Group connected. Announcements for existing books sent: {sent}/{total}; {failed} could not be delivered. Retry failed announcements in the Telegram tab on the website.",
        "group_code": "The group code is invalid or expired. Generate a new one on the website.",
        "book_title": "Send the book title:",         "book_author": "Send the author (send a dash - if unknown):",
        "book_genre": "Send the genre (send a dash - if unknown):",
        "book_description": "Send a short description (or send -):",
        "book_barcode": "Send the book barcode/QR text (or -):",
        "book_cover": "Send the cover image URL (or -):",
        "book_pdf": "Send the book PDF file (or -):",
        "cover_link": "🖼 View cover",
        "book_description_error": "The description must be no longer than 1000 characters. Try again:",
        "book_barcode_error": "The code must be no longer than 80 characters. Try again:",
        "book_cover_error": "The cover link must start with http:// or https://. Try again:",
        "barcode_taken": "This barcode is already assigned to another book.",
        "book_total": "How many copies? Enter a number from 1 to 1000:",
        "book_count_error": "Enter a whole number of copies from 1 to 1000.",
        "book_added": "✅ “{title}” was added. PDF: {pdf_status}. An announcement was sent to the connected group.",
        "book_added_no_group": "✅ “{title}” was added. PDF: {pdf_status}. Connect a group on the website to publish announcements.",
        "book_added_no_delivery": "✅ “{title}” was added. PDF: {pdf_status}, but the group announcement was not delivered. Check that the bot can post in the group.",
        "pdf_yes": "available",
        "pdf_no": "not available",
        "pdf_reading": "📖 Book PDF. Enjoy reading!",
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
        "due_reminder": "⏰ “{title}” is due tomorrow ({due}).",
        "overdue_reminder": "⚠️ “{title}” is overdue. Please return it to the library.",
        "site_lend_notice": "📗 You borrowed “{title}” from the librarian.\nReturn by: {due}",
        "my_results": "📚 My results",
        "reading_now": "📖 Currently reading:",
        "no_reading": "You have no books checked out right now.",
        "book_history": "📕 Books you returned:",
        "no_book_history": "You have not returned any books yet.",
        "no_results": "You have not borrowed any books yet.",
        "reading_status": "Reading",
        "lent_by_site": "Book lent; Telegram notification sent.",
        "lent_unlinked": "Book lent, but no bot user was found with this name and class.",
        "lent_ambiguous": "Book lent, but this name and class match multiple Telegram accounts. No notification was sent.",
        "lent_delivery_failed": "Book lent, but the Telegram notification could not be sent.",
        "returned_notice": "🙏 “{title}” was returned. Thank you!",
        "cancelled_notice": "❌ The reservation for “{title}” was cancelled.",
        "expired_notice": "⌛ The reservation for “{title}” expired. Reserve again if needed.",
        "child_prefix": "Your child: ",
    },
}

BOT_TEXT["uz"].update({
    "add_news_button": "📢 Yangilik e'lon qilish",
    "news_command": "Yangilik matnini yuboring (4096 belgigacha):",
    "news_photo": "Rasm yuboring yoki rasmsiz e'lon qilish uchun - yuboring:",
    "news_message_error": "Yangilik matni bo'sh bo'lmasin va rasm bilan 1024, rasmsiz 4096 belgidan oshmasin. Qayta yuboring:",
    "news_photo_error": "Rasmni yuklab bo'lmadi. Qayta yuboring yoki rasmsiz davom etish uchun - yuboring:",
    "news_preview": "👀 E'lon ko'rinishi:",
    "news_publish": "✅ E'lon qilish",
    "news_cancel": "Bekor qilish",
    "news_preview_pending": "E'lonni yuborish yoki bekor qilish uchun quyidagi tugmalardan foydalaning.",
    "news_no_groups": "E'lon yuborilmadi: avval guruhni sayt orqali ulang.",
    "news_sent": "✅ Yangilik {sent}/{total} ta ulangan guruhga yuborildi.",
    "news_partial": "⚠️ Yangilik {sent}/{total} ta guruhga yuborildi. Qolganlarini saytdagi Telegram bo'limidan qayta yuboring.",
    "draft_active": "Avval joriy kiritishni tugating yoki /cancel buyrug'i bilan bekor qiling.",
    "draft_cancelled": "Kiritish bekor qilindi.",
    "book_cover": "Kitob muqovasini rasm qilib yuboring, URL kiriting yoki bo'lmasa - yuboring:",
    "book_cover_error": "Muqova rasmini yuklab bo'lmadi. Qayta yuboring yoki - yuboring:",
})
BOT_TEXT["ru"].update({
    "add_news_button": "📢 Опубликовать новость",
    "news_command": "Отправьте текст новости (до 4096 символов):",
    "news_photo": "Отправьте изображение или - чтобы опубликовать без него:",
    "news_message_error": "Текст не должен быть пустым и длиннее 1024 символов с фото или 4096 без фото. Повторите:",
    "news_photo_error": "Не удалось загрузить изображение. Повторите или отправьте - без фото:",
    "news_preview": "👀 Предварительный просмотр:",
    "news_publish": "✅ Опубликовать",
    "news_cancel": "Отмена",
    "news_preview_pending": "Используйте кнопки ниже, чтобы опубликовать или отменить объявление.",
    "news_no_groups": "Новость не отправлена: сначала подключите группу на сайте.",
    "news_sent": "✅ Новость отправлена в группы: {sent}/{total}.",
    "news_partial": "⚠️ Новость отправлена в группы: {sent}/{total}. Остальные можно повторить в разделе Telegram на сайте.",
    "draft_cancelled": "Ввод отменён.",
    "draft_active": "Сначала завершите текущий ввод или отмените его командой /cancel.",
    "book_cover": "Отправьте изображение обложки, URL или - если её нет:",
    "book_cover_error": "Не удалось загрузить обложку. Повторите или отправьте -:",
})
BOT_TEXT["en"].update({
    "add_news_button": "📢 Publish a news post",
    "news_command": "Send the news text (up to 4096 characters):",
    "news_photo": "Send an image, or send - to publish without one:",
    "news_message_error": "Text must not be empty or exceed 1024 characters with an image, or 4096 without one. Try again:",
    "news_photo_error": "Could not download the image. Try again or send - to continue without one:",
    "news_preview": "👀 Announcement preview:",
    "news_publish": "✅ Publish",
    "news_cancel": "Cancel",
    "news_preview_pending": "Use the buttons below to publish or cancel the announcement.",
    "news_no_groups": "News was not sent: connect a group on the website first.",
    "news_sent": "✅ News sent to {sent}/{total} connected groups.",
    "news_partial": "⚠️ News sent to {sent}/{total} groups. Retry the rest in the website's Telegram section.",
    "draft_cancelled": "Entry cancelled.",
    "draft_active": "Finish the current entry first, or cancel it with /cancel.",
    "book_cover": "Send a cover image, a URL, or - if there is no cover:",
    "book_cover_error": "Could not download the cover. Try again or send -:",
})

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

def persistent_menu_keyboard(tg_id, button_key):
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=text(tg_id, button_key))]],
        resize_keyboard=True,
        is_persistent=True,
    )

def actor_id(m: Message):
    return m.chat.id if m.chat.type == "private" else m.from_user.id

async def notify(tg, text, school_id=None):
    if school_id is None:
        student = one("select school_id from students where tg_id=?", (tg,))
        school_id = student["school_id"] if student else None
        if school_id is None:
            parent = one(
                "select s.school_id from parent_children pc "
                "join students s on s.tg_id=pc.student_tg "
                "where pc.parent_tg=? order by s.school_id limit 1", (tg,))
            school_id = parent["school_id"] if parent else None
    if school_id is None:
        log.error("Telegram xabari uchun maktab topilmadi (tg_id=%s)", tg)
        return False
    return await send_tracked_notification(school_id, tg, text, "direct")

async def deliver_telegram_message(
        recipient_tg, message, parse_mode=None, button_text=None, button_url=None,
        photo_path=None):
    if not bot:
        raise RuntimeError("Telegram bot sozlanmagan")
    markup = None
    if button_text and button_url:
        markup = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text=button_text, url=button_url)
        ]])
    if photo_path:
        await bot.send_photo(
            recipient_tg, FSInputFile(photo_path), caption=message,
            parse_mode=parse_mode, reply_markup=markup)
    else:
        await bot.send_message(
            recipient_tg, message, parse_mode=parse_mode, reply_markup=markup)

async def send_tracked_notification(
        school_id, recipient_tg, message, kind, parse_mode=None,
        button_text=None, button_url=None, photo_path=None,
        news_announcement_id=None):
    now = int(time.time())
    try:
        await deliver_telegram_message(
            recipient_tg, message, parse_mode, button_text, button_url, photo_path)
    except Exception as e:
        q(
            "insert into telegram_notifications(school_id,recipient_tg,message,kind,"
            "parse_mode,button_text,button_url,photo_path,news_announcement_id,"
            "status,attempts,last_error,created,updated) "
            "values(?,?,?,?,?,?,?,?,?,'failed',1,?,?,?)",
            (school_id, recipient_tg, message, kind, parse_mode, button_text,
             button_url, photo_path, news_announcement_id, str(e)[:1000], now, now))
        log.warning("Telegram xabari yuborilmadi (tg_id=%s): %s", recipient_tg, e)
        return False
    return True

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
        [InlineKeyboardButton(text=text(tg_id, "my_results"), callback_data="student:result")],
        [InlineKeyboardButton(text=text(tg_id, "student_profile_button"), callback_data="student:profile")],
        [InlineKeyboardButton(
            text=text(tg_id, "parent_reminder_enabled" if (
                one("select notify_parents from students where tg_id=?", (tg_id,)) or {}
            ).get("notify_parents", True) else "parent_reminder_disabled"),
            callback_data="student:parent-reminders")],
        [InlineKeyboardButton(text=text(tg_id, "parent_code_button"), callback_data="student:code")],
    ]))
    await m.answer(
        text(tg_id, "quick_menu_hint"),
        reply_markup=persistent_menu_keyboard(tg_id, "student_menu_button"))

async def show_student_results(m: Message, tg_id: int, offset: int = 0):
    st = one("select 1 from students where tg_id=? and role='student'", (tg_id,))
    if not st:
        return await m.answer(text(tg_id, "not_started"))
    reading = rows(
        "select r.*, b.title from res r join books b on b.id=r.book_id "
        "where r.tg_id=? and r.st='olindi' order by r.id desc", (tg_id,))
    total = one(
        "select count(*) c from res where tg_id=? and st='qaytarildi'", (tg_id,))["c"]
    history = rows(
        "select r.*, b.title from res r join books b on b.id=r.book_id "
        "where r.tg_id=? and r.st='qaytarildi' order by r.id desc limit 10 offset ?",
        (tg_id, offset))
    if not reading and not total:
        return await m.answer(text(tg_id, "no_results"))
    parts = [text(tg_id, "my_results"), "\n" + text(tg_id, "reading_now")]
    parts.extend(
        f"• {x['title'][:120]} — {text(tg_id, 'reading_status')}"
        f"{' (' + fmt(x['due']) + ')' if x.get('due') else ''}"
        for x in reading
    )
    if not reading:
        parts.append(text(tg_id, "no_reading"))
    parts.append("\n" + text(tg_id, "book_history"))
    parts.extend(f"• {x['title'][:120]} — {fmt(x['created'])}" for x in history)
    if not history:
        parts.append(text(tg_id, "no_book_history"))
    pages = []
    if offset:
        pages.append(InlineKeyboardButton(
            text=text(tg_id, "newer"),
            callback_data=f"student:result:{max(0, offset - 10)}"))
    if offset + len(history) < total:
        pages.append(InlineKeyboardButton(
            text=text(tg_id, "older"),
            callback_data=f"student:result:{offset + 10}"))
    await m.answer("\n".join(parts), reply_markup=kb([pages]) if pages else None)

async def show_parent_menu(m: Message):
    tg_id = actor_id(m)
    await m.answer(text(tg_id, "parent_menu"), reply_markup=kb([
        [InlineKeyboardButton(text=text(tg_id, "parent_add"), callback_data="parent:add")],
        [InlineKeyboardButton(text=text(tg_id, "parent_list"), callback_data="parent:list")],
    ]))
    await m.answer(
        text(tg_id, "quick_menu_hint"),
        reply_markup=persistent_menu_keyboard(tg_id, "parent_menu_button"))

async def show_books(m: Message, school_id):
    tg_id = actor_id(m)
    books = rows("select * from books where school_id=?", (school_id,))
    if not books:
        return await m.answer(text(tg_id, "book_missing"))
    available_label = text(tg_id, "available").lower()
    await m.answer(text(tg_id, "book_list"), reply_markup=kb(
        [[InlineKeyboardButton(
            text=f"{b['title'][:30]} — {(b.get('author') or '—')[:20]} ({avail(b)}/{b['total']} {available_label})",
            callback_data=f"b:{b['id']}")] for b in books]))

async def show_book_details(m: Message, tg_id: int, book_id: int):
    book = one("select * from books where id=?", (book_id,))
    student = one("select school_id from students where tg_id=? and role='student'",
                  (tg_id,))
    if not book or not student or student["school_id"] != book["school_id"]:
        return await m.answer(text(tg_id, "bad_book"))
    details = (
        f"📖 <b>{escape(book['title'])}</b>\n"
        f"✍️ {escape(book.get('author') or '—')}\n"
        f"🎭 {escape(book.get('genre') or '—')}\n"
        f"📝 {escape(book.get('description') or '—')}"
    )
    if book.get("cover_url"):
        cover_path = Path("static") / Path(book["cover_url"]).name
        if cover_path.is_file():
            await m.answer_photo(FSInputFile(str(cover_path)))
        if book["cover_url"].startswith(("https://", "http://")):
            details += (
                f'\n<a href="{escape(book["cover_url"])}">'
                f'{escape(text(tg_id, "cover_link"))}</a>')
    await m.answer(
        details, parse_mode="HTML",
        reply_markup=kb([[
            InlineKeyboardButton(
                text=text(tg_id, "reserve"), callback_data=f"reserve-book:{book_id}")
        ]]))

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
    if b.get("pdf_url"):
        pdf_path = Path("static") / "uploads" / Path(b["pdf_url"]).name
        if pdf_path.is_file():
            try:
                await m.answer_document(
                    FSInputFile(str(pdf_path)), caption=text(tg, "pdf_reading"))
            except Exception:
                log.exception("Kitob PDF yuborilmadi (book_id=%s, tg_id=%s)", book_id, tg)
    parent_notices = {
        "uz": f"📚 Farzandingiz {name} ({cls}) “{b['title']}” kitobini band qildi.",
        "ru": f"📚 Ваш ребёнок {name} ({cls}) забронировал книгу «{b['title']}».",
        "en": f"📚 Your child {name} ({cls}) reserved “{b['title']}”.",
    }
    await notify_parents(tg, parent_notices, school_id=st["school_id"])

async def ask_days(m: Message, tg, book_id):
    book = one("select school_id from books where id=?", (book_id,))
    if not book:
        return await m.answer(text(tg, "bad_book"))
    q(
        "insert into students(tg_id,school_id,role,state,want) "
        "values(?,?,'student','days',?) on conflict(tg_id) do update set "
        "school_id=excluded.school_id, state='days', want=excluded.want, "
        "requested_days=null",
        (tg, book["school_id"], book_id))
    await m.answer(text(tg, "reserve_days"))

async def notify_parents(student_tg, content, school_id=None):
    parents = rows("select parent_tg from parent_children where student_tg=?", (student_tg,))
    if school_id is None:
        student = one("select school_id from students where tg_id=?", (student_tg,))
        school_id = student["school_id"] if student else None
    delivered = False
    for parent in parents:
        parent_id = parent["parent_tg"]
        message = (content.get(user_language(parent_id), content.get("uz"))
                   if isinstance(content, dict) else content)
        delivered = await send_tracked_notification(
            school_id, parent_id, message, "parent") or delivered
    return delivered

def create_news_announcement(
        school_id, librarian_id, message, photo_path, group_ids, scheduled_at):
    linked_groups = rows(
        "select telegram_group_id from library_groups where school_id=?",
        (school_id,))
    linked_ids = {group["telegram_group_id"] for group in linked_groups}
    targets = linked_ids if group_ids is None else set(group_ids)
    if not targets or not targets.issubset(linked_ids):
        raise ValueError("Yangilik uchun ulangan guruhlarni tanlang")
    now = int(time.time())
    status = "scheduled" if scheduled_at and scheduled_at > now else "queued"
    with pool.connection() as conn:
        with conn.transaction():
            announcement = conn.execute(
                _sql(
                    "insert into news_announcements(school_id,librarian_id,message,"
                    "photo_path,scheduled_at,status,created,updated) "
                    "values(?,?,?,?,?,?,?,?) returning id"),
                (school_id, librarian_id, message, photo_path, scheduled_at,
                 status, now, now),
            ).fetchone()
            for group_id in sorted(targets):
                conn.execute(
                    _sql(
                        "insert into news_announcement_groups(announcement_id,group_id) "
                        "values(?,?)"),
                    (announcement["id"], group_id),
                )
    return announcement["id"], len(targets)

async def deliver_news_announcement(announcement_id):
    now = int(time.time())
    announcement = one(
        "update news_announcements set status='sending', updated=? "
        "where id=? and status in ('queued','scheduled') returning *",
        (now, announcement_id))
    if not announcement:
        current = one(
            "select status from news_announcements where id=?",
            (announcement_id,))
        groups = rows(
            "select group_id from news_announcement_groups where announcement_id=?",
            (announcement_id,))
        return 0, len(groups), current["status"] if current else "missing"
    groups = rows(
        "select group_id from news_announcement_groups "
        "where announcement_id=? and status='pending' order by group_id",
        (announcement_id,))
    for group in groups:
        delivered = await send_tracked_notification(
            announcement["school_id"], group["group_id"],
            announcement["message"], "news_announcement",
            photo_path=announcement.get("photo_path"),
            news_announcement_id=announcement_id)
        q(
            "update news_announcement_groups set status=? "
            "where announcement_id=? and group_id=?",
            ("sent" if delivered else "failed", announcement_id, group["group_id"]))
        if delivered:
            sent_at = int(time.time())
            q(
                "update telegram_notifications set status='sent', last_error='', "
                "updated=?, sent_at=? where news_announcement_id=? "
                "and recipient_tg=? and status='failed'",
                (sent_at, sent_at, announcement_id, group["group_id"]))
    return refresh_news_announcement_status(announcement_id)

def refresh_news_announcement_status(announcement_id):
    totals = one(
        "select count(*) total, "
        "count(*) filter (where status='sent') sent from "
        "news_announcement_groups where announcement_id=?",
        (announcement_id,))
    sent, total = totals["sent"], totals["total"]
    status = "sent" if sent == total else "partial" if sent else "failed"
    finished_at = int(time.time())
    q(
        "update news_announcements set status=?, updated=?, sent_at=? where id=?",
        (status, finished_at, finished_at if sent else None, announcement_id))
    return sent, total, status

async def dispatch_scheduled_news():
    now = int(time.time())
    q(
        "update news_announcements set status=case when scheduled_at is null "
        "then 'queued' else 'scheduled' end, updated=? "
        "where status='sending' and updated<=?",
        (now, now - 300))
    due = rows(
        "select id from news_announcements where "
        "(status='queued' and updated<=?) or "
        "(status='scheduled' and scheduled_at<=?) "
        "order by coalesce(scheduled_at, created) limit 20",
        (now - 30, now))
    for announcement in due:
        await deliver_news_announcement(announcement["id"])

async def news_scheduler():
    while True:
        try:
            await dispatch_scheduled_news()
        except Exception:
            log.exception("Rejalashtirilgan yangiliklarni yuborishda xato")
        await asyncio.sleep(15)

async def show_librarian_menu(m: Message):
    tg_id = actor_id(m)
    await m.answer(text(tg_id, "librarian_menu"), reply_markup=kb([
        [InlineKeyboardButton(text=text(tg_id, "add_book_button"), callback_data="librarian:addbook")],
        [InlineKeyboardButton(text=text(tg_id, "add_news_button"), callback_data="librarian:news")],
    ]))
    await m.answer(
        text(tg_id, "quick_menu_hint"),
        reply_markup=persistent_menu_keyboard(tg_id, "librarian_menu_button"))

async def publish_book(book, group_ids=None):
    school = one("select name from schools where id=?", (book["school_id"],))
    groups = (
        rows(
            "select telegram_group_id from library_groups where school_id=?",
            (book["school_id"],))
        if group_ids is None
        else [{"telegram_group_id": group_id} for group_id in group_ids]
    )
    if not bot or not school or not groups:
        return False
    try:
        bot_info = await bot.get_me()
        if not bot_info.username:
            log.error("Telegram bot username sozlanmagan; kitob e'loni yuborilmadi.")
            return False
        link = f"https://t.me/{bot_info.username}?start=reserve_{book['id']}"
        available = max(avail(book), 0)
        safe_title = escape(book["title"])
        safe_author = escape(book.get("author") or "—")
        safe_genre = escape(book.get("genre") or "—")
        safe_school = escape(school["name"])
        safe_description = escape(book.get("description") or "")
        message = (
            "📚 <b>YANGI KITOB KUTUBXONADA!</b>\n"
            "🇺🇿 O'zbekcha\n"
            f"📖 <b>{safe_title}</b>\n✍️ Muallif: {safe_author}\n"
            f"🎭 Janr: {safe_genre}\n📦 Mavjud: {available}/{book['total']} nusxa\n"
            f"📄 PDF: {'mavjud' if book.get('pdf_url') else 'mavjud emas'}\n"
            f"{'📝 ' + safe_description + chr(10) if safe_description else ''}"
            f"🏫 {safe_school}\n\n"
            "📚 <b>НОВАЯ КНИГА В БИБЛИОТЕКЕ!</b>\n"
            f"📖 <b>{safe_title}</b>\n✍️ Автор: {safe_author}\n"
            f"🎭 Жанр: {safe_genre}\n📦 В наличии: {available}/{book['total']} экз.\n"
            f"📄 PDF: {'есть' if book.get('pdf_url') else 'нет'}\n"
            f"{'📝 ' + safe_description + chr(10) if safe_description else ''}"
            f"🏫 {safe_school}\n\n"
            "📚 <b>NEW BOOK AT THE LIBRARY!</b>\n"
            f"📖 <b>{safe_title}</b>\n✍️ Author: {safe_author}\n"
            f"🎭 Genre: {safe_genre}\n📦 Available: {available}/{book['total']} copies\n"
            f"📄 PDF: {'available' if book.get('pdf_url') else 'not available'}\n"
            f"{'📝 ' + safe_description + chr(10) if safe_description else ''}"
            f"🏫 {safe_school}\n\n"
            "📲 Bot orqali oldindan band qiling / Бронируйте в боте / Reserve in the bot"
        )
        cover_path = None
        cover_url = book.get("cover_url") or ""
        if cover_url.startswith("/static/uploads/"):
            candidate = Path("static") / "uploads" / Path(cover_url).name
            if candidate.is_file():
                cover_path = str(candidate)
        if cover_path:
            description = (book.get("description") or "").strip()
            message = (
                "📚 YANGI KITOB KUTUBXONADA!\n"
                f"📖 {book['title'][:120]}\n"
                f"✍️ Muallif: {(book.get('author') or '—')[:60]}\n"
                f"🎭 Janr: {(book.get('genre') or '—')[:40]}\n"
                f"📦 Mavjud: {available}/{book['total']} nusxa\n"
                f"📄 PDF: {'mavjud' if book.get('pdf_url') else 'mavjud emas'}\n"
                f"{'📝 ' + description[:80] + ('…' if len(description) > 80 else '') + chr(10) if description else ''}"
                f"🏫 {school['name'][:50]}\n"
                "📲 Band qilish uchun pastdagi tugmani bosing"
            )
            parse_mode = None
        else:
            parse_mode = "HTML"
        sent = 0
        for group in groups:
            delivered = await send_tracked_notification(
                book["school_id"], group["telegram_group_id"], message,
                "book_announcement", parse_mode=parse_mode,
                button_text="📲 Band qilish / Бронь / Reserve", button_url=link,
                photo_path=cover_path)
            if delivered:
                sent += 1
        return sent == len(groups)
    except Exception:
        log.exception("Yangi kitob e'loni yuborilmadi (book_id=%s)", book["id"])
        return False

async def publish_imported_books(books):
    if not books:
        return True
    results = []
    for book in books:
        results.append(await publish_book(book))
    return all(results)

async def publish_existing_books_to_group(school_id, group_id):
    books = rows("select * from books where school_id=? order by id", (school_id,))
    sent = 0
    for book in books:
        if await publish_book(book, [group_id]):
            sent += 1
    return len(books), sent

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
    new_group = one(
        "insert into library_groups(school_id,telegram_group_id,telegram_group_title) "
        "values(?,?,?) on conflict(school_id,telegram_group_id) do nothing "
        "returning telegram_group_id",
        (librarian["school_id"], m.chat.id, m.chat.title or ""))
    if not new_group:
        q("update library_groups set telegram_group_title=? "
          "where school_id=? and telegram_group_id=?",
          (m.chat.title or "", librarian["school_id"], m.chat.id))
        return await m.answer(text(tg_id, "group_linked"))
    total, sent = await publish_existing_books_to_group(
        librarian["school_id"], m.chat.id)
    if total and sent < total:
        return await m.answer(text(
            tg_id, "group_linked_books_failed",
            sent=sent, total=total, failed=total - sent))
    if total:
        return await m.answer(text(
            tg_id, "group_linked_books", sent=sent, total=total))
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
    if one("select 1 from librarian_news_drafts where tg_id=?", (tg_id,)):
        return await m.answer(text(tg_id, "draft_active"))
    librarian = one("select id, school_id from librarians where telegram_tg_id=?",
                    (tg_id,))
    if not librarian:
        return await m.answer(text(tg_id, "no_libraries"))
    q(
        "insert into librarian_book_drafts(tg_id,librarian_id,school_id,current_step) "
        "values(?,?,?,'title') on conflict(tg_id) do update set "
        "librarian_id=excluded.librarian_id, school_id=excluded.school_id, "
        "current_step='title', title=null, author=null, genre=null, "
        "description=null, cover_url=null, barcode=null, pdf_url=null",
        (tg_id, librarian["id"], librarian["school_id"]))
    await m.answer(text(tg_id, "book_title"))

async def begin_news(m: Message, tg_id: int | None = None):
    if m.chat.type != "private":
        return await m.answer(text(m.from_user.id, "use_private"))
    if tg_id is None:
        tg_id = actor_id(m)
    librarian = one("select id, school_id from librarians where telegram_tg_id=?",
                    (tg_id,))
    if not librarian:
        return await m.answer(text(tg_id, "no_libraries"))
    if one("select 1 from librarian_book_drafts where tg_id=?", (tg_id,)):
        return await m.answer(text(tg_id, "draft_active"))
    if not one("select 1 from library_groups where school_id=? limit 1",
               (librarian["school_id"],)):
        return await m.answer(text(tg_id, "news_no_groups"))
    q(
        "insert into librarian_news_drafts(tg_id,librarian_id,school_id,current_step) "
        "values(?,?,?,'message') on conflict(tg_id) do update set "
        "librarian_id=excluded.librarian_id, school_id=excluded.school_id, "
        "current_step='message', message=null, image_path=null",
        (tg_id, librarian["id"], librarian["school_id"]))
    await m.answer(text(tg_id, "news_command"))

def news_preview_keyboard(tg_id):
    return kb([[
        InlineKeyboardButton(
            text=text(tg_id, "news_publish"), callback_data="news:publish"),
        InlineKeyboardButton(
            text=text(tg_id, "news_cancel"), callback_data="news:cancel"),
    ]])

async def finish_news_draft(m: Message, tg_id: int, school_id: int,
                            message: str, photo_path: str | None):
    q(
        "update librarian_news_drafts set current_step='confirm', message=?, "
        "image_path=? where tg_id=? and school_id=?",
        (message, photo_path, tg_id, school_id))
    await m.answer(text(tg_id, "news_preview"))
    if photo_path:
        await m.answer_photo(
            FSInputFile(photo_path), caption=message,
            reply_markup=news_preview_keyboard(tg_id))
    else:
        await m.answer(message, reply_markup=news_preview_keyboard(tg_id))

@dp.callback_query(F.data == "news:publish")
async def confirm_news_publish(c: CallbackQuery):
    tg_id = c.from_user.id
    draft = one(
        "select * from librarian_news_drafts where tg_id=? and current_step='confirm'",
        (tg_id,))
    if not draft:
        await c.answer(text(tg_id, "draft_cancelled"), show_alert=True)
        return
    librarian = one(
        "select id, school_id from librarians where telegram_tg_id=?",
        (tg_id,))
    if (not librarian or librarian["id"] != draft["librarian_id"]
            or librarian["school_id"] != draft["school_id"]):
        q("delete from librarian_news_drafts where tg_id=?", (tg_id,))
        await c.answer(text(tg_id, "no_libraries"), show_alert=True)
        return
    if not one(
            "select 1 from library_groups where school_id=? limit 1",
            (draft["school_id"],)):
        q("delete from librarian_news_drafts where tg_id=?", (tg_id,))
        await c.answer(text(tg_id, "news_no_groups"), show_alert=True)
        return
    announcement_id, _ = create_news_announcement(
        draft["school_id"], draft["librarian_id"], draft["message"],
        draft.get("image_path"), None, None)
    q("delete from librarian_news_drafts where tg_id=?", (tg_id,))
    await c.answer()
    if c.message:
        await c.message.edit_reply_markup(reply_markup=None)
    sent, total, status = await deliver_news_announcement(announcement_id)
    result_key = "news_sent" if status == "sent" else "news_partial"
    if c.message:
        await c.message.answer(
            text(tg_id, result_key, sent=sent, total=total))
        await show_librarian_menu(c.message)

@dp.callback_query(F.data == "news:cancel")
async def cancel_news_publish(c: CallbackQuery):
    q(
        "delete from librarian_news_drafts where tg_id=? and current_step='confirm'",
        (c.from_user.id,))
    await c.answer()
    if c.message:
        await c.message.edit_reply_markup(reply_markup=None)
        await c.message.answer(text(c.from_user.id, "draft_cancelled"))
        await show_librarian_menu(c.message)

@dp.message(Command("addbook"))
async def add_book_command(m: Message):
    await begin_add_book(m)

@dp.message(Command("news"))
async def news_command(m: Message):
    await begin_news(m)

@dp.message(Command("cancel"))
async def cancel_book_draft(m: Message):
    if m.chat.type != "private":
        return await m.answer(text(m.from_user.id, "use_private"))
    q("delete from librarian_book_drafts where tg_id=?", (m.from_user.id,))
    q("delete from librarian_news_drafts where tg_id=?", (m.from_user.id,))
    await m.answer(text(m.from_user.id, "draft_cancelled"))

@dp.callback_query(F.data == "librarian:addbook")
async def add_book_button(c: CallbackQuery):
    await c.answer()
    await begin_add_book(c.message, c.from_user.id)

@dp.callback_query(F.data == "librarian:news")
async def add_news_button(c: CallbackQuery):
    await c.answer()
    await begin_news(c.message, c.from_user.id)

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
    student = one("select role from students where tg_id=?", (m.from_user.id,))
    if student and student["role"] == "student":
        return await show_student_menu(m)
    if student and student["role"] == "parent":
        return await show_parent_menu(m)
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
    if payload.lower().startswith("reserve_"):
        book_id = payload.split("_", 1)[1]
        if not book_id.isdigit() or int(book_id) > 2147483647:
            return await m.answer(text(m.from_user.id, "bad_book"))
        q("update bot_users set language=coalesce(language,'uz') where tg_id=?",
          (m.from_user.id,))
        return await ask_days(m, m.from_user.id, int(book_id))
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

@dp.callback_query(F.data.startswith("student:result"))
async def student_results(c: CallbackQuery):
    parts = c.data.split(":")
    offset = max(int(parts[2]), 0) if len(parts) > 2 else 0
    await c.answer()
    await show_student_results(c.message, c.from_user.id, offset)

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

@dp.callback_query(F.data == "student:profile")
async def student_profile(c: CallbackQuery):
    st = one("select 1 from students where tg_id=? and role='student'", (c.from_user.id,))
    await c.answer()
    if not st:
        return await c.message.answer(text(c.from_user.id, "not_started"))
    q("update students set state='student_profile' where tg_id=?", (c.from_user.id,))
    await c.message.answer(text(c.from_user.id, "profile_prompt"))

@dp.callback_query(F.data == "student:parent-reminders")
async def toggle_parent_reminders(c: CallbackQuery):
    student = one(
        "select notify_parents from students where tg_id=? and role='student'",
        (c.from_user.id,))
    await c.answer()
    if not student:
        return await c.message.answer(text(c.from_user.id, "not_started"))
    enabled = not student["notify_parents"]
    q("update students set notify_parents=? where tg_id=?",
      (enabled, c.from_user.id))
    await c.message.answer(text(c.from_user.id, "parent_reminder_changed"))
    await show_student_menu(c.message)

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
    await show_book_details(c.message, c.from_user.id, int(c.data[2:]))

@dp.callback_query(F.data.startswith("reserve-book:"))
async def reserve_book_details(c: CallbackQuery):
    await c.answer()
    book_id = c.data.split(":", 1)[1]
    if not book_id.isdigit():
        return await c.message.answer(text(c.from_user.id, "bad_book"))
    await ask_days(c.message, c.from_user.id, int(book_id))

async def save_telegram_photo(m: Message) -> Path:
    if not m.photo:
        raise ValueError("Telegram xabarida rasm yo'q")
    photo = m.photo[-1]
    if photo.file_size and photo.file_size > 2_500_000:
        raise ValueError("Rasm 2.5 MB dan kichik bo'lishi kerak")
    directory = Path("static") / "uploads"
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{uuid.uuid4().hex}.jpg"
    try:
        await m.bot.download(photo, destination=path)
        content = path.read_bytes()
        if len(content) > 2_500_000 or not content.startswith(b"\xff\xd8\xff"):
            path.unlink(missing_ok=True)
            raise ValueError("Telegram rasmi noto'g'ri yoki juda katta")
    except ValueError:
        raise
    except Exception:
        path.unlink(missing_ok=True)
        log.exception("Telegram rasmi saqlanmadi (tg_id=%s)", m.from_user.id)
        raise
    return path

@dp.message(F.photo)
async def got_librarian_photo(m: Message):
    if m.chat.type != "private":
        return
    news_draft = one(
        "select * from librarian_news_drafts where tg_id=?",
        (m.from_user.id,))
    if news_draft and news_draft["current_step"] == "photo":
        if len(news_draft["message"].encode("utf-16-le")) // 2 > 1024:
            q(
                "update librarian_news_drafts set current_step='message', message=null "
                "where tg_id=?",
                (m.from_user.id,))
            return await m.answer(text(m.from_user.id, "news_message_error"))
        try:
            path = await save_telegram_photo(m)
        except ValueError:
            return await m.answer(text(m.from_user.id, "news_photo_error"))
        except Exception:
            return await m.answer(text(m.from_user.id, "news_photo_error"))
        return await finish_news_draft(
            m, m.from_user.id, news_draft["school_id"],
            news_draft["message"], str(path))
    if news_draft and news_draft["current_step"] == "confirm":
        return await m.answer(text(m.from_user.id, "news_preview_pending"))
    book_draft = one(
        "select * from librarian_book_drafts where tg_id=?",
        (m.from_user.id,))
    if book_draft and book_draft["current_step"] == "cover":
        try:
            path = await save_telegram_photo(m)
        except ValueError:
            return await m.answer(text(m.from_user.id, "book_cover_error"))
        except Exception:
            return await m.answer(text(m.from_user.id, "book_cover_error"))
        q(
            "update librarian_book_drafts set cover_url=?, current_step='pdf' "
            "where tg_id=?",
            (f"/static/uploads/{path.name}", m.from_user.id))
        return await m.answer(text(m.from_user.id, "book_pdf"))

@dp.message(F.document)
async def got_book_pdf(m: Message):
    if m.chat.type != "private" or not m.document:
        return
    draft = one("select * from librarian_book_drafts where tg_id=?", (m.from_user.id,))
    if not draft or draft["current_step"] != "pdf":
        return
    document = m.document
    if (not (document.file_name or "").lower().endswith(".pdf")
            or document.mime_type not in (None, "application/pdf")):
        return await m.answer(text(m.from_user.id, "book_pdf"))
    if document.file_size and document.file_size > 20_000_000:
        return await m.answer(text(m.from_user.id, "book_pdf"))
    directory = Path("static") / "uploads"
    directory.mkdir(parents=True, exist_ok=True)
    name = f"{uuid.uuid4().hex}.pdf"
    path = directory / name
    try:
        await m.bot.download(document, destination=path)
        if path.stat().st_size > 20_000_000 or not path.read_bytes().startswith(b"%PDF-"):
            path.unlink(missing_ok=True)
            return await m.answer(text(m.from_user.id, "book_pdf"))
    except Exception:
        path.unlink(missing_ok=True)
        log.exception("Kutubxonachi PDF fayli saqlanmadi (tg_id=%s)", m.from_user.id)
        return await m.answer(text(m.from_user.id, "book_pdf"))
    q("update librarian_book_drafts set pdf_url=?, current_step='total' where tg_id=?",
      (f"/static/uploads/{name}", m.from_user.id))
    await m.answer(text(m.from_user.id, "book_total"))

@dp.message(F.text & ~F.text.startswith("/"))
async def got_text(m: Message):
    if m.chat.type == "private":
        news_draft = one(
            "select * from librarian_news_drafts where tg_id=?",
            (m.from_user.id,))
        if news_draft:
            value = (m.text or "").strip()
            if news_draft["current_step"] == "message":
                if not value or len(value.encode("utf-16-le")) // 2 > 4096:
                    return await m.answer(text(m.from_user.id, "news_message_error"))
                q(
                    "update librarian_news_drafts set message=?, current_step='photo' "
                    "where tg_id=?",
                    (value, m.from_user.id))
                return await m.answer(text(m.from_user.id, "news_photo"))
            if news_draft["current_step"] == "photo":
                if value not in ("-", "—"):
                    return await m.answer(text(m.from_user.id, "news_photo"))
                return await finish_news_draft(
                    m, m.from_user.id, news_draft["school_id"],
                    news_draft["message"], None)
            if news_draft["current_step"] == "confirm":
                return await m.answer(text(m.from_user.id, "news_preview_pending"))
    code = (m.text or "").strip().upper()
    if (m.chat.type == "private" and len(code) == 16
            and all(char in "0123456789ABCDEF" for char in code)):
        return await link_librarian_code(m, code)
    if m.chat.type == "private":
        menu_roles = {
            text(m.from_user.id, "student_menu_button"): "student",
            text(m.from_user.id, "parent_menu_button"): "parent",
            text(m.from_user.id, "librarian_menu_button"): "librarian",
        }
        selected_role = menu_roles.get((m.text or "").strip())
        if selected_role == "librarian" and one(
                "select id from librarians where telegram_tg_id=?",
                (m.from_user.id,)):
            return await show_librarian_menu(m)
        if selected_role in ("student", "parent"):
            user = one("select role from students where tg_id=?", (m.from_user.id,))
            if user and user["role"] == selected_role:
                if selected_role == "student":
                    return await show_student_menu(m)
                return await show_parent_menu(m)
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
            q("update librarian_book_drafts set genre=?, current_step='description' where tg_id=?",
              (value, m.from_user.id))
            return await m.answer(text(m.from_user.id, "book_description"))
        if draft["current_step"] == "description":
            value = "" if value in ("-", "—") else value
            if len(value) > 1000:
                return await m.answer(text(m.from_user.id, "book_description_error"))
            q("update librarian_book_drafts set description=?, current_step='barcode' where tg_id=?",
              (value, m.from_user.id))
            return await m.answer(text(m.from_user.id, "book_barcode"))
        if draft["current_step"] == "barcode":
            value = "" if value in ("-", "—") else value
            if len(value) > 80:
                return await m.answer(text(m.from_user.id, "book_barcode_error"))
            q("update librarian_book_drafts set barcode=?, current_step='cover' where tg_id=?",
              (value, m.from_user.id))
            return await m.answer(text(m.from_user.id, "book_cover"))
        if draft["current_step"] == "cover":
            value = "" if value in ("-", "—") else value
            if value and not value.startswith(("https://", "http://")):
                return await m.answer(text(m.from_user.id, "book_cover_error"))
            q("update librarian_book_drafts set cover_url=?, current_step='pdf' where tg_id=?",
              (value, m.from_user.id))
            return await m.answer(text(m.from_user.id, "book_pdf"))
        if draft["current_step"] == "pdf":
            if value not in ("-", "—"):
                return await m.answer(text(m.from_user.id, "book_pdf"))
            q("update librarian_book_drafts set pdf_url='', current_step='total' where tg_id=?",
              (m.from_user.id,))
            return await m.answer(text(m.from_user.id, "book_total"))
        if draft["current_step"] == "total":
            if not value.isdigit() or not 1 <= int(value) <= 1000:
                return await m.answer(text(m.from_user.id, "book_count_error"))
            if draft["barcode"] and one(
                    "select id from books where school_id=? and barcode=?",
                    (draft["school_id"], draft["barcode"])):
                return await m.answer(text(m.from_user.id, "barcode_taken"))
            book = one(
                "insert into books(school_id,title,author,genre,total,description,cover_url,barcode,pdf_url) "
                "values(?,?,?,?,?,?,?,?,?) returning *",
                (draft["school_id"], draft["title"], draft["author"] or "",
                 draft["genre"] or "", int(value), draft["description"] or "",
                 draft["cover_url"] or "", draft["barcode"] or "", draft["pdf_url"] or ""))
            q("delete from librarian_book_drafts where tg_id=?", (m.from_user.id,))
            published = await publish_book(book)
            school = one("select 1 from library_groups where school_id=? limit 1",
                         (draft["school_id"],))
            announce_key = "book_added" if published else (
                "book_added_no_group" if not school else "book_added_no_delivery")
            await m.answer(text(
                m.from_user.id, announce_key, title=book["title"],
                pdf_status=text(m.from_user.id, "pdf_yes" if book["pdf_url"] else "pdf_no")))
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
    if st["state"] == "student_profile" and st["role"] == "student":
        parts = [part.strip() for part in m.text.split(",", 1)]
        if len(parts) != 2 or len(parts[0]) < 3 or not parts[1] or len(parts[0]) > 60 or len(parts[1]) > 10:
            return await m.answer(text(m.from_user.id, "profile_error"))
        q("update students set name=?, cls=?, state=null where tg_id=?",
          (parts[0], parts[1], m.from_user.id))
        await m.answer(text(m.from_user.id, "profile_saved"))
        return await show_student_menu(m)
    if st["state"] == "child_profile" and st["role"] == "student":
        parts = [part.strip() for part in m.text.split(",", 1)]
        if len(parts) != 2 or len(parts[0]) < 3 or not parts[1] or len(parts[0]) > 60 or len(parts[1]) > 10:
            return await m.answer(text(m.from_user.id, "profile_error"))
        q("update students set name=?, cls=?, state=null where tg_id=?",
          (parts[0], parts[1], m.from_user.id))
        return await issue_parent_code(m, m.from_user.id, st["school_id"])
    if st["state"] == "days":
        t = m.text.strip()
        if not t.isdigit() or not 1 <= int(t) <= 60:
            return await m.answer(text(m.from_user.id, "days_error"))
        if st["name"] and st["cls"]:
            return await reserve(
                m, m.from_user.id, st["want"], int(t), st["name"], st["cls"])
        q("update students set requested_days=?, state='identity' where tg_id=?",
          (int(t), m.from_user.id))
        return await m.answer(text(m.from_user.id, "identity"))
    if st["state"] == "identity":
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
            if x["tg_id"]:
                await notify(
                    x["tg_id"],
                    text(x["tg_id"], "expired_notice", title=x["title"]),
                    school_id=x["school_id"])
        upcoming = rows(
            "select r.id, r.school_id, r.tg_id, r.due, r.reminder_pre_sent, "
            "r.reminder_pre_parent_sent, b.title, s.notify_parents "
            "from res r join books b on b.id=r.book_id "
            "left join students s on s.tg_id=r.tg_id "
            "where r.st='olindi' and r.tg_id<>0 and r.due>? and r.due<=? "
            "and (r.reminder_pre_sent is null or "
            "(s.notify_parents is not false and exists "
            "(select 1 from parent_children pc where pc.student_tg=r.tg_id) "
            "and r.reminder_pre_parent_sent is null))",
            (now, now + 86400))
        for x in upcoming:
            message = text(
                x["tg_id"], "due_reminder", title=x["title"], due=fmt(x["due"]))
            if x["reminder_pre_sent"] is None:
                await notify(x["tg_id"], message, school_id=x["school_id"])
                q("update res set reminder_pre_sent=? where id=? and st='olindi'",
                  (now, x["id"]))
            if (x["notify_parents"] is not False
                    and x["reminder_pre_parent_sent"] is None):
                await notify_parents(
                    x["tg_id"],
                    {
                        language: BOT_TEXT[language]["due_reminder"].format(
                            title=x["title"], due=fmt(x["due"]))
                        for language in BOT_TEXT
                    },
                    school_id=x["school_id"])
                q(
                    "update res set reminder_pre_parent_sent=? "
                    "where id=? and st='olindi'",
                    (now, x["id"]))

        local_now = datetime.fromtimestamp(now, TZ)
        today_seven = int(local_now.replace(
            hour=7, minute=0, second=0, microsecond=0).timestamp())
        if now >= today_seven:
            overdue = rows(
                "select r.id, r.school_id, r.tg_id, r.due, "
                "r.reminder_overdue_sent, r.reminder_overdue_parent_sent, "
                "b.title, s.notify_parents from res r "
                "join books b on b.id=r.book_id "
                "left join students s on s.tg_id=r.tg_id "
                "where r.st='olindi' and r.tg_id<>0 and r.due<=? and "
                "(r.reminder_overdue_sent is null or r.reminder_overdue_sent<? "
                "or (s.notify_parents is not false and exists "
                "(select 1 from parent_children pc where pc.student_tg=r.tg_id) "
                "and (r.reminder_overdue_parent_sent is null "
                "or r.reminder_overdue_parent_sent<?)))",
                (today_seven, today_seven, today_seven))
            for x in overdue:
                messages = {
                    language: BOT_TEXT[language]["overdue_reminder"].format(
                        title=x["title"])
                    for language in BOT_TEXT
                }
                if (x["reminder_overdue_sent"] is None
                        or x["reminder_overdue_sent"] < today_seven):
                    await notify(
                        x["tg_id"], messages[user_language(x["tg_id"])],
                        school_id=x["school_id"])
                    q(
                        "update res set reminder_overdue_sent=? "
                        "where id=? and st='olindi'",
                        (now, x["id"]))
                if (x["notify_parents"] is not False
                        and (x["reminder_overdue_parent_sent"] is None
                             or x["reminder_overdue_parent_sent"] < today_seven)):
                    await notify_parents(x["tg_id"], messages, school_id=x["school_id"])
                    q(
                        "update res set reminder_overdue_parent_sent=? "
                        "where id=? and st='olindi'",
                        (now, x["id"]))
        await asyncio.sleep(max(1, 60 - int(time.time()) % 60))

# ---------- Veb sayt API ----------
@asynccontextmanager
async def lifespan(app):
    tasks = [
        asyncio.create_task(expirer()),
        asyncio.create_task(news_scheduler()),
    ]
    if bot:
        tasks.append(asyncio.create_task(dp.start_polling(bot)))
    else:
        print("DIQQAT: .env faylida BOT_TOKEN yo'q, bot ishlamaydi.")
    yield
    for t in tasks:
        t.cancel()
    pool.close()

app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")

class Reg(BaseModel):
    name: str; fam: str; vil: str; tum: str; maktab: str; login: str; parol: str

class Login(BaseModel):
    login: str; parol: str

class DatabaseReset(BaseModel):
    confirmation: str

class BookIn(BaseModel):
    title: str; author: str = ""; genre: str = ""; total: int = 1
    description: str = ""; cover_url: str = ""; barcode: str = ""; pdf_url: str = ""

class NewsIn(BaseModel):
    message: str
    image_url: str = ""
    group_ids: list[int] | None = None
    scheduled_at: str | None = None

class BookUpdate(BaseModel):
    title: str; author: str = ""; genre: str = ""; total: int
    description: str = ""; cover_url: str = ""; barcode: str = ""; pdf_url: str = ""

class CoverUpload(BaseModel):
    data_url: str

def store_cover(data_url: str) -> str:
    try:
        header, encoded = data_url.split(",", 1)
        image = base64.b64decode(encoded, validate=True)
    except (ValueError, binascii.Error) as exc:
        raise HTTPException(400, "Muqova rasmi noto'g'ri") from exc
    if len(image) > 2_500_000:
        raise HTTPException(413, "Muqova rasmi 2.5 MB dan kichik bo'lishi kerak")
    formats = {
        "data:image/jpeg;base64": (b"\xff\xd8\xff", ".jpg"),
        "data:image/png;base64": (b"\x89PNG\r\n\x1a\n", ".png"),
        "data:image/gif;base64": (b"GIF8", ".gif"),
        "data:image/webp;base64": (b"RIFF", ".webp"),
    }
    file_format = formats.get(header.lower())
    if not file_format or not image.startswith(file_format[0]):
        raise HTTPException(400, "Faqat JPEG, PNG, GIF yoki WebP rasmi qabul qilinadi")
    if file_format[1] == ".webp" and image[8:12] != b"WEBP":
        raise HTTPException(400, "WebP rasmi noto'g'ri")
    name = f"{uuid.uuid4().hex}{file_format[1]}"
    directory = Path("static") / "uploads"
    directory.mkdir(parents=True, exist_ok=True)
    (directory / name).write_bytes(image)
    return f"/static/uploads/{name}"

def store_pdf(content: bytes) -> str:
    if len(content) > 20_000_000:
        raise HTTPException(413, "PDF fayl 20 MB dan kichik bo'lishi kerak")
    if not content.startswith(b"%PDF-"):
        raise HTTPException(400, "Faqat to'g'ri PDF fayl qabul qilinadi")
    name = f"{uuid.uuid4().hex}.pdf"
    directory = Path("static") / "uploads"
    directory.mkdir(parents=True, exist_ok=True)
    (directory / name).write_bytes(content)
    return f"/static/uploads/{name}"

class Lend(BaseModel):
    name: str; cls: str = ""; book_id: int; days: int = 14

def me(authorization: str = Header("")):
    u = one("select * from librarians where token=?", (authorization.replace("Bearer ", ""),)) if authorization else None
    if not u:
        raise HTTPException(401, "Kirish kerak")
    return u

@app.post("/api/book-covers")
def upload_book_cover(d: CoverUpload, u=Depends(me)):
    if len(d.data_url) > 3_400_000:
        raise HTTPException(413, "Muqova rasmi 2.5 MB dan kichik bo'lishi kerak")
    return {"cover_url": store_cover(d.data_url)}

@app.post("/api/book-pdfs")
async def upload_book_pdf(file: UploadFile = File(...), u=Depends(me)):
    content = await file.read(20_000_001)
    if len(content) > 20_000_000:
        raise HTTPException(413, "PDF fayl 20 MB dan kichik bo'lishi kerak")
    return {"pdf_url": store_pdf(content)}

@app.api_route("/", methods=["GET", "HEAD"])
def index():
    return FileResponse("static/index.html")

@app.api_route("/admin/reset", methods=["GET", "HEAD"])
def admin_reset_page():
    return FileResponse("static/admin-reset.html")

@app.post("/api/admin/reset-database")
def reset_database(
    d: DatabaseReset,
    admin_key: str = Header("", alias="X-Admin-Key"),
):
    if len(ADMIN_RESET_KEY) < 32:
        raise HTTPException(503, "ADMIN_RESET_KEY sozlanmagan yoki 32 belgidan qisqa")
    if not secrets.compare_digest(admin_key, ADMIN_RESET_KEY):
        raise HTTPException(403, "Admin kaliti xato")
    if d.confirmation != "DELETE ALL DATA":
        raise HTTPException(400, "Tasdiqlash iborasi noto'g'ri")

    tables = (
        "telegram_notifications", "news_announcement_groups", "news_announcements",
        "library_groups", "librarian_news_drafts", "librarian_book_drafts",
        "library_codes", "parent_codes", "parent_children", "res", "pupils",
        "students", "books", "bot_users", "librarians", "schools",
    )
    with pool.connection() as conn:
        with conn.transaction():
            conn.execute(
                "truncate table " + ", ".join(tables) + " restart identity"
            )
    log.warning("Barcha kutubxona ma'lumotlari administrator tomonidan o'chirildi")
    return {"ok": True}

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
    groups = rows(
        "select telegram_group_id, telegram_group_title from library_groups "
        "where school_id=? order by telegram_group_title, telegram_group_id",
        (u["school_id"],))
    return {
        "linked": bool(u.get("telegram_tg_id")),
        "group_linked": bool(groups),
        "groups": groups,
    }

@app.get("/api/telegram/notifications")
def get_telegram_notifications(u=Depends(me)):
    now = int(time.time())
    return rows(
        "select id, recipient_tg, message, kind, status, attempts, last_error, "
        "created, updated, sent_at from telegram_notifications "
        "where school_id=? and (status<>'sending' or updated<=?) order by "
        "case when status='failed' or status='sending' then 0 else 1 end, "
        "created desc limit 100",
        (u["school_id"], now - 120))

@app.post("/api/telegram/notifications/{notification_id}/retry")
async def retry_telegram_notification(notification_id: int, u=Depends(me)):
    now = int(time.time())
    notification = one(
        "update telegram_notifications set status='sending', attempts=attempts+1, "
        "updated=? where id=? and school_id=? and "
        "(status='failed' or (status='sending' and updated<=?)) returning *",
        (now, notification_id, u["school_id"], now - 120))
    if not notification:
        raise HTTPException(404, "Qayta yuboriladigan xabar topilmadi yoki u allaqachon yuborilmoqda")
    try:
        await deliver_telegram_message(
            notification["recipient_tg"], notification["message"],
            notification["parse_mode"], notification["button_text"],
            notification["button_url"], notification.get("photo_path"))
    except Exception as e:
        q(
            "update telegram_notifications set status='failed', last_error=?, "
            "updated=? where id=? and school_id=?",
            (str(e)[:1000], int(time.time()), notification_id, u["school_id"]))
        log.warning("Telegram xabari qayta yuborilmadi (notification_id=%s): %s",
                    notification_id, e)
        return {"ok": True, "status": "failed"}
    q(
        "update telegram_notifications set status='sent', last_error='', "
        "updated=?, sent_at=? where id=? and school_id=?",
        (int(time.time()), int(time.time()), notification_id, u["school_id"]))
    announcement_id = notification.get("news_announcement_id")
    if announcement_id:
        q(
            "update news_announcement_groups set status='sent' "
            "where announcement_id=? and group_id=?",
            (announcement_id, notification["recipient_tg"]))
        refresh_news_announcement_status(announcement_id)
    return {"ok": True, "status": "sent"}

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

@app.get("/api/dashboard")
def get_dashboard(u=Depends(me)):
    now = int(time.time())
    counts = one(
        "select coalesce(sum(total),0) total_copies, count(*) book_titles from books "
        "where school_id=?", (u["school_id"],))
    active = one(
        "select count(*) c from res where school_id=? and "
        "(st='olindi' or (st='band' and until>?))",
        (u["school_id"], now))["c"]
    reserved = one(
        "select count(*) c from res where school_id=? and st='band' and until>?",
        (u["school_id"], now))["c"]
    borrowed = one(
        "select count(*) c from res where school_id=? and st='olindi'",
        (u["school_id"],))["c"]
    overdue = one(
        "select count(*) c from res where school_id=? and st='olindi' and due<?",
        (u["school_id"], now))["c"]
    return {
        "book_titles": counts["book_titles"],
        "total_copies": counts["total_copies"],
        "borrowed": borrowed,
        "available": max(counts["total_copies"] - active, 0),
        "reserved": reserved,
        "overdue": overdue,
    }

def validate_book_fields(title: str, author: str, genre: str, description: str,
                         cover_url: str, barcode: str, pdf_url: str = "") -> tuple[str, str, str, str, str, str, str]:
    values = (title.strip(), author.strip(), genre.strip(), description.strip(),
              cover_url.strip(), barcode.strip(), pdf_url.strip())
    if not values[0]:
        raise HTTPException(400, "Kitob nomini yozing")
    if len(values[0]) > 200 or len(values[1]) > 100 or len(values[2]) > 60:
        raise HTTPException(400, "Nomi 200, muallif 100, janr 60 belgidan oshmasin")
    if len(values[3]) > 1000 or len(values[5]) > 80:
        raise HTTPException(400, "Tavsif 1000, shtrix-kod 80 belgidan oshmasin")
    if values[4] and not values[4].startswith(("https://", "http://", "/static/uploads/")):
        raise HTTPException(400, "Muqova havolasi http:// yoki https:// bilan boshlanishi kerak")
    if values[6] and not values[6].startswith("/static/uploads/"):
        raise HTTPException(400, "PDF fayl noto'g'ri")
    return values

@app.put("/api/books/{book_id}")
def update_book(book_id: int, d: BookUpdate, u=Depends(me)):
    title, author, genre, description, cover_url, barcode, pdf_url = validate_book_fields(
        d.title, d.author, d.genre, d.description, d.cover_url, d.barcode, d.pdf_url)
    if not 1 <= d.total <= 1000:
        raise HTTPException(400, "Nusxalar soni 1 dan 1000 gacha bo'lishi kerak")
    book = one("select id from books where id=? and school_id=?",
               (book_id, u["school_id"]))
    if not book:
        raise HTTPException(404, "Kitob topilmadi")
    now = int(time.time())
    active = one(
        "select count(*) c from res where book_id=? and "
        "(st='olindi' or (st='band' and until>?))", (book_id, now))["c"]
    if d.total < active:
        raise HTTPException(
            400, f"Nusxalar soni hozir o'quvchilardagi {active} ta faol kitobdan kam bo'lmasin")
    duplicate = one(
        "select id from books where school_id=? and barcode=? and barcode<>'' and id<>?",
        (u["school_id"], barcode, book_id))
    if duplicate:
        raise HTTPException(409, "Bu shtrix-kod boshqa kitobda ishlatilgan")
    q("update books set title=?, author=?, genre=?, total=?, description=?, cover_url=?, "
      "barcode=?, pdf_url=? where id=? and school_id=?",
      (title, author, genre, d.total, description, cover_url, barcode, pdf_url,
       book_id, u["school_id"]))
    return {"ok": True}

@app.delete("/api/books/{book_id}")
def delete_book(book_id: int, u=Depends(me)):
    book = one("select id from books where id=? and school_id=?",
               (book_id, u["school_id"]))
    if not book:
        raise HTTPException(404, "Kitob topilmadi")
    if one("select 1 from res where book_id=? limit 1", (book_id,)):
        raise HTTPException(
            409, "Kitob tarixda qatnashgan; o'quvchilar tarixini saqlash uchun uni o'chirib bo'lmaydi")
    q("delete from books where id=? and school_id=?", (book_id, u["school_id"]))
    return {"ok": True}

@app.post("/api/books")
async def add_book(d: BookIn, u=Depends(me)):
    title, author, genre, description, cover_url, barcode, pdf_url = validate_book_fields(
        d.title, d.author, d.genre, d.description, d.cover_url, d.barcode, d.pdf_url)
    if not 1 <= d.total <= 1000:
        raise HTTPException(400, "Nusxalar soni 1 dan 1000 gacha bo'lishi kerak")
    if barcode and one("select id from books where school_id=? and barcode=?",
                       (u["school_id"], barcode)):
        raise HTTPException(409, "Bu shtrix-kod boshqa kitobda ishlatilgan")
    book = one(
        "insert into books(school_id,title,author,genre,total,description,cover_url,barcode,pdf_url) "
        "values(?,?,?,?,?,?,?,?,?) returning *",
        (u["school_id"], title, author, genre, d.total, description, cover_url, barcode, pdf_url))
    announced = await publish_book(book)
    return {"ok": True, "announced": announced}

@app.post("/api/news")
async def publish_news(d: NewsIn, u=Depends(me)):
    message = d.message.strip()
    image_url = d.image_url.strip()
    if not message:
        raise HTTPException(400, "Yangilik matnini kiriting")
    max_length = 1024 if image_url else 4096
    if len(message.encode("utf-16-le")) // 2 > max_length:
        raise HTTPException(
            400, f"Xabar {'rasm bilan' if image_url else ''} {max_length} belgidan oshmasin")
    if image_url:
        if not image_url.startswith("/static/uploads/"):
            raise HTTPException(400, "Yangilik rasmi noto'g'ri")
        image_path = Path("static") / "uploads" / Path(image_url).name
        if not image_path.is_file():
            raise HTTPException(400, "Yangilik rasmi topilmadi")
        photo_path = str(image_path)
    else:
        photo_path = None
    if not bot:
        raise HTTPException(503, "Telegram bot sozlanmagan")
    linked_groups = rows(
        "select telegram_group_id from library_groups where school_id=?",
        (u["school_id"],))
    linked_ids = {group["telegram_group_id"] for group in linked_groups}
    if not linked_ids:
        raise HTTPException(400, "Avval Telegram guruhini ulang")
    if d.group_ids is None:
        group_ids = sorted(linked_ids)
    else:
        group_ids = list(dict.fromkeys(d.group_ids))
        if not group_ids or not set(group_ids).issubset(linked_ids):
            raise HTTPException(400, "Faqat ulangan guruhlarni tanlang")
    scheduled_at = None
    if d.scheduled_at:
        try:
            local_time = datetime.fromisoformat(d.scheduled_at)
        except ValueError as exc:
            raise HTTPException(400, "Rejalashtirilgan vaqt noto'g'ri") from exc
        if local_time.tzinfo is None:
            local_time = local_time.replace(tzinfo=TZ)
        scheduled_at = int(local_time.timestamp())
        if scheduled_at <= int(time.time()):
            raise HTTPException(400, "Rejalashtirilgan vaqt kelajakda bo'lishi kerak")
    announcement_id, total = create_news_announcement(
        u["school_id"], u["id"], message, photo_path, group_ids, scheduled_at)
    if scheduled_at:
        return {
            "id": announcement_id,
            "status": "scheduled",
            "scheduled_at": scheduled_at,
            "total": total,
        }
    sent, total, status = await deliver_news_announcement(announcement_id)
    return {"id": announcement_id, "sent": sent, "total": total, "status": status}

@app.get("/api/news")
def get_news_history(u=Depends(me)):
    history = rows(
        "select n.id, n.message, n.photo_path, n.scheduled_at, n.status, "
        "n.created, n.sent_at, count(g.group_id) total, "
        "count(*) filter (where g.status='sent') sent from news_announcements n "
        "left join news_announcement_groups g on g.announcement_id=n.id "
        "where n.school_id=? group by n.id order by n.created desc limit 100",
        (u["school_id"],))
    for item in history:
        item["image_url"] = (
            f"/static/uploads/{Path(item['photo_path']).name}"
            if item.get("photo_path") else "")
    return history

@app.post("/api/news/{announcement_id}/resend")
async def resend_news(announcement_id: int, u=Depends(me)):
    if one(
            "select 1 from telegram_notifications where news_announcement_id=? "
            "and status='sending' limit 1", (announcement_id,)):
        raise HTTPException(409, "Yangilik xabari qayta yuborilmoqda")
    claimed = one(
        "update news_announcements set status='queued', scheduled_at=null, "
        "updated=? where id=? and school_id=? and status<>'sending' returning id",
        (int(time.time()), announcement_id, u["school_id"]))
    if not claimed:
        exists = one(
            "select 1 from news_announcements where id=? and school_id=?",
            (announcement_id, u["school_id"]))
        if not exists:
            raise HTTPException(404, "Yangilik topilmadi")
        raise HTTPException(409, "Yangilik hozir yuborilmoqda")
    q(
        "update news_announcement_groups set status='pending' "
        "where announcement_id=?",
        (announcement_id,))
    sent, total, status = await deliver_news_announcement(announcement_id)
    return {"id": announcement_id, "sent": sent, "total": total, "status": status}

@app.delete("/api/news/{announcement_id}")
def delete_news(announcement_id: int, u=Depends(me)):
    with pool.connection() as conn:
        with conn.transaction():
            announcement = conn.execute(
                _sql(
                    "select id, status from news_announcements "
                    "where id=? and school_id=? for update"),
                (announcement_id, u["school_id"]),
            ).fetchone()
            if not announcement:
                raise HTTPException(404, "Yangilik topilmadi")
            if announcement["status"] == "sending":
                raise HTTPException(
                    409, "Yuborilayotgan yangilikni hozir o'chirib bo'lmaydi")
            notifications = conn.execute(
                _sql(
                    "select status from telegram_notifications "
                    "where news_announcement_id=? for update"),
                (announcement_id,),
            ).fetchall()
            if any(item["status"] == "sending" for item in notifications):
                raise HTTPException(409, "Yangilik xabari qayta yuborilmoqda")
            conn.execute(
                _sql("delete from telegram_notifications where news_announcement_id=?"),
                (announcement_id,))
            conn.execute(
                _sql("delete from news_announcement_groups where announcement_id=?"),
                (announcement_id,))
            conn.execute(
                _sql("delete from news_announcements where id=? and school_id=?"),
                (announcement_id, u["school_id"]))
    return {"ok": True}

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
    returnable_overdue = r["st"] == "muddati" and r.get("due") is not None
    if new == "qaytarildi":
        valid_status = r["st"] == "olindi" or returnable_overdue
    else:
        valid_status = r["st"] == "band"
    if not valid_status:
        raise HTTPException(409, "Kitobning holati o'zgargan. Ro'yxatni yangilang.")
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
    telegram_status = "unlinked"
    if r["tg_id"]:
        telegram_status = (
            "sent" if await notify(
                r["tg_id"], msg, school_id=r["school_id"]) else "failed")
        await notify_parents(
            r["tg_id"],
            {
                language: BOT_TEXT[language]["child_prefix"] + localized_notices[language]
                for language in BOT_TEXT
            },
            school_id=r["school_id"])
    return {"ok": True, "telegram": telegram_status}

@app.post("/api/lend")
async def lend(d: Lend, u=Depends(me)):
    b = one("select * from books where id=? and school_id=?", (d.book_id, u["school_id"]))
    name, cls = d.name.strip(), d.cls.strip()
    if not b or not name or not cls or len(name) > 60 or len(cls) > 10:
        raise HTTPException(400, "O'quvchi, sinf va kitobni tanlang")
    if avail(b) < 1:
        raise HTTPException(400, "Bu kitob hozir qolmagan")
    now = int(time.time())
    days = min(max(d.days, 1), 60)
    matches = rows(
        "select tg_id from students where school_id=? and role='student' "
        "and lower(trim(name))=lower(?) and upper(trim(cls))=upper(?) "
        "order by tg_id",
        (u["school_id"], name, cls))
    tg_id = matches[0]["tg_id"] if len(matches) == 1 else 0
    due = now + days * 86400
    q(
        "insert into res(school_id,book_id,tg_id,name,created,until,st,days,due,cls) "
        "values(?,?,?,?,?,?,'olindi',?,?,?)",
        (u["school_id"], b["id"], tg_id, name, now, now, days, due, cls))
    if len(matches) > 1:
        return {"ok": True, "telegram": "ambiguous"}
    if not matches:
        return {"ok": True, "telegram": "not_linked"}
    delivered = await notify(
        tg_id, text(tg_id, "site_lend_notice", title=b["title"], due=fmt(due)),
        school_id=u["school_id"])
    return {"ok": True, "telegram": "sent" if delivered else "failed"}


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
    description: str = ""; cover_url: str = ""; barcode: str = ""

class BooksIn(BaseModel):
    items: list[BookRow]

@app.post("/api/books/import")
async def import_books(d: BooksIn, u=Depends(me)):
    sid, skipped = u["school_id"], 0
    existing = rows("select title, author, barcode from books where school_id=?", (sid,))
    have = {(r["title"].lower(), (r["author"] or "").lower())
            for r in existing}
    barcodes = {r["barcode"] for r in existing if r["barcode"]}
    to_add = []
    for b in d.items[:5000]:
        if not b.title.strip():
            continue
        ti, au, genre, description, cover_url, barcode, _ = validate_book_fields(
            b.title, b.author, b.genre, b.description, b.cover_url, b.barcode)
        key = (ti.lower(), au.lower())
        if key in have or (barcode and barcode in barcodes):
            skipped += 1
            continue
        have.add(key)
        if barcode:
            barcodes.add(barcode)
        total = min(max(b.total, 1), 1000)
        to_add.append((sid, ti, au, genre, total, description, cover_url, barcode))
    if not to_add:
        return {"added": 0, "skipped": skipped, "announced": True}
    book_values = ",".join(["(?,?,?,?,?,?,?,?)"] * len(to_add))
    inserted_books = rows(
        "insert into books(school_id,title,author,genre,total,description,cover_url,barcode) values " +
        book_values + " returning *",
        tuple(value for item in to_add for value in item))
    announced = await publish_imported_books(inserted_books)
    return {"added": len(to_add), "skipped": skipped, "announced": announced}
