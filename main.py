import asyncio, logging, os, secrets, time, hashlib
from types import SimpleNamespace
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta

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
create table if not exists librarians(id serial primary key, school_id int, name text, fam text, login text unique, pw text, token text);
create table if not exists books(id serial primary key, school_id int, title text, author text, total int);
create table if not exists students(tg_id bigint primary key, school_id int, name text, pending int, want int, role text, state text, requested_days int, cls text);
create table if not exists res(id serial primary key, school_id int, book_id int, tg_id bigint, name text, created bigint, until bigint, st text, days int, due bigint, cls text);
create table if not exists pupils(id serial primary key, school_id int, name text, cls text, unique(school_id,name,cls));
create table if not exists parent_children(parent_tg bigint, student_tg bigint, primary key(parent_tg, student_tg));
create table if not exists parent_codes(code text primary key, student_tg bigint, school_id int, expires bigint);
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

# ---------- Telegram bot ----------
bot = Bot(TOKEN) if TOKEN else None
dp = Dispatcher()

def kb(btns):
    return InlineKeyboardMarkup(inline_keyboard=btns)

async def notify(tg, text):
    if not bot:
        return
    try:
        await bot.send_message(tg, text)
    except Exception as e:
        log.warning("Telegram xabari yuborilmadi (tg_id=%s): %s", tg, e)

async def show_viloyatlar(m: Message):
    viloyatlar = rows("select distinct vil from schools order by vil")
    if not viloyatlar:
        return await m.answer("Hozircha birorta maktab kutubxonasi ro'yxatdan o'tmagan.")
    await m.answer("Viloyatingizni tanlang:", reply_markup=kb(
        [[InlineKeyboardButton(text=x["vil"], callback_data=f"v:{i}")]
         for i, x in enumerate(viloyatlar)]))

async def show_tumanlar(m: Message, vil: str, vil_index: int):
    tumanlar = rows("select distinct tum from schools where vil=? order by tum", (vil,))
    await m.answer("Tumaningizni tanlang:", reply_markup=kb(
        [[InlineKeyboardButton(text=x["tum"], callback_data=f"d:{vil_index}:{i}")]
         for i, x in enumerate(tumanlar)]))

async def show_maktablar(m: Message, vil: str, tum: str):
    maktablar = rows("select id, name from schools where vil=? and tum=? order by name", (vil, tum))
    await m.answer("Maktabingizni tanlang:", reply_markup=kb(
        [[InlineKeyboardButton(text=x["name"], callback_data=f"s:{x['id']}")]
         for x in maktablar]))

async def show_roles(m: Message):
    await m.answer("Siz kim bo'lasiz?", reply_markup=kb([
        [InlineKeyboardButton(text="O'quvchi", callback_data="role:student")],
        [InlineKeyboardButton(text="Ota-ona", callback_data="role:parent")],
    ]))

async def show_student_menu(m: Message):
    await m.answer("O'quvchi bo'limi:", reply_markup=kb([
        [InlineKeyboardButton(text="📚 Kitob tanlash", callback_data="student:books")],
        [InlineKeyboardButton(text="🔗 Ota-onaga ulanish kodini olish", callback_data="student:code")],
    ]))

async def show_parent_menu(m: Message):
    await m.answer("Ota-ona bo'limi:", reply_markup=kb([
        [InlineKeyboardButton(text="➕ Farzand qo'shish", callback_data="parent:add")],
        [InlineKeyboardButton(text="👨‍👩‍👧 Farzandlarim va kitoblari", callback_data="parent:list")],
    ]))

async def show_books(m: Message, school_id):
    books = rows("select * from books where school_id=?", (school_id,))
    if not books:
        return await m.answer("Bu maktab kutubxonasida hali kitob kiritilmagan.")
    await m.answer("Kitobni tanlang. Band qilingan kitob 2 kun saqlanadi:", reply_markup=kb(
        [[InlineKeyboardButton(text=f"{b['title']} ({avail(b)} ta bor)", callback_data=f"b:{b['id']}")] for b in books]))

async def reserve(m: Message, tg, book_id, days, name, cls):
    st = one("select * from students where tg_id=?", (tg,))
    b = one("select * from books where id=?", (book_id,))
    if not st or not b or b["school_id"] != st["school_id"]:
        return await m.answer("Kitob topilmadi. /start ni bosing.")
    now = int(time.time())
    if one("select count(*) c from res where tg_id=? and st='band' and until>?", (tg, now))["c"] >= 2:
        return await m.answer("Bir vaqtda 2 tadan ortiq kitob band qila olmaysiz.")
    if avail(b) < 1:
        return await m.answer("Afsuski, bu kitob hozir qolmagan.")
    q("insert into res(school_id,book_id,tg_id,name,created,until,st,days,cls) values(?,?,?,?,?,?,'band',?,?)",
      (b["school_id"], b["id"], tg, name, now, now + HOLD, days, cls))
    q("update students set name=?, cls=?, state=null, want=null, requested_days=null where tg_id=?",
      (name, cls, tg))
    await m.answer(f"✅ \"{b['title']}\" band qilindi.\n{fmt(now + HOLD)} gacha kutubxonaga kelib oling.\n"
                   f"Olganingizdan keyin {days} kun ichida qaytarasiz.")
    await notify_parents(tg, f"📚 Farzandingiz {name} ({cls}) \"{b['title']}\" kitobini band qildi.")

async def ask_days(m: Message, tg, book_id):
    q("update students set want=?, state='days', requested_days=null where tg_id=?", (book_id, tg))
    await m.answer("Kitobni necha kunga olmoqchisiz? Kunni raqam bilan yozing (1 dan 60 gacha, masalan: 10):")

async def notify_parents(student_tg, text):
    parents = rows("select parent_tg from parent_children where student_tg=?", (student_tg,))
    for parent in parents:
        await notify(parent["parent_tg"], text)

@dp.message(CommandStart())
async def start(m: Message):
    await show_viloyatlar(m)

@dp.message(Command("maktab"))
async def change_school(m: Message):
    await show_viloyatlar(m)

@dp.message(Command("band"))
async def mine(m: Message):
    r = rows("select r.*, b.title from res r join books b on b.id=r.book_id where r.tg_id=? and r.st='band' and r.until>?",
             (m.from_user.id, int(time.time())))
    await m.answer("\n".join(f"📖 {x['title']} — {fmt(x['until'])} gacha" for x in r) or "Band qilingan kitobingiz yo'q.")

@dp.callback_query(F.data.startswith("v:"))
async def pick_viloyat(c: CallbackQuery):
    vil_index = int(c.data[2:])
    viloyatlar = rows("select distinct vil from schools order by vil")
    await c.answer()
    if vil_index < 0 or vil_index >= len(viloyatlar):
        return await c.message.answer("Viloyat topilmadi. /start ni bosing.")
    await show_tumanlar(c.message, viloyatlar[vil_index]["vil"], vil_index)

@dp.callback_query(F.data.startswith("d:"))
async def pick_tuman(c: CallbackQuery):
    _, vil_index, tum_index = c.data.split(":")
    viloyatlar = rows("select distinct vil from schools order by vil")
    await c.answer()
    vi, ti = int(vil_index), int(tum_index)
    if vi < 0 or vi >= len(viloyatlar):
        return await c.message.answer("Viloyat topilmadi. /start ni bosing.")
    vil = viloyatlar[vi]["vil"]
    tumanlar = rows("select distinct tum from schools where vil=? order by tum", (vil,))
    if ti < 0 or ti >= len(tumanlar):
        return await c.message.answer("Tuman topilmadi. /start ni bosing.")
    await show_maktablar(c.message, vil, tumanlar[ti]["tum"])

@dp.callback_query(F.data.startswith("s:"))
async def pick_school(c: CallbackQuery):
    sid = int(c.data[2:])
    if not one("select 1 from schools where id=?", (sid,)):
        await c.answer("Maktab topilmadi", show_alert=True)
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
    if role not in ("student", "parent") or not st:
        return await c.message.answer("Avval /start ni bosing.")
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
        return await c.message.answer("Avval /start ni bosing.")
    await show_books(c.message, st["school_id"])

@dp.callback_query(F.data == "student:code")
async def student_code(c: CallbackQuery):
    st = one("select * from students where tg_id=? and role='student'", (c.from_user.id,))
    await c.answer()
    if not st:
        return await c.message.answer("Avval /start ni bosing.")
    if not st["name"]:
        q("update students set state='child_profile' where tg_id=?", (c.from_user.id,))
        return await c.message.answer(
            "Farzandingiz ro'yxatda tanilishi uchun ism-familiyasi va sinfini yozing "
            "(masalan: Aziza Karimova, 7-A):")
    await issue_parent_code(c.message, c.from_user.id, st["school_id"])

async def issue_parent_code(m: Message, student_tg, school_id):
    code = secrets.token_hex(4).upper()
    expires = int(time.time()) + 900
    q("delete from parent_codes where student_tg=?", (student_tg,))
    q("insert into parent_codes(code,student_tg,school_id,expires) values(?,?,?,?)",
      (code, student_tg, school_id, expires))
    await m.answer(
        f"🔗 Ota-onangizga shu kodni yuboring: <code>{code}</code>\n"
        "Kod 15 daqiqa ichida bir marta ishlatiladi. Ota-ona botda "
        "\"Farzand qo'shish\" tugmasini bosib kodni kiritsin.",
        parse_mode="HTML")

@dp.callback_query(F.data == "parent:add")
async def parent_add(c: CallbackQuery):
    st = one("select 1 from students where tg_id=? and role='parent'", (c.from_user.id,))
    await c.answer()
    if not st:
        return await c.message.answer("Avval /start ni bosing.")
    q("update students set state='link_code' where tg_id=?", (c.from_user.id,))
    await c.message.answer("Farzandingiz yuborgan 8 belgili ulanish kodini kiriting:")

@dp.callback_query(F.data == "parent:list")
async def parent_list(c: CallbackQuery):
    st = one("select 1 from students where tg_id=? and role='parent'", (c.from_user.id,))
    await c.answer()
    if not st:
        return await c.message.answer("Avval /start ni bosing.")
    children = rows(
        "select s.tg_id, s.name, s.cls, sc.name school from parent_children pc "
        "join students s on s.tg_id=pc.student_tg join schools sc on sc.id=s.school_id "
        "where pc.parent_tg=? order by s.name, s.tg_id", (c.from_user.id,))
    if not children:
        return await c.message.answer("Hali farzand ulanmagan. \"Farzand qo'shish\" tugmasini bosing.")
    await c.message.answer("Kitoblar tarixini ko'rish uchun farzandingizni tanlang:",
                           reply_markup=kb([
                               [InlineKeyboardButton(
                                   text=(f"{x['name'] or 'O‘quvchi'}"
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
        return await c.message.answer("Bu o'quvchi sizga ulanmagan.")
    total = one("select count(*) c from res where tg_id=?", (child_tg,))["c"]
    history = rows(
        "select r.*, b.title from res r join books b on b.id=r.book_id "
        "where r.tg_id=? order by r.id desc limit 10 offset ?", (child_tg, offset))
    if not history:
        return await c.message.answer("Bu farzandingizda hali kitob buyurtmalari yo'q.")
    statuses = {"band": "Band qilingan", "olindi": "O'qiyapti", "qaytarildi": "Qaytarilgan",
                "bekor": "Bekor qilingan", "muddati": "Band muddati tugagan"}
    message = "📚 Farzandingizning kitoblar tarixi:\n\n" + "\n".join(
        f"• {x['title'][:120]} — {statuses.get(x['st'], x['st'])}"
        f"{' (' + x['name'] + ', ' + x['cls'] + ')' if x['cls'] else ''}"
        f"\n  {fmt(x['created'])}" for x in history)
    pages = []
    if offset:
        pages.append(InlineKeyboardButton(text="⬅️ Yangiroq", callback_data=f"child:{child_tg}:{max(0, offset - 10)}"))
    if offset + len(history) < total:
        pages.append(InlineKeyboardButton(text="Avvalgilari ➡️", callback_data=f"child:{child_tg}:{offset + 10}"))
    await c.message.answer(message, reply_markup=kb([pages]) if pages else None)

@dp.callback_query(F.data.startswith("b:"))
async def pick_book(c: CallbackQuery):
    await c.answer()
    st = one("select * from students where tg_id=? and role='student'", (c.from_user.id,))
    if not st:
        return await c.message.answer("Avval /start ni bosing.")
    await ask_days(c.message, c.from_user.id, int(c.data[2:]))

@dp.message(F.text & ~F.text.startswith("/"))
async def got_text(m: Message):
    st = one("select * from students where tg_id=?", (m.from_user.id,))
    if not st:
        return await show_viloyatlar(m)
    if st["state"] == "link_code" and st["role"] == "parent":
        code = m.text.strip().upper()
        entry = one("select * from parent_codes where code=? and expires>?", (code, int(time.time())))
        if not entry:
            return await m.answer("Kod noto'g'ri yoki muddati o'tgan. Farzandingizdan yangi kod oling.")
        if entry["student_tg"] == m.from_user.id:
            return await m.answer("O'zingizni farzand sifatida ulay olmaysiz.")
        if not q("delete from parent_codes where code=? and expires>?",
                 (code, int(time.time()))).rowcount:
            return await m.answer("Kod allaqachon ishlatilgan yoki muddati o'tgan. Yangi kod so'rang.")
        q("insert into parent_children(parent_tg,student_tg) values(?,?) on conflict do nothing",
          (m.from_user.id, entry["student_tg"]))
        q("update students set state=null where tg_id=?", (m.from_user.id,))
        await m.answer("✅ Farzandingiz ulandi. Yana farzand qo'shish yoki kitoblar tarixini ko'rish mumkin.")
        await show_parent_menu(m)
        return
    if st["state"] == "child_profile" and st["role"] == "student":
        parts = [part.strip() for part in m.text.split(",", 1)]
        if len(parts) != 2 or len(parts[0]) < 3 or not parts[1] or len(parts[0]) > 60 or len(parts[1]) > 10:
            return await m.answer("Ism-familiya va sinfni vergul bilan yozing (masalan: Aziza Karimova, 7-A):")
        q("update students set name=?, cls=?, state=null where tg_id=?",
          (parts[0], parts[1], m.from_user.id))
        return await issue_parent_code(m, m.from_user.id, st["school_id"])
    if st["state"] == "days" and st["role"] == "student":
        t = m.text.strip()
        if not t.isdigit() or not 1 <= int(t) <= 60:
            return await m.answer("Iltimos, 1 dan 60 gacha raqam yozing (masalan: 10):")
        q("update students set requested_days=?, state='identity' where tg_id=?",
          (int(t), m.from_user.id))
        return await m.answer("Ism-familiyangiz va sinfingizni yozing (masalan: Aziza Karimova, 7-A):")
    if st["state"] == "identity" and st["role"] == "student":
        parts = [part.strip() for part in m.text.split(",", 1)]
        if len(parts) != 2 or len(parts[0]) < 3 or not parts[1] or len(parts[0]) > 60 or len(parts[1]) > 10:
            return await m.answer("Ism-familiya va sinfni vergul bilan yozing (masalan: Aziza Karimova, 7-A):")
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
            await notify(x["tg_id"], f"⌛ \"{x['title']}\" bandining muddati tugadi. Kerak bo'lsa, qaytadan band qiling.")
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
    title: str; author: str = ""; total: int = 1

class Lend(BaseModel):
    name: str; book_id: int; days: int = 14

def me(authorization: str = Header("")):
    u = one("select * from librarians where token=?", (authorization.replace("Bearer ", ""),)) if authorization else None
    if not u:
        raise HTTPException(401, "Kirish kerak")
    return u

@app.get("/")
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

@app.get("/api/books")
def get_books(u=Depends(me)):
    return [dict(b, avail=avail(b)) for b in rows("select * from books where school_id=?", (u["school_id"],))]

@app.post("/api/books")
def add_book(d: BookIn, u=Depends(me)):
    if not d.title.strip():
        raise HTTPException(400, "Kitob nomini yozing")
    q("insert into books(school_id,title,author,total) values(?,?,?,?)", (u["school_id"], d.title.strip(), d.author.strip(), max(d.total, 1)))
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
    due = int(time.time()) + (r.get("days") or 14) * 86400
    q("update res set st=?, due=? where id=?", (new, due if new == "olindi" else r.get("due"), rid))
    msg = {"olindi": f"📗 \"{r['title']}\" kitobini olganingiz belgilandi.\nQaytarish muddati: {fmt(due)}",
           "qaytarildi": f"🙏 \"{r['title']}\" kitobi qaytarildi. Rahmat!",
           "bekor": f"❌ \"{r['title']}\" bandi bekor qilindi."}[new]
    await notify(r["tg_id"], msg)
    if r["tg_id"]:
        await notify_parents(r["tg_id"], f"Farzandingiz: {msg}")
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
    title: str; author: str = ""; total: int = 1

class BooksIn(BaseModel):
    items: list[BookRow]

@app.post("/api/books/import")
def import_books(d: BooksIn, u=Depends(me)):
    sid, skipped = u["school_id"], 0
    have = {(r["title"].lower(), (r["author"] or "").lower())
            for r in rows("select title, author from books where school_id=?", (sid,))}
    to_add = []
    for b in d.items[:5000]:
        ti, au = b.title.strip()[:200], b.author.strip()[:100]
        if not ti:
            continue
        key = (ti.lower(), au.lower())
        if key in have:
            skipped += 1
            continue
        have.add(key)
        to_add.append((sid, ti, au, min(max(b.total, 1), 1000)))
    many("insert into books(school_id,title,author,total) values(?,?,?,?)", to_add)
    return {"added": len(to_add), "skipped": skipped}
