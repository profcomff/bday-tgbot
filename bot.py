import asyncio
from aiogram import Bot, Dispatcher, types
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from aiogram.filters import Command
import aiosqlite
import os
from datetime import datetime, timedelta, time
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram import Router
import random
from aiogram.types import ReplyKeyboardRemove
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger
import logging
import pytz

API_TOKEN = '7690462834:AAG1zWXVzYB2yKvNG5fyQiARWYwxBgsrCtk'

bot = Bot(token=API_TOKEN)
dp = Dispatcher()

DB_PATH = 'bot_db.sqlite3'

router = Router()

class RegStates(StatesGroup):
    full_name = State()
    birthday = State()
    wish = State()
    edit_menu = State()
    edit_full_name = State()
    edit_birthday = State()
    edit_wish = State()

REMINDER_OFFSETS = [21, 14, 7, 3, 1]  # дней до ДР
scheduler = AsyncIOScheduler()

USER_COMMANDS = [
    '/start — регистрация или повторное приветствие',
    '/me — мои данные',
    '/ward — мой подопечный',
    '/edit — изменить профиль',
    '/menu — список команд',
    '/help — помощь и инструкция'
]
ADMIN_COMMANDS = [
    '/admin_users — список всех пользователей',
    '/admin_set [giver_id] [ward_id] — вручную назначить пару',
    '/admin_set_name <ФИО_дарителя> <ФИО_подопечного> — назначить пару по ФИО',
    '/admin_random — рандомное распределение пар',
    '/admin_reminders — расписание напоминаний',
    '/admin_pairs — таблица пар',
    '/make_admin <telegram_id> — назначить пользователя админом',
    '/admin_delete <telegram_id> — удалить пользователя',
    '/admin — список админ-команд'
]

# Настройка логирования
logging.basicConfig(
    filename='event_log.txt',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    encoding='utf-8'
)

MSK = pytz.timezone('Europe/Moscow')

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_id INTEGER UNIQUE,
                full_name TEXT,
                birthday TEXT,
                wish TEXT,
                ward_id INTEGER,
                giver_id INTEGER,
                is_admin INTEGER DEFAULT 0,
                registered_at TEXT
            )
        ''')
        await db.commit()

@router.message(Command('start'))
async def cmd_start(message: types.Message, state: FSMContext):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT * FROM users WHERE telegram_id = ?', (message.from_user.id,))
        user = await cursor.fetchone()
        if user:
            await message.answer('Вы уже зарегистрированы!\nДля изменения профиля используйте /edit')
            logging.info(f"/start повторно: telegram_id={message.from_user.id}")
        else:
            welcome_text = (
                '👋 <b>Добро пожаловать!</b>\n\n'
                'Этот бот поможет вам участвовать в системе "Тайный даритель" на дни рождения.\n'
                'Вы сможете указать свои пожелания к подарку, а также узнать, кому вы дарите подарок и когда.\n\n'
                'Пожалуйста, пройдите короткую регистрацию.\n'
                'Для списка команд используйте /menu или /help.'
            )
            await message.answer(welcome_text, parse_mode='HTML')
            await message.answer('Введите ваше ФИО:')
            await state.set_state(RegStates.full_name)
            logging.info(f"/start регистрация: telegram_id={message.from_user.id}")

@router.message(RegStates.full_name)
async def reg_full_name(message: types.Message, state: FSMContext):
    if message.text.startswith('/'):
        await message.answer('Пожалуйста, введите ваше ФИО, а не команду.')
        return
    await state.update_data(full_name=message.text)
    await message.answer('Введите вашу дату рождения (в формате ДД.ММ.ГГГГ):')
    await state.set_state(RegStates.birthday)

@router.message(RegStates.birthday)
async def reg_birthday(message: types.Message, state: FSMContext):
    try:
        datetime.strptime(message.text, '%d.%m.%Y')
    except ValueError:
        await message.answer('Неверный формат даты. Введите в формате ДД.ММ.ГГГГ:')
        return
    await state.update_data(birthday=message.text)
    await message.answer('Введите ваши пожелания к подарку:')
    await state.set_state(RegStates.wish)

@router.message(RegStates.wish)
async def reg_wish(message: types.Message, state: FSMContext):
    data = await state.get_data()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('''INSERT INTO users (telegram_id, full_name, birthday, wish, registered_at) VALUES (?, ?, ?, ?, ?)''',
            (message.from_user.id, data['full_name'], data['birthday'], message.text, datetime.now().isoformat()))
        await db.commit()
    logging.info(f"Регистрация: telegram_id={message.from_user.id}, ФИО={data['full_name']}, birthday={data['birthday']}, wish={message.text}")
    await state.clear()
    await message.answer('Регистрация завершена! Для списка команд используйте /menu или /help.')

# Команда для редактирования профиля
@router.message(Command('edit'))
async def edit_profile(message: types.Message, state: FSMContext):
    kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text='Изменить ФИО')],
        [KeyboardButton(text='Изменить дату рождения')],
        [KeyboardButton(text='Изменить пожелания')],
        [KeyboardButton(text='Отмена')]
    ], resize_keyboard=True)
    await message.answer('Что вы хотите изменить?', reply_markup=kb)
    await state.set_state(RegStates.edit_menu)

@router.message(RegStates.edit_menu)
async def edit_menu_handler(message: types.Message, state: FSMContext):
    if message.text == 'Изменить ФИО':
        await message.answer('Введите новое ФИО:', reply_markup=types.ReplyKeyboardRemove())
        await state.set_state(RegStates.edit_full_name)
    elif message.text == 'Изменить дату рождения':
        await message.answer('Введите новую дату рождения (ДД.ММ.ГГГГ):', reply_markup=types.ReplyKeyboardRemove())
        await state.set_state(RegStates.edit_birthday)
    elif message.text == 'Изменить пожелания':
        await message.answer('Введите новые пожелания:', reply_markup=types.ReplyKeyboardRemove())
        await state.set_state(RegStates.edit_wish)
    elif message.text == 'Отмена':
        await state.clear()
        await message.answer('Редактирование отменено.', reply_markup=types.ReplyKeyboardRemove())
    else:
        await message.answer('Пожалуйста, выберите действие с клавиатуры.')

@router.message(RegStates.edit_full_name)
async def edit_full_name(message: types.Message, state: FSMContext):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('UPDATE users SET full_name = ? WHERE telegram_id = ?', (message.text, message.from_user.id))
        await db.commit()
    await state.clear()
    await message.answer('ФИО успешно обновлено!')

@router.message(RegStates.edit_birthday)
async def edit_birthday(message: types.Message, state: FSMContext):
    try:
        datetime.strptime(message.text, '%d.%m.%Y')
    except ValueError:
        await message.answer('Неверный формат даты. Введите в формате ДД.ММ.ГГГГ:')
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('UPDATE users SET birthday = ? WHERE telegram_id = ?', (message.text, message.from_user.id))
        await db.commit()
    await state.clear()
    await message.answer('Дата рождения успешно обновлена!')

@router.message(RegStates.edit_wish)
async def edit_wish(message: types.Message, state: FSMContext):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('UPDATE users SET wish = ? WHERE telegram_id = ?', (message.text, message.from_user.id))
        await db.commit()
    await state.clear()
    await message.answer('Пожелания успешно обновлены!')

@router.message(Command('ward'))
async def show_ward(message: types.Message):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT ward_id FROM users WHERE telegram_id = ?', (message.from_user.id,))
        user = await cursor.fetchone()
        if not user or not user[0]:
            await message.answer('Вам пока не назначен подопечный.')
            return
        cursor = await db.execute('SELECT full_name, birthday, wish FROM users WHERE id = ?', (user[0],))
        ward = await cursor.fetchone()
        if not ward:
            await message.answer('Информация о подопечном не найдена.')
            return
        await message.answer(f'Ваш подопечный: {ward[0]}\nДень рождения: {ward[1]}\nПожелания: {ward[2]}')

@router.message(Command('me'))
async def show_my_data(message: types.Message):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT full_name, birthday, wish FROM users WHERE telegram_id = ?', (message.from_user.id,))
        user = await cursor.fetchone()
        if not user:
            await message.answer('Вы не зарегистрированы.')
            return
        await message.answer(f'Ваши данные:\nФИО: {user[0]}\nДень рождения: {user[1]}\nПожелания: {user[2]}')

# Проверка на администратора
async def is_admin(telegram_id):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT is_admin FROM users WHERE telegram_id = ?', (telegram_id,))
        user = await cursor.fetchone()
        return user and user[0] == 1

@router.message(Command('admin_users'))
async def admin_list_users(message: types.Message):
    if not await is_admin(message.from_user.id):
        await message.answer('Доступ запрещён.')
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT id, full_name, birthday, wish, telegram_id, is_admin, ward_id, giver_id FROM users')
        users = await cursor.fetchall()
        if not users:
            await message.answer('Пользователей нет.')
            return
        text = 'Пользователи:\n\n'
        for u in users:
            text += (f"ID: {u[0]}\nФИО: {u[1]}\nДР: {u[2]}\nПожелания: {u[3]}\nTG: {u[4]}\nАдмин: {u[5]}\nПодопечный: {u[6]}\nДаритель: {u[7]}\n---\n")
        await message.answer(text)

@router.message(Command('admin_set'))
async def admin_set_pair(message: types.Message):
    if not await is_admin(message.from_user.id):
        await message.answer('Доступ запрещён.')
        return
    args = message.text.split()
    if len(args) != 3:
        await message.answer('Используйте: /admin_set [giver_id] [ward_id]')
        return
    giver_id, ward_id = args[1], args[2]
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('UPDATE users SET ward_id = ? WHERE id = ?', (ward_id, giver_id))
        await db.execute('UPDATE users SET giver_id = ? WHERE id = ?', (giver_id, ward_id))
        await db.commit()
    logging.info(f"Назначение пары вручную: giver_id={giver_id}, ward_id={ward_id}, by={message.from_user.id}")
    await clear_all_reminders()
    await schedule_all_reminders()
    await message.answer(f'Назначено: {giver_id} дарит {ward_id}. Напоминания обновлены.')

@router.message(Command('admin_random'))
async def admin_random_assign(message: types.Message):
    if not await is_admin(message.from_user.id):
        await message.answer('Доступ запрещён.')
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT id FROM users')
        users = await cursor.fetchall()
        user_ids = [u[0] for u in users]
        if len(user_ids) < 2:
            await message.answer('Недостаточно пользователей для распределения.')
            return
        for attempt in range(1000):
            random.shuffle(user_ids)
            shifted = user_ids[1:] + user_ids[:1]
            has_reverse = False
            pair_map = {giver: ward for giver, ward in zip(user_ids, shifted)}
            for giver, ward in pair_map.items():
                if pair_map.get(ward) == giver:
                    has_reverse = True
                    break
            if not has_reverse:
                break
        else:
            await message.answer('Не удалось распределить без обратных пар. Попробуйте ещё раз.')
            logging.warning(f"Рандомное распределение: не удалось без обратных пар после 1000 попыток")
            return
        for giver, ward in zip(user_ids, shifted):
            await db.execute('UPDATE users SET ward_id = ? WHERE id = ?', (ward, giver))
            await db.execute('UPDATE users SET giver_id = ? WHERE id = ?', (giver, ward))
        await db.commit()
    logging.info(f"Рандомное распределение по кругу (без обратных пар): {user_ids}")
    await clear_all_reminders()
    await schedule_all_reminders()
    await message.answer('Распределение завершено! Напоминания обновлены.')

@router.message(Command('admin_set_name'))
async def admin_set_pair_by_name(message: types.Message):
    if not await is_admin(message.from_user.id):
        await message.answer('Доступ запрещён.')
        return
    args = message.text.split(maxsplit=2)
    if len(args) != 3:
        await message.answer('Используйте: /admin_set_name <ФИО_дарителя> <ФИО_подопечного>')
        return
    giver_name, ward_name = args[1], args[2]
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT id FROM users WHERE full_name = ?', (giver_name,))
        giver = await cursor.fetchone()
        cursor = await db.execute('SELECT id FROM users WHERE full_name = ?', (ward_name,))
        ward = await cursor.fetchone()
        if not giver or not ward:
            await message.answer('Не найден(ы) пользователь(и) с таким ФИО.')
            return
        await db.execute('UPDATE users SET ward_id = ? WHERE id = ?', (ward[0], giver[0]))
        await db.execute('UPDATE users SET giver_id = ? WHERE id = ?', (giver[0], ward[0]))
        await db.commit()
    logging.info(f"Назначение пары по ФИО: giver={giver_name}, ward={ward_name}, by={message.from_user.id}")
    await clear_all_reminders()
    await schedule_all_reminders()
    await message.answer(f'Назначено: {giver_name} дарит {ward_name}. Напоминания обновлены.')

@router.message(Command('admin_pairs'))
async def admin_pairs(message: types.Message):
    if not await is_admin(message.from_user.id):
        await message.answer('Доступ запрещён.')
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT u1.full_name, u2.full_name, u2.birthday FROM users u1 LEFT JOIN users u2 ON u1.ward_id = u2.id')
        pairs = await cursor.fetchall()
        if not pairs:
            await message.answer('Пар не найдено.')
            return
        text = '<b>Таблица пар:</b>\n\n'
        for giver, ward, bday in pairs:
            if ward:
                text += f'Даритель: <b>{giver}</b> — Подопечный: <b>{ward}</b> (ДР: {bday})\n'
            else:
                text += f'Даритель: <b>{giver}</b> — Подопечный: <i>не назначен</i>\n'
        await message.answer(text, parse_mode='HTML')

@router.message(Command('admin_reminders'))
async def admin_reminders(message: types.Message):
    if not await is_admin(message.from_user.id):
        await message.answer('Доступ запрещён.')
        return
    jobs = scheduler.get_jobs()
    if not jobs:
        await message.answer('Нет запланированных напоминаний.')
        return
    text = 'Запланированные напоминания:\n'
    for job in jobs:
        args = job.args
        if len(args) == 3:
            giver_id, ward_id, days_before = args
            text += f'Даритель ID: {giver_id}, Подопечный ID: {ward_id}, за {days_before} дн., дата: {job.next_run_time}\n'
    await message.answer(text)

@router.message(Command('help'))
async def help_command(message: types.Message):
    is_admin_flag = await is_admin(message.from_user.id)
    text = (
        '🎉 <b>Бот для распределения дарителей и подопечных на день рождения</b>\n\n'
        '<b>Для пользователей:</b>\n'
        '1. Зарегистрируйтесь через /start.\n'
        '2. Заполните ФИО, дату рождения и пожелания.\n'
        '3. Используйте /me для просмотра своих данных.\n'
        '4. Используйте /ward, чтобы узнать, кому вы дарите подарок.\n'
        '5. Изменить данные — /edit.\n'
        '6. Список команд — /menu.\n'
        '\n'
    )
    if is_admin_flag:
        text += (
            '<b>Для администраторов:</b>\n'
            '• /admin_users — список всех пользователей.\n'
            '• /admin_set [giver_id] [ward_id] — вручную назначить пару.\n'
            '• /admin_set_name <ФИО_дарителя> <ФИО_подопечного> — назначить пару по ФИО.\n'
            '• /admin_random — рандомное распределение пар.\n'
            '• /admin_reminders — расписание напоминаний.\n'
            '• /admin_pairs — таблица пар.\n'
            '• /make_admin <telegram_id> — назначить пользователя админом.\n'
            '• /admin_delete <telegram_id> — удалить пользователя.\n'
            '• /admin — список админ-команд.\n'
        )
    text += (
        'Пользователи получают напоминания о дне рождения подопечного за 21, 14, 7, 3 и 1 день.\n'
    )
    await message.answer(text, parse_mode='HTML')

@router.message(Command('menu'))
async def menu_command(message: types.Message):
    is_admin_flag = await is_admin(message.from_user.id)
    text = '<b>Доступные команды:</b>\n'
    text += '\n'.join(USER_COMMANDS)
    if is_admin_flag:
        text += '\n\n<b>Админ-команды:</b>\n' + '\n'.join(ADMIN_COMMANDS)
    await message.answer(text, parse_mode='HTML')

@router.message(Command('make_admin'))
async def make_admin_command(message: types.Message):
    if not await is_admin(message.from_user.id):
        await message.answer('Доступ запрещён. Только админ может назначать других админов.')
        return
    args = message.text.split()
    if len(args) != 2 or not args[1].isdigit():
        await message.answer('Используйте: /make_admin <telegram_id>')
        return
    tg_id = int(args[1])
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT id FROM users WHERE telegram_id = ?', (tg_id,))
        user = await cursor.fetchone()
        if not user:
            await message.answer('Пользователь с таким Telegram ID не найден.')
            return
        await db.execute('UPDATE users SET is_admin = 1 WHERE telegram_id = ?', (tg_id,))
        await db.commit()
    logging.info(f"Назначен админ: telegram_id={tg_id} (назначил {message.from_user.id})")
    await message.answer(f'Пользователь с Telegram ID {tg_id} теперь админ.')

@router.message(Command('admin_delete'))
async def admin_delete_user(message: types.Message):
    if not await is_admin(message.from_user.id):
        await message.answer('Доступ запрещён.')
        return
    args = message.text.split()
    if len(args) != 2 or not args[1].isdigit():
        await message.answer('Используйте: /admin_delete <telegram_id>')
        return
    tg_id = int(args[1])
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT id FROM users WHERE telegram_id = ?', (tg_id,))
        user = await cursor.fetchone()
        if not user:
            await message.answer('Пользователь с таким Telegram ID не найден.')
            return
        await db.execute('DELETE FROM users WHERE telegram_id = ?', (tg_id,))
        await db.commit()
    logging.info(f"Удалён пользователь: telegram_id={tg_id} (удалил {message.from_user.id})")
    await message.answer(f'Пользователь с Telegram ID {tg_id} удалён.')

@router.message(Command('admin'))
async def admin_commands(message: types.Message):
    if not await is_admin(message.from_user.id):
        await message.answer('Доступ запрещён.')
        return
    text = 'Админ-команды:\n' + '\n'.join(ADMIN_COMMANDS)
    await message.answer(text)

# Регистрация роутера

dp.include_router(router)

async def schedule_all_reminders():
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT id, birthday, giver_id FROM users WHERE giver_id IS NOT NULL')
        users = await cursor.fetchall()
        for user_id, birthday, giver_id in users:
            if not birthday or not giver_id:
                continue
            bday = datetime.strptime(birthday, '%d.%m.%Y')
            now = datetime.now(MSK)
            this_year = bday.replace(year=now.year)
            if this_year.date() < now.date():
                this_year = this_year.replace(year=now.year + 1)
            for days_before in REMINDER_OFFSETS:
                remind_date = this_year - timedelta(days=days_before)
                remind_dt = datetime.combine(remind_date, time(12, 0))
                remind_dt = MSK.localize(remind_dt)
                if remind_dt > now:
                    scheduler.add_job(send_reminder, DateTrigger(run_date=remind_dt), args=[giver_id, user_id, days_before])

async def send_reminder(giver_id, ward_id, days_before):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT full_name, birthday, wish FROM users WHERE id = ?', (ward_id,))
        ward = await cursor.fetchone()
        if not ward:
            return
        text = f"Напоминание: до дня рождения вашего подопечного {ward[0]} осталось {days_before} дн.\nДР: {ward[1]}\nПожелания: {ward[2]}"
        cursor = await db.execute('SELECT telegram_id FROM users WHERE id = ?', (giver_id,))
        giver = await cursor.fetchone()
        if giver:
            try:
                await bot.send_message(giver[0], text)
            except Exception:
                pass

# Удаление всех напоминаний
async def clear_all_reminders():
    for job in scheduler.get_jobs():
        job.remove()

async def main():
    await init_db()
    scheduler.start()
    await schedule_all_reminders()
    await dp.start_polling(bot)

if __name__ == '__main__':
    asyncio.run(main()) 