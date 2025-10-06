import asyncio
import logging
import random
from datetime import date, datetime, time, timedelta

import pytz
from aiogram import Bot, Dispatcher, Router, types
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import KeyboardButton, ReplyKeyboardMarkup, ReplyKeyboardRemove
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger

from src.database.db import db
from src.utils.settings import get_settings

logging.basicConfig(
    filename="event_log.txt",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    encoding="utf-8",
)

settings = get_settings()
bot = Bot(token=settings.API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
router = Router()


class RegStates(StatesGroup):
    full_name = State()
    birthday = State()
    wish = State()
    edit_menu = State()
    edit_full_name = State()
    edit_birthday = State()
    edit_wish = State()


REMINDER_OFFSETS = settings.REMINDER_OFFSETS
scheduler = AsyncIOScheduler()
MSK = pytz.timezone("Europe/Moscow")

USER_COMMANDS = [
    "/start — регистрация или повторное приветствие",
    "/me — мои данные",
    "/ward — мой подопечный",
    "/edit — изменить профиль",
    "/menu — список команд",
    "/help — помощь и инструкция",
]

ADMIN_COMMANDS = [
    "/users — список всех пользователей",
    "/set [giver_id] [ward_id] — вручную назначить пару",
    "/set_name [ФИО_дарителя] [ФИО_подопечного] — назначить по ФИО",
    "/random — рандомное распределение пар",
    "/reminders — расписание напоминаний",
    "/pairs — таблица пар",
    "/make_admin [telegram_id] — назначить админом",
    "/admin_revoke [telegram_id] — лишить пользователя прав админа",
    "/delete [telegram_id] — удалить пользователя",
    "/reset [user_id] — сбросить связь пользователя",
]


def format_bday(bday: date | None) -> str:
    """Форматирование даты рождения"""
    if not bday:
        return "—"
    return bday.strftime("%d.%m.%Y")


# ===== ОСНОВНЫЕ ХЕНДЛЕРЫ =====


# --- РЕГИСТРАЦИЯ ---
@router.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    """Обработчик команды /start"""
    async with db.pool.acquire() as conn:
        user = await conn.fetchrow(
            "SELECT * FROM users WHERE telegram_id = $1", message.from_user.id
        )

    if user:
        await message.answer(
            "Вы уже зарегистрированы!\nДля изменения профиля используйте /edit"
        )
        logging.info(f"/start повторно: telegram_id={message.from_user.id}")
    else:
        welcome_text = (
            "👋 <b>Добро пожаловать!</b>\n\n"
            'Этот бот поможет вам участвовать в системе "Тайный даритель" на дни рождения.\n'
            "Вы сможете указать свои пожелания к подарку, а также узнать, кому вы дарите подарок и когда.\n\n"
            "Пожалуйста, пройдите короткую регистрацию.\n"
            "Для списка команд используйте /menu или /help."
        )
        await message.answer(welcome_text, parse_mode="HTML")
        await message.answer("Введите ваше ФИО:")
        await state.set_state(RegStates.full_name)
        logging.info(f"/start регистрация: telegram_id={message.from_user.id}")


@router.message(RegStates.full_name)
async def reg_full_name(message: types.Message, state: FSMContext):
    """Обработка ввода ФИО"""
    if message.text.startswith("/"):
        await message.answer("Пожалуйста, введите ваше ФИО, а не команду.")
        return
    await state.update_data(full_name=message.text)
    await message.answer("Введите вашу дату рождения (в формате ДД.MM.ГГГГ):")
    await state.set_state(RegStates.birthday)


@router.message(RegStates.birthday)
async def reg_birthday(message: types.Message, state: FSMContext):
    """Обработка ввода даты рождения"""
    try:
        b = datetime.strptime(message.text, "%d.%m.%Y").date()
    except ValueError:
        await message.answer("Неверный формат даты. Введите в формате ДД.MM.ГГГГ:")
        return
    await state.update_data(birthday=b)
    await message.answer("Введите ваши пожелания к подарку:")
    await state.set_state(RegStates.wish)


@router.message(RegStates.wish)
async def reg_wish(message: types.Message, state: FSMContext):
    """Завершение регистрации"""
    data = await state.get_data()
    async with db.pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO users (telegram_id, full_name, birthday, wish, registered_at)
            VALUES ($1, $2, $3, $4, NOW())
            """,
            message.from_user.id,
            data["full_name"],
            data["birthday"],
            message.text,
        )
    logging.info(
        f"Регистрация: telegram_id={message.from_user.id}, ФИО={data['full_name']}"
    )
    await state.clear()
    await message.answer(
        "Регистрация завершена! Для списка команд используйте /menu или /help."
    )


# --- ПОКАЗ ДАННЫХ ---
@router.message(Command("me"))
async def show_my_data(message: types.Message):
    """Показ данных пользователя"""
    async with db.pool.acquire() as conn:
        user = await conn.fetchrow(
            "SELECT full_name, birthday, wish FROM users WHERE telegram_id = $1",
            message.from_user.id,
        )

    if not user:
        await message.answer("Вы не зарегистрированы.")
        return

    await message.answer(
        f'Ваши данные:\nФИО: {user["full_name"]}\n'
        f'День рождения: {format_bday(user["birthday"])}\n'
        f'Пожелания: {user["wish"]}'
    )


@router.message(Command("ward"))
async def show_ward(message: types.Message):
    """Показ подопечного"""
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT ward_id FROM users WHERE telegram_id = $1", message.from_user.id
        )

        if not row or not row.get("ward_id"):
            await message.answer("Вам пока не назначен подопечный.")
            return

        ward_id = row["ward_id"]
        ward = await conn.fetchrow(
            "SELECT full_name, birthday, wish FROM users WHERE id = $1", ward_id
        )

    if not ward:
        await message.answer("Информация о подопечном не найдена.")
        return

    await message.answer(
        f'Ваш подопечный: {ward["full_name"]}\n'
        f'День рождения: {format_bday(ward["birthday"])}\n'
        f'Пожелания: {ward["wish"]}'
    )


# --- РЕДАКТИРОВАНИЕ ---
@router.message(Command("edit"))
async def edit_profile(message: types.Message, state: FSMContext):
    """Меню редактирования"""
    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Изменить ФИО")],
            [KeyboardButton(text="Изменить дату рождения")],
            [KeyboardButton(text="Изменить пожелания")],
            [KeyboardButton(text="Отмена")],
        ],
        resize_keyboard=True,
    )
    await message.answer("Что вы хотите изменить?", reply_markup=kb)
    await state.set_state(RegStates.edit_menu)


@router.message(RegStates.edit_menu)
async def edit_menu_handler(message: types.Message, state: FSMContext):
    """Обработка выбора в меню редактирования"""
    if message.text == "Изменить ФИО":
        await message.answer("Введите новое ФИО:", reply_markup=ReplyKeyboardRemove())
        await state.set_state(RegStates.edit_full_name)
    elif message.text == "Изменить дату рождения":
        await message.answer(
            "Введите новую дату рождения (ДД.MM.ГГГГ):",
            reply_markup=ReplyKeyboardRemove(),
        )
        await state.set_state(RegStates.edit_birthday)
    elif message.text == "Изменить пожелания":
        await message.answer(
            "Введите новые пожелания:", reply_markup=ReplyKeyboardRemove()
        )
        await state.set_state(RegStates.edit_wish)
    elif message.text == "Отмена":
        await state.clear()
        await message.answer(
            "Редактирование отменено.", reply_markup=ReplyKeyboardRemove()
        )
    else:
        await message.answer("Пожалуйста, выберите действие с клавиатуры.")


@router.message(RegStates.edit_full_name)
async def edit_full_name(message: types.Message, state: FSMContext):
    """Изменение ФИО"""
    async with db.pool.acquire() as conn:
        await conn.execute(
            "UPDATE users SET full_name = $1 WHERE telegram_id = $2",
            message.text,
            message.from_user.id,
        )
    await state.clear()
    await message.answer("ФИО успешно обновлено!")


@router.message(RegStates.edit_birthday)
async def edit_birthday(message: types.Message, state: FSMContext):
    """Изменение даты рождения"""
    try:
        b = datetime.strptime(message.text, "%d.%m.%Y").date()
    except ValueError:
        await message.answer("Неверный формат даты. Введите в формате ДД.MM.ГГГГ:")
        return

    async with db.pool.acquire() as conn:
        await conn.execute(
            "UPDATE users SET birthday = $1 WHERE telegram_id = $2",
            b,
            message.from_user.id,
        )
    await state.clear()
    await message.answer("Дата рождения успешно обновлена!")


@router.message(RegStates.edit_wish)
async def edit_wish(message: types.Message, state: FSMContext):
    """Изменение пожеланий"""
    async with db.pool.acquire() as conn:
        await conn.execute(
            "UPDATE users SET wish = $1 WHERE telegram_id = $2",
            message.text,
            message.from_user.id,
        )
    await state.clear()
    await message.answer("Пожелания успешно обновлены!")


# --- АДМИНСКИЕ КОМАНДЫ ---
@router.message(Command("users"))
async def show_users(message: types.Message):
    """Список всех пользователей"""
    if not await db.is_admin(message.from_user.id):
        await message.answer("Доступ запрещён.")
        return

    async with db.pool.acquire() as conn:
        users = await conn.fetch(
            "SELECT id, full_name, birthday, wish, telegram_id, is_admin, ward_id, giver_id FROM users ORDER BY id"
        )

    if not users:
        await message.answer("Пользователей нет.")
        return

    text = "Пользователи:\n\n"
    for u in users:
        text += (
            f"ID: {u['id']}\nФИО: {u['full_name']}\n"
            f"ДР: {format_bday(u['birthday'])}\nПожелания: {u['wish']}\n"
            f"TG: {u['telegram_id']}\nАдмин: {int(u['is_admin'])}\n"
            f"Подопечный: {u['ward_id']}\nДаритель: {u['giver_id']}\n---\n"
        )

    await message.answer(text)


@router.message(Command("pairs"))
async def show_pairs(message: types.Message):
    """Таблица пар даритель-подопечный"""
    if not await db.is_admin(message.from_user.id):
        await message.answer("Доступ запрещён.")
        return

    try:
        async with db.pool.acquire() as conn:
            pairs = await conn.fetch(
                """
                SELECT
                    g.id AS giver_id,
                    g.telegram_id AS giver_telegram_id,
                    g.full_name AS giver_name,
                    w.id AS ward_id,
                    w.telegram_id AS ward_telegram_id,
                    w.full_name AS ward_name,
                    w.birthday AS ward_birthday
                FROM
                    users g
                JOIN
                    users w ON g.ward_id = w.id
                ORDER BY
                    w.birthday
            """
            )

            if not pairs:
                await message.answer("Нет активных пар даритель-подопечный.")
                return

            text = "<b>Таблица пар даритель → подопечный:</b>\n\n"

            for i, pair in enumerate(pairs, 1):
                ward_bday = pair["ward_birthday"]
                bday_formatted = (
                    ward_bday.strftime("%d.%m.%Y") if ward_bday else "не указана"
                )

                text += (
                    f"{i}. <b>Даритель:</b> {pair['giver_name']} (ID: {pair['giver_telegram_id']})\n"
                    f"   <b>Подопечный:</b> {pair['ward_name']} (ID: {pair['ward_telegram_id']})\n"
                    f"   <b>День рождения подопечного:</b> {bday_formatted}\n\n"
                )

            if len(text) > 4000:
                parts = [text[i : i + 4000] for i in range(0, len(text), 4000)]
                for part in parts:
                    await message.answer(part, parse_mode="HTML")
            else:
                await message.answer(text, parse_mode="HTML")

    except Exception as e:
        logging.exception(f"Ошибка при получении таблицы пар: {e}")
        await message.answer(f"Произошла ошибка: {str(e)}")


@router.message(Command("random"))
async def random_distribution(message: types.Message):
    """Рандомное распределение пар"""
    if not await db.is_admin(message.from_user.id):
        await message.answer("Доступ запрещён.")
        return

    try:
        async with db.pool.acquire() as conn:
            users = await conn.fetch("SELECT id FROM users ORDER BY id")

            if len(users) < 2:
                await message.answer(
                    "Недостаточно пользователей для распределения пар (нужно минимум 2)."
                )
                return

            user_ids = [user["id"] for user in users]
            shuffled_ids = user_ids.copy()
            random.shuffle(shuffled_ids)

            pairs = []
            for i in range(len(shuffled_ids)):
                giver_id = shuffled_ids[i]
                ward_id = shuffled_ids[(i + 1) % len(shuffled_ids)]
                pairs.append((giver_id, ward_id))

            for giver_id, ward_id in pairs:
                await conn.execute(
                    "UPDATE users SET ward_id = $1 WHERE id = $2", ward_id, giver_id
                )
                await conn.execute(
                    "UPDATE users SET giver_id = $1 WHERE id = $2", giver_id, ward_id
                )

            logging.info(f"Рандомное распределение по кругу: {shuffled_ids}")

            await clear_all_reminders()
            await schedule_all_reminders()

            await message.answer(
                f"Успешно распределены {len(pairs)} пар пользователей. Напоминания обновлены."
            )

    except Exception as e:
        logging.exception(f"Ошибка при рандомном распределении: {e}")
        await message.answer(f"Произошла ошибка: {str(e)}") @ router.message(
            Command("set")
        )


async def set_pair(message: types.Message):
    """Назначение пары вручную по ID"""
    if not await db.is_admin(message.from_user.id):
        await message.answer("Доступ запрещён.")
        return

    parts = message.text.split()
    if len(parts) != 3:
        await message.answer("Использование: /set [giver_id] [ward_id]")
        return

    try:
        giver_id = int(parts[1])
        ward_id = int(parts[2])
    except ValueError:
        await message.answer("Ошибка: ID должны быть числами.")
        return

    async with db.pool.acquire() as conn:
        giver = await conn.fetchrow("SELECT id FROM users WHERE id = $1", giver_id)
        ward = await conn.fetchrow("SELECT id FROM users WHERE id = $1", ward_id)

        if not giver:
            await message.answer(f"Даритель с ID {giver_id} не найден.")
            return
        if not ward:
            await message.answer(f"Подопечный с ID {ward_id} не найден.")
            return

        await conn.execute(
            "UPDATE users SET ward_id = $1 WHERE id = $2", ward_id, giver_id
        )
        await conn.execute(
            "UPDATE users SET giver_id = $1 WHERE id = $2", giver_id, ward_id
        )

    await clear_all_reminders()
    await schedule_all_reminders()

    await message.answer(
        f"Пара назначена: Даритель #{giver_id} → Подопечный #{ward_id}. Напоминания обновлены."
    )


@router.message(Command("set_name"))
async def set_pair_by_name(message: types.Message):
    """Назначение пары вручную по ФИО"""
    if not await db.is_admin(message.from_user.id):
        await message.answer("Доступ запрещён.")
        return

    cmd_parts = message.text.split(maxsplit=1)
    if len(cmd_parts) < 2:
        await message.answer(
            'Использование: /set_name "ФИО дарителя" "ФИО подопечного"'
        )
        return

    full_text = cmd_parts[1].strip()

    if '"' in full_text:
        import re

        names = re.findall(r'"([^"]*)"', full_text)
        if len(names) < 2:
            await message.answer(
                'Укажите ФИО дарителя и подопечного в кавычках: /set_name "ФИО дарителя" "ФИО подопечного"'
            )
            return
        giver_name, ward_name = names[0], names[1]
    else:
        async with db.pool.acquire() as conn:
            all_users = await conn.fetch("SELECT id, full_name FROM users")

        possible_givers = []
        possible_wards = []

        for user in all_users:
            if user["full_name"].lower() in full_text.lower():
                if not possible_givers:
                    possible_givers.append(user)
                elif not possible_wards:
                    possible_wards.append(user)

        if not possible_givers or not possible_wards:
            await message.answer(
                'Не удалось определить дарителя и подопечного. Используйте кавычки: /set_name "ФИО дарителя" "ФИО подопечного"'
            )
            return

        giver_name = possible_givers[0]["full_name"]
        ward_name = possible_wards[0]["full_name"]

    async with db.pool.acquire() as conn:
        giver = await conn.fetchrow(
            "SELECT id FROM users WHERE full_name = $1", giver_name
        )
        ward = await conn.fetchrow(
            "SELECT id FROM users WHERE full_name = $1", ward_name
        )

        if not giver:
            await message.answer(f'Даритель с ФИО "{giver_name}" не найден.')
            return
        if not ward:
            await message.answer(f'Подопечный с ФИО "{ward_name}" не найден.')
            return

        giver_id = giver["id"]
        ward_id = ward["id"]

        await conn.execute(
            "UPDATE users SET ward_id = $1 WHERE id = $2", ward_id, giver_id
        )
        await conn.execute(
            "UPDATE users SET giver_id = $1 WHERE id = $2", giver_id, ward_id
        )

    await clear_all_reminders()
    await schedule_all_reminders()

    await message.answer(
        f'Пара назначена: Даритель "{giver_name}" → Подопечный "{ward_name}". Напоминания обновлены.'
    )


@router.message(Command("make_admin"))
async def make_admin_command(message: types.Message):
    """Назначение админа"""
    if not await db.is_admin(message.from_user.id):
        await message.answer("Доступ запрещён.")
        return

    parts = message.text.split()
    if len(parts) != 2:
        await message.answer("Использование: /make_admin [telegram_id]")
        return

    try:
        telegram_id = int(parts[1])
    except ValueError:
        await message.answer("Ошибка: ID должен быть числом.")
        return

    async with db.pool.acquire() as conn:
        user = await conn.fetchrow(
            "SELECT id FROM users WHERE telegram_id = $1", telegram_id
        )

        if not user:
            await message.answer(f"Пользователь с Telegram ID {telegram_id} не найден.")
            return

        await conn.execute(
            "UPDATE users SET is_admin = true WHERE telegram_id = $1", telegram_id
        )

    await message.answer(
        f"Пользователь с Telegram ID {telegram_id} назначен администратором."
    )


@router.message(Command("delete"))
async def delete_user(message: types.Message):
    """Удаление пользователя"""
    if not await db.is_admin(message.from_user.id):
        await message.answer("Доступ запрещён.")
        return

    parts = message.text.split()
    if len(parts) != 2:
        await message.answer("Использование: /delete [telegram_id]")
        return

    try:
        telegram_id = int(parts[1])
    except ValueError:
        await message.answer("Ошибка: ID должен быть числом.")
        return

    if telegram_id == message.from_user.id:
        await message.answer("Вы не можете удалить самого себя.")
        return

    async with db.pool.acquire() as conn:
        user = await conn.fetchrow(
            "SELECT id, ward_id, giver_id FROM users WHERE telegram_id = $1",
            telegram_id,
        )

        if not user:
            await message.answer(f"Пользователь с Telegram ID {telegram_id} не найден.")
            return

        ward_id = user["ward_id"]
        giver_id = user["giver_id"]

        if ward_id:
            await conn.execute(
                "UPDATE users SET giver_id = NULL WHERE id = $1", ward_id
            )
        if giver_id:
            await conn.execute(
                "UPDATE users SET ward_id = NULL WHERE id = $1", giver_id
            )

        await conn.execute("DELETE FROM users WHERE telegram_id = $1", telegram_id)

    await clear_all_reminders()
    await schedule_all_reminders()

    await message.answer(
        f"Пользователь с Telegram ID {telegram_id} удален. Связи обновлены. Напоминания перепланированы."
    )


@router.message(Command("admin_revoke"))
async def revoke_admin_rights(message: types.Message):
    """Лишение пользователя прав администратора"""
    if not await db.is_admin(message.from_user.id):
        await message.answer("Доступ запрещён.")
        return

    parts = message.text.split()
    if len(parts) != 2:
        await message.answer("Использование: /admin_revoke [telegram_id]")
        return

    try:
        telegram_id = int(parts[1])
    except ValueError:
        await message.answer("Ошибка: ID должен быть числом.")
        return

    if telegram_id == message.from_user.id:
        await message.answer("Вы не можете лишить прав администратора самого себя.")
        return

    async with db.pool.acquire() as conn:
        user = await conn.fetchrow(
            "SELECT id, is_admin FROM users WHERE telegram_id = $1", telegram_id
        )

        if not user:
            await message.answer(f"Пользователь с Telegram ID {telegram_id} не найден.")
            return

        if not user["is_admin"]:
            await message.answer(
                f"Пользователь с Telegram ID {telegram_id} не является администратором."
            )
            return

        await conn.execute(
            "UPDATE users SET is_admin = false WHERE telegram_id = $1", telegram_id
        )

    await message.answer(
        f"Пользователь с Telegram ID {telegram_id} лишен прав администратора."
    )


@router.message(Command("reset"))
async def reset_connections(message: types.Message):
    """Сброс связей пользователя"""
    if not await db.is_admin(message.from_user.id):
        await message.answer("Доступ запрещён.")
        return

    parts = message.text.split()

    if len(parts) == 1 or parts[1].lower() == "all":
        async with db.pool.acquire() as conn:
            await conn.execute("UPDATE users SET ward_id = NULL, giver_id = NULL")
        await message.answer("Связи всех пользователей сброшены.")
        await clear_all_reminders()
        return

    try:
        user_id = int(parts[1])
    except ValueError:
        await message.answer(
            "Ошибка: ID должен быть числом. Используйте `/admin_reset [user_id]` или `/admin_reset all`"
        )
        return

    async with db.pool.acquire() as conn:
        user = await conn.fetchrow(
            "SELECT id, ward_id, giver_id FROM users WHERE id = $1", user_id
        )

        if not user:
            await message.answer(f"Пользователь с ID {user_id} не найден.")
            return

        ward_id = user["ward_id"]
        giver_id = user["giver_id"]

        await conn.execute(
            "UPDATE users SET ward_id = NULL, giver_id = NULL WHERE id = $1", user_id
        )

        if ward_id:
            await conn.execute(
                "UPDATE users SET giver_id = NULL WHERE id = $1", ward_id
            )
        if giver_id:
            await conn.execute(
                "UPDATE users SET ward_id = NULL WHERE id = $1", giver_id
            )

    await clear_all_reminders()
    await schedule_all_reminders()

    await message.answer(
        f"Связи пользователя #{user_id} и связанных с ним пользователей сброшены. Напоминания обновлены."
    )


@router.message(Command("reminders"))
async def show_reminders(message: types.Message):
    """Показ запланированных напоминаний"""
    if not await db.is_admin(message.from_user.id):
        await message.answer("Доступ запрещён.")
        return

    jobs = scheduler.get_jobs()
    if not jobs:
        await message.answer("Нет запланированных напоминаний.")
        return

    now = datetime.now(MSK)
    text = "<b>Запланированные напоминания:</b>\n\n"

    for i, job in enumerate(jobs, 1):
        run_date = job.trigger.run_date
        time_diff = run_date - now
        days = time_diff.days
        hours = time_diff.seconds // 3600

        giver_id, ward_id, days_before = job.args

        async with db.pool.acquire() as conn:
            giver_info = await conn.fetchrow(
                "SELECT full_name FROM users WHERE id = $1", giver_id
            )
            ward_info = await conn.fetchrow(
                "SELECT full_name FROM users WHERE id = $1", ward_id
            )

        giver_name = giver_info["full_name"] if giver_info else f"ID: {giver_id}"
        ward_name = ward_info["full_name"] if ward_info else f"ID: {ward_id}"

        text += (
            f"{i}. {run_date.strftime('%d.%m.%Y %H:%M')} (через {days}д {hours}ч)\n"
            f"   Даритель: {giver_name}\n"
            f"   Подопечный: {ward_name}\n"
            f"   За {days_before} дней до ДР\n\n"
        )

    await message.answer(text, parse_mode="HTML")


@router.message(Command("menu"))
async def menu_command(message: types.Message):
    """Список команд"""
    try:
        text = "<b>Доступные команды:</b>\n\n"
        text += "\n".join(["• " + c for c in USER_COMMANDS]) + "\n"

        try:
            is_admin_flag = await db.is_admin(message.from_user.id)
            if is_admin_flag:
                text += "\n<b>Команды администратора:</b>\n"
                text += "\n".join(["• " + c for c in ADMIN_COMMANDS])
        except Exception as e:
            logging.warning(f"Не удалось проверить админские права: {e}")

        await message.answer(text, parse_mode="HTML")
    except Exception as e:
        logging.exception(f"Критическая ошибка в команде menu: {e}")
        await message.answer(
            "Произошла ошибка при получении списка команд. Пожалуйста, попробуйте позже."
        )


@router.message(Command("help"))
async def help_command(message: types.Message):
    """Справка"""
    try:
        is_admin_flag = await db.is_admin(message.from_user.id)
        text = (
            "🎉 <b>Бот для распределения дарителей и подопечных на день рождения</b>\n\n"
            "<b>Для пользователей:</b>\n"
            "1. Зарегистрируйтесь через /start.\n"
            "2. Заполните ФИО, дату рождения и пожелания.\n"
            "3. Используйте /me для просмотра своих данных.\n"
            "4. Используйте /ward, чтобы узнать, кому вы дарите подарок.\n"
            "5. Изменить данные — /edit.\n"
            "6. Список команд — /menu.\n"
            "\n"
        )

        if is_admin_flag:
            text += (
                "<b>Для администраторов:</b>\n"
                + "\n".join(["• " + c for c in ADMIN_COMMANDS])
                + "\n\n"
            )

        text += "Пользователи получают напоминания о дне рождения подопечного за 21, 14, 7, 3 и 1 день.\n"
        await message.answer(text, parse_mode="HTML")
    except Exception as e:
        logging.exception(f"Ошибка в команде help: {e}")
        await message.answer(
            "Произошла ошибка при получении справки. Пожалуйста, попробуйте позже."
        )


# ===== НАПОМИНАНИЯ =====
async def schedule_all_reminders():
    """Планирование всех напоминаний"""
    await clear_all_reminders()

    async with db.pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, birthday, giver_id FROM users WHERE giver_id IS NOT NULL"
        )

    now = datetime.now(MSK)

    for rec in rows:
        user_id = rec["id"]
        bday = rec["birthday"]
        giver_id = rec["giver_id"]

        if not bday or not giver_id:
            continue

        try:
            this_year = date(year=now.year, month=bday.month, day=bday.day)
        except ValueError:
            continue

        if this_year < now.date():
            try:
                this_year = date(year=now.year + 1, month=bday.month, day=bday.day)
            except ValueError:
                continue

        for days_before in REMINDER_OFFSETS:
            remind_date = this_year - timedelta(days=days_before)
            remind_dt = datetime.combine(remind_date, time(12, 0))
            remind_dt = MSK.localize(remind_dt)

            if remind_dt > now:
                scheduler.add_job(
                    send_reminder,
                    DateTrigger(run_date=remind_dt),
                    args=[giver_id, user_id, days_before],
                )


async def send_reminder(giver_id: int, ward_id: int, days_before: int):
    """Отправка напоминания"""
    async with db.pool.acquire() as conn:
        ward = await conn.fetchrow(
            "SELECT full_name, birthday, wish FROM users WHERE id = $1", ward_id
        )
        giver = await conn.fetchrow(
            "SELECT telegram_id FROM users WHERE id = $1", giver_id
        )

    if not ward or not giver:
        return

    text = (
        f"Напоминание: до дня рождения вашего подопечного {ward['full_name']} осталось {days_before} дн.\n"
        f"ДР: {format_bday(ward['birthday'])}\nПожелания: {ward['wish']}"
    )

    try:
        await bot.send_message(giver["telegram_id"], text)
    except Exception as e:
        logging.error(f"Ошибка отправки напоминания: {e}")


async def clear_all_reminders():
    """Очистка всех напоминаний"""
    for job in scheduler.get_jobs():
        try:
            job.remove()
        except Exception as e:
            logging.error(f"Ошибка удаления job: {e}")


# ===== ЗАПУСК БОТА =====
dp.include_router(router)


async def main():
    """Главная функция"""
    await db.init()
    scheduler.start()
    await schedule_all_reminders()

    # Удаляем вебхук перед запуском полинга
    await bot.delete_webhook(drop_pending_updates=True)

    try:
        await dp.start_polling(bot)
    finally:
        await clear_all_reminders()
        await db.close()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
