<<<<<<< HEAD
# bday-tgbot
Бот для распределения дарителей на ДР
=======
# Birthday Gift Bot

## Быстрый старт

1. **Вставьте токен бота**
   - Откройте файл `bot.py`.
   - Найдите строку:
     ```python
     API_TOKEN = os.getenv('TG_BOT_TOKEN') or 'YOUR_BOT_TOKEN_HERE'
     ```
   - Замените на:
     ```python
     API_TOKEN = 'ВАШ_ТОКЕН_БОТА'
     ```
   - Сохраните файл.

2. **Установите зависимости**
   ```sh
   pip install -r requirements.txt
   ```

3. **Запустите бота**
   ```sh
   python bot.py
   ```

4. **Сделайте себя администратором**
   - Зарегистрируйтесь в боте.
   - После регистрации бот создаст базу данных `bot_db.sqlite3`.
   - Откройте файл базы данных в любой программе для работы с SQLite (например, DB Browser for SQLite) или выполните команду:
     ```sh
     sqlite3 bot_db.sqlite3 "UPDATE users SET is_admin = 1 WHERE telegram_id = ВАШ_TG_ID;"
     ```
   - Узнать свой Telegram ID можно через бота [@userinfobot](https://t.me/userinfobot).

5. **Пользуйтесь!**
   - Все команды и функции уже реализованы.
   - Для админов доступны команды `/admin_users`, `/admin_set`, `/admin_random`, `/admin_reminders`.

---

## Запуск бота на сервере (Linux)

1. **Установите Python 3.11+ и pip**
2. **Склонируйте/загрузите проект и перейдите в папку с ботом**
3. **Установите зависимости:**
   ```sh
   pip install -r requirements.txt
   ```
4. **Вставьте токен в bot.py** (как описано выше)
5. **Запустите бота в screen/tmux или через systemd:**

### Вариант 1: screen
```sh
screen -S birthdaybot
python3 bot.py
# Для выхода из screen: Ctrl+A, затем D
```

### Вариант 2: systemd (рекомендуется для продакшена)
Создайте файл `/etc/systemd/system/birthdaybot.service`:
```
[Unit]
Description=Birthday Gift Telegram Bot
After=network.target

[Service]
Type=simple
User=ВАШ_ПОЛЬЗОВАТЕЛЬ
WorkingDirectory=/path/to/your/bot
ExecStart=/usr/bin/python3 bot.py
Restart=always

[Install]
WantedBy=multi-user.target
```

Затем:
```sh
sudo systemctl daemon-reload
sudo systemctl enable birthdaybot
sudo systemctl start birthdaybot
sudo systemctl status birthdaybot
```

---

**Рекомендации по безопасности:**
- Не публикуйте токен бота в открытом доступе.
- Используйте отдельного пользователя Linux для запуска бота.
- Регулярно делайте резервные копии базы данных `bot_db.sqlite3`.

## Если нужна помощь с запуском на сервере, в Docker или автоматизацией — напишите мне! 
>>>>>>> 000f8e0 (Initial commit: birthday bot)
