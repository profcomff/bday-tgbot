import asyncpg
import logging
from src.utils.settings import get_settings

class Database:
    def __init__(self):
        self.pool = None
        self.settings = get_settings()

    async def init(self):
        """Инициализация пула соединений"""
        try:
            dsn = self.settings.ASYNC_PG_DSN
            self.pool = await asyncpg.create_pool(
                dsn=dsn,
                min_size=1,
                max_size=10,
            )
            logging.info("DB pool created")
        except Exception as e:
            logging.exception("Ошибка инициализации пула БД")
            raise

    async def close(self):
        """Закрытие пула соединений"""
        if self.pool:
            await self.pool.close()
            logging.info("DB pool closed")

    async def is_admin(self, telegram_id: int) -> bool:
        """Проверка, является ли пользователь админом"""
        try:
            if not self.pool:
                logging.warning("DB pool is None, reinitializing...")
                await self.init()  # Пробуем переинициализировать пул
                if not self.pool:
                    logging.error("Failed to reinitialize DB pool")
                    return False
                    
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT is_admin FROM users WHERE telegram_id = $1", 
                    telegram_id
                )
                return bool(row and (row.get('is_admin') or row['is_admin']))
        except Exception as e:
            logging.exception(f"Error in is_admin check: {e}")
            return False  # В случае ошибки возвращаем False вместо вызова исключения

db = Database()