from typing import List

from pydantic import BaseSettings


class Settings(BaseSettings):
    API_TOKEN: str
    DATABASE_URL: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/bot_db"
    REMINDER_OFFSETS: List[int] = [21, 14, 7, 3, 1]
    DEBUG: bool = False

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

    @property
    def ASYNC_PG_DSN(self) -> str:
        """
        Возвращает DSN для asyncpg (без +asyncpg), например:
        postgresql://user:pass@host:port/db
        """
        if self.DATABASE_URL.startswith("postgresql+asyncpg://"):
            return self.DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://")
        return self.DATABASE_URL


def get_settings() -> Settings:
    return Settings()
