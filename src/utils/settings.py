from typing import List

from pydantic import BaseSettings


class Settings(BaseSettings):
    API_TOKEN: str
    DB_DSN: str
    REMINDER_OFFSETS: List[int] = [21, 14, 7, 3, 1]
    DEBUG: bool = False

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


def get_settings() -> Settings:
    return Settings()
