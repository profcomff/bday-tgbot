import sys
import os
from logging.config import fileConfig

from sqlalchemy import engine_from_config, pool, create_engine, MetaData
from alembic import context

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

try:
    from src.utils.settings import get_settings

    settings = get_settings()
except Exception:
    settings = None

alembic_url_from_ini = config.get_main_option("sqlalchemy.url")
if alembic_url_from_ini and alembic_url_from_ini.strip():
    DATABASE_URL_RAW = alembic_url_from_ini
elif settings is not None and getattr(settings, "DATABASE_URL", None):
    DATABASE_URL_RAW = settings.DATABASE_URL
else:
    raise RuntimeError(
        "Не найден DATABASE_URL: укажи sqlalchemy.url в alembic.ini "
        "или задай DATABASE_URL в src.utils.settings"
    )

if "+asyncpg" in DATABASE_URL_RAW:
    DATABASE_URL_SYNC = DATABASE_URL_RAW.replace("+asyncpg", "+psycopg2")
else:
    DATABASE_URL_SYNC = DATABASE_URL_RAW

target_metadata = MetaData()

def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode."""
    context.configure(
        url=DATABASE_URL_SYNC,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode (sync engine for Alembic)."""
    connectable = create_engine(
        DATABASE_URL_SYNC,
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
