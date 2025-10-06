import asyncio
import logging
import os

import asyncpg

from src.utils.settings import get_settings

logging.basicConfig(level=logging.INFO)


async def apply_migrations():
    """Applies SQL migrations from the migrations folder."""
    settings = get_settings()
    conn = None
    try:
        conn = await asyncpg.connect(dsn=settings.ASYNC_PG_DSN)


        migrations_dir = "migrations"
        if not os.path.exists(migrations_dir):
            logging.info("Migrations directory not found. No migrations to apply.")
            return

        migration_files = sorted(
            [f for f in os.listdir(migrations_dir) if f.endswith(".sql")]
        )

        for migration_file in migration_files:
            logging.info(f"Applying migration {migration_file}...")
            with open(os.path.join(migrations_dir, migration_file), "r") as f:
                sql_script = f.read()

            await conn.execute(sql_script)
            logging.info(f"Migration {migration_file} applied successfully.")

    except Exception as e:
        logging.exception(f"Error applying migrations: {e}")
        raise
    finally:
        if conn:
            await conn.close()


if __name__ == "__main__":
    asyncio.run(apply_migrations())
