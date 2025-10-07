import logging

from aiogram import Bot


class TelegramLoggingHandler(logging.Handler):
    def __init__(self, bot: Bot, chat_id: int):
        super().__init__()
        self.bot = bot
        self.chat_id = chat_id

    def emit(self, record: logging.LogRecord):
        log_entry = self.format(record)
        # The actual sending is async, but emit is sync.
        # We need to schedule it in the bot's event loop.
        import asyncio

        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self.bot.send_message(self.chat_id, log_entry))
        except RuntimeError:  # No running loop
            asyncio.run(self.bot.send_message(self.chat_id, log_entry))
        except Exception as e:
            # Avoid logging loops
            print(f"Failed to send log message to Telegram: {e}")

