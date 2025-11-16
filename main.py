import asyncio
import os
from aiogram import Bot, Dispatcher
from aiogram.filters import CommandStart
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo

# -------------------------------------------------
# Загружаем токен
# -------------------------------------------------
BOT_TOKEN = os.getenv("BOT_TOKEN")
print("🚀 BOT_TOKEN LOADED:", repr(BOT_TOKEN))

if not BOT_TOKEN:
    raise Exception("❌ BOT_TOKEN NOT FOUND IN ENVIRONMENT")

# -------------------------------------------------

bot = Bot(BOT_TOKEN)
dp = Dispatcher()


@dp.message(CommandStart())
async def start(message: Message):
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Open App",
                    web_app=WebAppInfo(url="https://aibest-five.vercel.app/")
                )
            ]
        ]
    )
    await message.answer("Запустить Mini App:", reply_markup=kb)


async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())