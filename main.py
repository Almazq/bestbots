import asyncio
from aiogram import Bot, Dispatcher
from aiogram.filters import CommandStart
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo

# 🚨 ВСТАВЬ СВОЙ НАСТОЯЩИЙ ТОКЕН СЮДА (если репо приватный)
BOT_TOKEN = "8425860077:AAESfF3o_58rN9uKMtnWStW0iCyrJNqa56w"

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
