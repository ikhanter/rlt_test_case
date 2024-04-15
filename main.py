"""Main module for activating Bot."""
import asyncio

from aiogram import Bot, Dispatcher, types

from rlt_test_case.config import TOKEN
from rlt_test_case.database import start_db, process_json
from rlt_test_case.message_processor import is_valid_json, make_json

dp = Dispatcher()


@dp.message()
async def process_query(message: types.Message) -> None:
    """Process JSON for getting summarized data."""
    result = make_json(message.text)
    if not result:
        await message.answer("Your message is not JSON-like.")
    elif not is_valid_json(result):
        await message.answer(
            """Your JSON is not valid. Valid JSON should have next field:
            \'dt_from\': date-time,
            \'dt_upto\': date-time,
            \'group_type\': [\'hour\', \'day\', \'month\']""",
        )
    else:
        data = await process_json(result, 2)
        await message.answer(str(data))

async def on_startup() -> None:
    """Initialize and fill DB."""
    await start_db()
    print("Bot initialized")  # noqa: T201

async def main() -> None:
    """Run main thread."""
    task = asyncio.create_task(on_startup())
    bot = Bot(TOKEN)
    await task
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
