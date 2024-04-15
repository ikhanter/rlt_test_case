import asyncio

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command

from rlt_test_case.database import start_db, process_json
from rlt_test_case.config import TOKEN
from rlt_test_case.message_processor import is_valid_json, make_json

dp = Dispatcher()


@dp.message()
async def process_query(message: types.Message):
    result = make_json(message.text)
    if not result:
        await message.answer('Your message is not JSON-like.')
    else:
        if not is_valid_json(result):
            await message.answer(
                '''Your JSON is not valid. Valid JSON should have next field:
                \'dt_from\': date-time,
                \'dt_upto\': date-time,
                \'group_type\': [\'hour\', \'day\', \'month\']'''
            )
        else:
            data = await process_json(result, 2)
            await message.answer(str(data))

async def on_startup():
    await start_db()
    print('Bot initialized')

async def main():
    asyncio.create_task(on_startup())
    bot = Bot(TOKEN)
    await dp.start_polling(bot)


if __name__ == '__main__':
    asyncio.run(main())