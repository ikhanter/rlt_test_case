"""Make client for DB and process data."""
import bson
import copy
import json
from datetime import datetime
from typing import Literal

import motor.motor_asyncio as motor
from dateutil.relativedelta import relativedelta

from .config import DB_HOST, DB_PASSWORD, DB_PORT, DB_USERNAME, DB_NAME

client = motor.AsyncIOMotorClient(
    f"{DB_HOST}:{DB_PORT}",
    username=DB_USERNAME,
    password=DB_PASSWORD,
)

db = client.get_database(DB_NAME)

intervals = {
        "hour": relativedelta(hours=1),
        "day": relativedelta(days=1),
        "week": relativedelta(weeks=1),
        "month": relativedelta(months=1),
    }

async def start_db() -> None:
    """Make a collection and fill the data."""
    if "payments" not in await db.list_collection_names():
        async with await client.start_session() as session:
            payments_collection = db["payments"]
            with open("sampleDB/sample_collection.bson", "rb") as f:  # noqa: ASYNC101, PTH123
                bson_data = f.read()
                data = bson.decode_all(bson_data)
            await payments_collection.insert_many(data, session=session)

async def process_json(json_data: dict, realization: Literal[1, 2]) -> dict:  # noqa: C901
    """Process JSON, get data from the DB and return result."""
    match json_data["group_type"]:
            case "hour":
                interval = intervals["hour"]
            case "day":
                interval = intervals["day"]
            case "week":
                interval = intervals["week"]
            case "month":
                interval = intervals["month"]

    result = {
            "dataset": [],
            "labels": [],
        }

    async def realization_1() -> dict:
        """Process data with accurate value of last minute.

        Вариант обработки данных с точным возвратом суммы значений.
        В исходных данных наблюдается проблема последней минуты,
        когда при запросе с dt_upto: .......T23:59:00
        правильным считается выгрузка данных с учетом всей последней минуты,
        в то время как по логике учетываться она не должна.
        """
        filter_query = {
            "dt": {
                "$gte": datetime.fromisoformat(json_data["dt_from"]),
                "$lt": datetime.fromisoformat(json_data["dt_upto"]),
            },
        }

        docs = await db["payments"].find(filter_query).to_list(None)

        current_date = datetime.fromisoformat(json_data["dt_from"])
        finish_date = datetime.fromisoformat(json_data["dt_upto"])

        while current_date < finish_date:
            temp_end_date = current_date + interval
            if temp_end_date > finish_date:
                temp_end_date = finish_date

            summary = 0
            for doc in docs:
                if (doc["dt"] >= current_date) and (doc["dt"] < temp_end_date):
                    summary += doc["value"]
            result["dataset"].append(summary)
            result["labels"].append(current_date.isoformat())
            current_date = temp_end_date

        return json.dumps(result)

    async def realization_2() -> dict:
        """Process data with rounded value of last minute.

        Вариант обработки данных с возвратом суммы значений с округлением.
        В исходных данных наблюдается проблема последней минуты,
        когда при запросе с dt_upto: .......T23:59:00
        правильным считается выгрузка данных с учетом всей последней минуты.
        В данном случае она учитывается.
        (Понял логику так, что если у нас есть запрос на данные за 4 месяца, а dt_upto
        соответствует общему периоду в 3 месяца с лишним, то главное,
        чтобы начала периодов входили во временной отрезок, от которых мы как обычно считаем
        заданные интервалы.
        Поэтому, например, если у нас dt_upto: ...T23:59:00 и group_type: day, то считаться будет
        вплоть до ...T00:00:00 следующего дня, не считая его. А если dt_upto: ...T00:00:00, то
        создастся новая запись, соответствующая новому периоду, с 
         сооветствующей summary (или 0, если выплат нет) в dataset.)
        Но мне кажется, что вариант с тем, чтобы dt_upto буквально задавал
        верхнюю границу выборки, а не верхнюю границу начала периода - интуитивнее.
        Однако логика тестов подразумевает другое.
        """  # noqa: E501
        start_date = datetime.fromisoformat(json_data["dt_from"])
        end_date = datetime.fromisoformat(json_data["dt_upto"])
        temp_end_date = copy.copy(start_date)

        while start_date <= end_date:

            if start_date == end_date:
                temp_end_date = end_date
            else:
                temp_end_date = start_date + interval

            filter_query = {
                "dt": {
                    "$gte": start_date,
                    "$lt": temp_end_date,
                },
            }

            docs = await db["payments"].find(filter_query).to_list(None)

            summary = 0

            for doc in docs:
                summary += doc["value"]

            result["dataset"].append(summary)
            result["labels"].append(start_date.isoformat())
            start_date += interval

        return json.dumps(result)

    match realization:
        case 1:
            return await realization_1()
        case 2:
            return await realization_2()
