import bson
from datetime import datetime
from dateutil.relativedelta import relativedelta

import motor.motor_asyncio as motor

from .config import DB_HOST, DB_PASSWORD, DB_PORT, DB_USERNAME, DB_NAME
from .message_processor import json_schema

client = motor.AsyncIOMotorClient(f'{DB_HOST}:{DB_PORT}', username=DB_USERNAME, password=DB_PASSWORD)

db = client.get_database(DB_NAME)

async def start_db():
    if 'payments' not in await db.list_collection_names():
        async with await client.start_session() as session:
            payments_collection = db['payments']
            with open('sampleDB/sample_collection.bson', 'rb') as f:
                bson_data = f.read()
                data = bson.decode_all(bson_data)
            await payments_collection.insert_many(data, session=session)

async def process_json(json_data):
    intervals = {
        'hour': relativedelta(hours=1),
        'day': relativedelta(days=1),
        'month': relativedelta(months=1),
    }
    filter = {
        'dt': {
            '$gte': datetime.fromisoformat(json_data['dt_from']),
            '$lt': datetime.fromisoformat(json_data['dt_upto']),
        }
    }
    docs = await db['payments'].find(filter).to_list(None)

    result = {
        'dataset': [],
        'labels': [],
    }

    current_date = datetime.fromisoformat(json_data['dt_from'])
    finish_date = datetime.fromisoformat(json_data['dt_upto'])
    
    match json_data['group_type']:
        case 'hour':
            interval = intervals['hour']
        case 'day':
            interval = intervals['day']
        case 'month':
            interval = intervals['month']

    while current_date < finish_date:
        temp_end_date = current_date + interval
        if temp_end_date > finish_date:
            temp_end_date = finish_date
        summary = 0
        for doc in docs:
            if doc['dt'] >= current_date and doc['dt'] < temp_end_date:
                summary += doc['value']
        result['dataset'].append(summary)
        result['labels'].append(current_date.isoformat())
        current_date = temp_end_date

    return result
