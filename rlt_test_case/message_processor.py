import json
from jsonschema import validate, ValidationError

from aiogram import types

json_schema = {
    'type' : 'object',
    'properties' : {
        'dt_from' : {
            'type' : 'string',
            'format': 'date-time',
        },
        'dt_upto' : {
            'type' : 'string',
            'format': 'date-time',
        },
        'group_type' : {
            'type' : 'string',
            'enum' : ['hour', 'day', 'month'],
        }
    },
    'required' : ['dt_from', 'dt_upto', 'group_type'],
}

def make_json(message_text: str):
    try:
        json_data = json.loads(message_text)
        return json_data
    except json.JSONDecodeError:
        return False

def is_valid_json(json_data: dict):
    try:
        validate(json_data, json_schema)
        return True
    except ValidationError:
        return False
