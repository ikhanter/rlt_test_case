"""Process message to get JSON and validate it."""
import json
from jsonschema import validate, ValidationError


json_schema = {
    "type" : "object",
    "properties" : {
        "dt_from" : {
            "type" : "string",
            "format": "date-time",
        },
        "dt_upto" : {
            "type" : "string",
            "format": "date-time",
        },
        "group_type" : {
            "type" : "string",
            "enum" : ["hour", "day", "month"],
        },
    },
    "required" : ["dt_from", "dt_upto", "group_type"],
}

def make_json(message_text: str) -> dict | bool:
    """Try to get JSON from a message or return False."""
    try:
        return json.loads(message_text)
    except json.JSONDecodeError:
        return False

def is_valid_json(json_data: dict) -> bool:
    """Return True or False if inputted JSON is valid."""
    try:
        validate(json_data, json_schema)
    except ValidationError:
        return False
    return True
