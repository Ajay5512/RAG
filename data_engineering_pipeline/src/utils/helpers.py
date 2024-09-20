import re
from typing import Dict
from src.config import REPLACEMENTS
import json

def replace_strange_chars(text: str) -> str:
    """Replaces strange characters in a string with more standard equivalents."""
    for old, new in REPLACEMENTS.items():
        text = text.replace(old, new)
    return text

class MongoJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)  # Convert ObjectId to string
        return super(MongoJSONEncoder, self).default(obj)