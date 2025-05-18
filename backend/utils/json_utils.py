# backend/utils/json_utils.py
import json
from datetime import date, datetime
from decimal import Decimal


class CustomJSONEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles common non-serializable types"""

    def default(self, obj): # type: ignore
        # Convert datetime to ISO format
        if isinstance(obj, datetime):
            return obj.isoformat()

        # Convert date to ISO format
        if isinstance(obj, date):
            return obj.isoformat()

        # Convert Decimal to float
        if isinstance(obj, Decimal):
            return float(obj)

        # Let the base class handle other types
        return super().default(obj)


def dumps(obj, **kwargs):
    """JSON dumps with custom encoder"""
    return json.dumps(obj, cls=CustomJSONEncoder, **kwargs)
