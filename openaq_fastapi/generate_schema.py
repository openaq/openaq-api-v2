import json
import re
from typing import Dict

from fastapi.openapi.utils import get_openapi

from openaq_fastapi.main import app


def convert_to_3_1(schema: Dict) -> Dict:
    schema_replaced = re.sub('\"exclusiveMinimum\"\:\s*0\.0,', '"exclusiveMinimum":true,"minimum":0.0,', json.dumps(schema))
    new_schema = json.loads(schema_replaced)
    new_schema["openapi"] = "3.1.0"
    return new_schema

with open('openapi.json', 'w') as f:
    schema = get_openapi(
        title=app.title,
        version=app.version,
        openapi_version=app.openapi_version,
        description=app.description,
        servers=[{"url": "https://api.openaq.org"}],
        routes=app.routes,
    )
    json.dump(convert_to_3_1(schema), f)

