from dateutil.parser import parse
from dateutil.tz import UTC
from datetime import date


def fix_date(
    d: date | str | int | None,
):
    if isinstance(d, date):
        pass
    elif isinstance(d, str):
        if d == 'infinity':
            d = None
        elif d == '-infinity':
            d = None
        else:
            d = parse(d).date()
    elif isinstance(d, datetime):
        d = d.date()

    return d
