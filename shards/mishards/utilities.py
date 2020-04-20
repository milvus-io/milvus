import datetime
from mishards import exceptions


def format_date(start, end):
    return ((start.year - 1900) * 10000 + (start.month - 1) * 100 + start.day,
            (end.year - 1900) * 10000 + (end.month - 1) * 100 + end.day)


def range_to_date(range_obj, metadata=None):
    try:
        start = datetime.datetime.strptime(range_obj.start_date, '%Y-%m-%d')
        end = datetime.datetime.strptime(range_obj.end_date, '%Y-%m-%d')
        assert start < end
    except (ValueError, AssertionError):
        raise exceptions.InvalidRangeError('Invalid time range: {} {}'.format(
            range_obj.start_date, range_obj.end_date),
            metadata=metadata)

    return format_date(start, end)
