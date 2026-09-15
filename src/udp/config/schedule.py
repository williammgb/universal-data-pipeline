"""A dataset's `schedule:`, a 5-field cron expression read in UTC.

APScheduler 3 reads some cron expressions differently from cron itself: weekday numbers start
on Monday, a day of month and a weekday must both match, a name silently drops what follows it
(`jan-3` is read as `jan`, `jan-mar/2` as `jan-mar`), and a step larger than its range is
refused. Only expressions both read the same way are accepted: weekdays by name, steps on
numbers only, and at most one of the two day fields set.
"""

from datetime import UTC
from typing import Annotated

from apscheduler.triggers.cron import CronTrigger
from pydantic import AfterValidator

MONTHS = ("jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec")
WEEKDAYS = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")
_LONGEST_MONTH = (31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)


def _value(text: str, field: str, low: int, high: int, names: tuple[str, ...]) -> int:
    if text in names:
        return names.index(text) + low
    if not (text.isascii() and text.isdigit()):
        or_name = f" or a name like {names[0]}" if names else ""
        raise ValueError(f"{field} '{text}' is not a number{or_name}")
    value = int(text)
    if not low <= value <= high:
        raise ValueError(f"{field} {value} is outside {low}-{high}")
    return value


def _numbers(text: str, field: str, low: int, high: int, names: tuple[str, ...] = ()) -> set[int]:
    """The values a comma list of `*`, `n`, `a-b`, `*/s` and `a-b/s` parts selects."""
    values: set[int] = set()
    for part in text.split(","):
        base, slash, step_text = part.partition("/")
        if base == "*":
            first, last = low, high
        else:
            first_text, dash, last_text = base.partition("-")
            first = _value(first_text, field, low, high, names)
            last = _value(last_text, field, low, high, names) if dash else first
            if first > last:
                raise ValueError(f"{field} range '{base}' runs backwards")
            if slash and not dash:
                raise ValueError(f"{field} '{part}' needs * or a range before the /")
            if slash and (first_text in names or last_text in names):
                raise ValueError(f"{field} '{part}' can only step over numbers")
        step = 1
        if slash:
            if not (step_text.isascii() and step_text.isdigit()) or int(step_text) == 0:
                raise ValueError(f"{field} '{part}' needs a step of at least 1")
            step = int(step_text)
            if step > last - first:
                raise ValueError(f"{field} '{part}' steps further than its range {first}-{last}")
        values.update(range(first, last + 1, step))
    return values


def _weekdays(text: str) -> None:
    for part in text.split(","):
        first, dash, last = part.partition("-")
        if first not in WEEKDAYS or (dash and last not in WEEKDAYS):
            raise ValueError(f"day of week '{part}' is not a day name; use day names like mon-fri")
        if dash and WEEKDAYS.index(first) > WEEKDAYS.index(last):
            raise ValueError(f"day of week range '{part}' runs backwards; weeks start on mon")


def check_schedule(expression: str) -> str:
    fields = expression.split()
    if len(fields) != 5:
        raise ValueError(
            f"has {len(fields)} fields; a schedule is 5: minute hour day-of-month month "
            "day-of-week, in UTC"
        )
    minute, hour, day, month, weekday = fields
    _numbers(minute, "minute", 0, 59)
    _numbers(hour, "hour", 0, 23)
    days = _numbers(day, "day of month", 1, 31)
    months = _numbers(month, "month", 1, 12, MONTHS)
    if weekday != "*":
        _weekdays(weekday)
        if day != "*":
            raise ValueError("set day of month or day of week, not both")
    if not any(number <= _LONGEST_MONTH[m - 1] for number in days for m in months):
        raise ValueError(f"day of month '{day}' never occurs in month '{month}'")
    return expression


CronSchedule = Annotated[str, AfterValidator(check_schedule)]


def cron_trigger(expression: str) -> CronTrigger:
    return CronTrigger.from_crontab(expression, timezone=UTC)
