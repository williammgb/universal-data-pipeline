import time
from collections.abc import Callable, Sequence

import structlog

RETRY_WAITS = (1.0, 2.0, 4.0)

log = structlog.get_logger(step="extract")


def retry[T](
    call: Callable[[], T],
    *,
    transient: Callable[[Exception], bool],
    waits: Sequence[float],
    sleep: Callable[[float], None] = time.sleep,
) -> T:
    """Call, retrying transient errors once per wait. The last error is re-raised unchanged."""
    for attempt, wait in enumerate(waits, start=1):
        try:
            return call()
        except Exception as error:
            if not transient(error):
                raise
            # Only the error's class: driver messages can carry connection strings.
            log.warning(
                "retrying", attempt=attempt, wait_seconds=wait, error_class=type(error).__name__
            )
            sleep(wait)
    return call()
