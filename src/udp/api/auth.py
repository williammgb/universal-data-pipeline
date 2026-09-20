"""The API key check: one place that decides what needs a key and what counts as one.

Keys are compared byte for byte in constant time, so a wrong key tells a caller nothing about
how wrong it was. The health check stays open, because the container's own health probe uses
it and it says nothing about the data.
"""

import hmac
from collections.abc import Mapping, Sequence

HEADER = "X-API-Key"
OPEN_PATHS = ("/api/health",)


def parse_keys(text: str | None) -> tuple[str, ...]:
    """The keys in UDP_API_KEYS: separated by commas, spaces trimmed, blanks dropped.

    A key may therefore not contain a comma or begin or end with a space; `./run` mints hex
    keys, and the README says so for keys made by hand.
    """
    if not text:
        return ()
    return tuple(dict.fromkeys(key.strip() for key in text.split(",") if key.strip()))


def presented(headers: Mapping[str, str]) -> str | None:
    """The key a caller sent, from X-API-Key or an Authorization: Bearer header."""
    header = headers.get(HEADER) or headers.get(HEADER.lower())
    if header:
        return header
    scheme, _, value = headers.get("authorization", "").partition(" ")
    # Trimmed: a client that writes "Bearer  key" would otherwise present a key with a space
    # in front of it, which can never match.
    key = value.strip()
    return key if scheme.lower() == "bearer" and key else None


def accepted(keys: Sequence[str], key: str | None) -> bool:
    """With no keys configured the API is open; otherwise the key must match one exactly."""
    if not keys:
        return True
    if not key:
        return False
    # Compared as bytes: compare_digest refuses strings holding non-ASCII characters.
    given = key.encode()
    return any(hmac.compare_digest(given, configured.encode()) for configured in keys)


def needs_a_key(path: str) -> bool:
    """Everything the API serves needs a key; the dashboard's own pages and files do not."""
    return (path == "/api" or path.startswith("/api/")) and path not in OPEN_PATHS
