import json
from collections.abc import Iterator
from contextlib import nullcontext
from pathlib import Path

import pytest
from fakes import MemoryLoader
from fastapi.testclient import TestClient
from hypothesis import given
from hypothesis import strategies as st
from typer.testing import CliRunner

from udp.api.app import create_app
from udp.api.auth import HEADER, accepted, needs_a_key, parse_keys, presented
from udp.api.catalog import PostgresCatalog
from udp.cli import app as cli

UNREACHABLE = "postgresql://x:x@127.0.0.1:1/x"
SOURCES = Path("sources")
# Under /api, so it needs a key, and it answers without a database: the check is what is being
# measured here, not how long a refused connection takes.
PROTECTED = "/api/openapi.json"

# Keys that are prefixes of one another, differ only in case, carry spaces or are not ASCII:
# every one of those is a way a sloppy comparison says yes when it should say no.
KEYS = st.sampled_from(
    ["k", "key", "keys", "KEY", "key ", " key", "kéy", "k€y", "x" * 64, "a,b", "0", "false"]
)
# What a caller can actually put in a header: HTTP trims the spaces around a value and refuses
# anything outside ASCII, so those keys are exercised against the check itself instead.
SENDABLE = st.sampled_from(["k", "key", "keys", "KEY", "x" * 64, "a,b", "0", "false"])


OPENED: list[PostgresCatalog] = []


@pytest.fixture(autouse=True)
def _give_back_every_connection() -> Iterator[None]:
    """Each app here holds a pool; left open, they use up the database's clients."""
    yield
    while OPENED:
        OPENED.pop().close()


def _client(keys: tuple[str, ...]) -> TestClient:
    catalog = PostgresCatalog(UNREACHABLE)
    OPENED.append(catalog)
    app = create_app(
        catalog,
        SOURCES,
        {},
        lambda: nullcontext(MemoryLoader()),
        None,
        keys,
    )
    return TestClient(app)


@given(
    st.lists(KEYS, min_size=1, max_size=4, unique=True),
    st.one_of(st.none(), KEYS, st.text(max_size=5)),
)
def test_a_key_is_accepted_exactly_when_it_is_one_of_the_configured_ones(
    configured: list[str], sent: str | None
) -> None:
    keys = tuple(configured)

    assert accepted(keys, sent) == (sent is not None and sent in keys)


# One app for every example: building a FastAPI app per request is what makes a property slow.
CONFIGURED = ("key", "keys", "KEY", "x" * 64, "a,b")
GUARDED = _client(CONFIGURED)


@given(st.one_of(st.none(), SENDABLE, st.text(alphabet="abcdefKEY", max_size=4)))
def test_a_request_is_refused_unless_it_carries_a_configured_key(sent: str | None) -> None:
    headers = {} if sent is None else {HEADER: sent}

    refused = GUARDED.get(PROTECTED, headers=headers).status_code == 401

    assert refused != (sent is not None and sent in CONFIGURED)


def test_a_bearer_token_is_the_same_key_by_another_name() -> None:
    good = GUARDED.get(PROTECTED, headers={"Authorization": f"Bearer {CONFIGURED[0]}"})
    bad = GUARDED.get(PROTECTED, headers={"Authorization": f"Basic {CONFIGURED[0]}"})

    assert good.status_code != 401
    assert bad.status_code == 401


def test_no_configured_key_leaves_every_address_open() -> None:
    for text in ("", None, ",, ", "   "):
        assert parse_keys(text) == ()
    client = _client(())

    assert client.get(PROTECTED).status_code != 401


def test_the_health_check_never_needs_a_key_but_everything_else_does(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Answered without waiting on a database that is not there: the key is what is being tested.
    monkeypatch.setattr(PostgresCatalog, "ping", lambda self: None)
    client = _client(("secret",))

    assert client.get("/api/health").status_code == 200
    for path in ("/api/datasets", "/api/runs", "/api/metrics", "/api/docs", "/api/openapi.json"):
        assert client.get(path).status_code == 401, path
    assert needs_a_key("/api") and not needs_a_key("/apiary") and not needs_a_key("/datasets/x/y")


def test_a_refusal_says_what_is_needed_and_never_repeats_the_key() -> None:
    client = _client(("secret",))

    refused = client.get(PROTECTED, headers={HEADER: "wrong"})

    assert refused.json() == {"detail": "an API key is required"}
    assert "secret" not in refused.text and "wrong" not in refused.text


def test_keys_are_read_as_written_and_a_key_is_taken_from_either_header() -> None:
    assert parse_keys(" one , two,,three ") == ("one", "two", "three")
    assert parse_keys("one,one") == ("one",)
    assert presented({HEADER: "from-header"}) == "from-header"
    assert presented({"authorization": "Bearer from-bearer"}) == "from-bearer"
    assert presented({"authorization": "Bearer"}) is None
    assert presented({}) is None


def test_the_api_description_is_the_same_with_and_without_keys() -> None:
    result = CliRunner().invoke(cli, ["openapi"], env={"UDP_DATABASE_URL": UNREACHABLE})
    with_keys = CliRunner().invoke(
        cli, ["openapi"], env={"UDP_DATABASE_URL": UNREACHABLE, "UDP_API_KEYS": "secret"}
    )

    assert result.exit_code == 0 and with_keys.exit_code == 0
    assert json.loads(result.stdout) == json.loads(with_keys.stdout)
