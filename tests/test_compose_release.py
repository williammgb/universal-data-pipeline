"""deploy/compose.release.yaml: the platform from the published image, with no source code."""

from pathlib import Path
from typing import Any

import yaml

RELEASE = Path(__file__).resolve().parent.parent / "deploy" / "compose.release.yaml"
IMAGE = "ghcr.io/williammgb/universal-data-pipeline:${UDP_VERSION:-latest}"


def _services() -> dict[str, dict[str, Any]]:
    services: dict[str, dict[str, Any]] = yaml.safe_load(RELEASE.read_text())["services"]
    return services


def test_every_app_service_pulls_the_published_image() -> None:
    on_image = [name for name in _services() if name != "postgres"]

    assert on_image == ["app", "scaffold", "migrate", "api", "scheduler"]
    for name in on_image:
        assert _services()[name]["image"] == IMAGE, name


def test_no_service_builds_from_source() -> None:
    assert [name for name, service in _services().items() if "build" in service] == []


def test_a_plain_up_starts_the_four_platform_services() -> None:
    started = {name for name, service in _services().items() if not service.get("profiles")}

    assert started == {"postgres", "migrate", "api", "scheduler"}


def test_sources_come_from_a_folder_next_to_the_file_read_only() -> None:
    assert "./sources:/app/sources:ro" in _services()["app"]["volumes"]


def test_every_app_service_but_scaffold_reads_sources_only() -> None:
    for name in ("app", "migrate", "api", "scheduler"):
        assert "./sources:/app/sources:ro" in _services()[name]["volumes"], name


def test_scaffold_writes_sources_and_runs_without_the_database() -> None:
    scaffold = _services()["scaffold"]

    assert scaffold["profiles"] == ["app"]
    assert scaffold["volumes"] == ["./sources:/app/sources", "./pipelines:/app/pipelines:ro"]
    # `run --no-deps` skips the dependencies; none at all keeps `up` from waiting on it.
    assert not scaffold.get("depends_on")
