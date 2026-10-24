"""deploy/udp and deploy/udp.cmd: the launchers beside compose.release.yaml, proven through their
dry run, from another folder, so no Docker is needed. The .cmd runs on Windows only."""

import os
import re
import shutil
import stat
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

import pytest

DEPLOY = Path(__file__).resolve().parent.parent / "deploy"
SECRETS = ("POSTGRES_PASSWORD", "UDP_API_KEYS")
HEX32 = re.compile(r"[0-9a-f]{32}")
NO_DOCKER = "Docker is not installed or not on PATH: see https://docs.docker.com/get-docker/"


@dataclass
class Launcher:
    kind: str
    folder: Path
    elsewhere: Path

    @property
    def shown(self) -> str:
        """The launcher's folder as the launcher itself prints it."""
        if self.kind == "cmd":
            return str(self.folder)
        found = subprocess.run(
            [_tool("sh"), "-c", 'cd "$1" && pwd', "sh", self.folder.as_posix()],
            capture_output=True,
            text=True,
            check=True,
        )
        return found.stdout.strip()

    @property
    def sep(self) -> str:
        return "\\" if self.kind == "cmd" else "/"

    @property
    def compose(self) -> str:
        return f"docker compose -f {self.shown}{self.sep}compose.release.yaml"

    def run(
        self,
        *arguments: str,
        exported: dict[str, str] | None = None,
        dry: bool = True,
        path: str | None = None,
    ) -> subprocess.CompletedProcess[str]:
        env = {k: v for k, v in os.environ.items() if k not in (*SECRETS, "UDP_DRY_RUN")}
        env |= exported or {}
        if dry:
            env["UDP_DRY_RUN"] = "1"
        if path is not None:
            env["PATH"] = path
        if self.kind == "cmd":
            command = [_tool("cmd"), "/c", str(self.folder / "udp.cmd"), *arguments]
        else:
            command = [_tool("sh"), (self.folder / "udp").as_posix(), *arguments]
        return subprocess.run(
            command, cwd=self.elsewhere, env=env, capture_output=True, text=True, timeout=60
        )

    def env_file(self) -> Path:
        return self.folder / ".env"


def _tool(name: str) -> str:
    found = shutil.which(name)
    assert found is not None, f"{name} is not on the PATH"
    return found


def _calls(result: subprocess.CompletedProcess[str]) -> list[str]:
    """What the launcher printed, without the first run's note about .env."""
    return [line for line in result.stdout.splitlines() if not line.startswith("Saved a new")]


@pytest.fixture(
    params=[
        "sh",
        pytest.param(
            "cmd",
            marks=pytest.mark.skipif(sys.platform != "win32", reason="udp.cmd is for Windows"),
        ),
    ]
)
def launcher(request: pytest.FixtureRequest, tmp_path: Path) -> Launcher:
    folder = tmp_path / "release"
    folder.mkdir()
    for name in ("udp", "udp.cmd", "compose.release.yaml"):
        shutil.copy(DEPLOY / name, folder / name)
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    return Launcher(request.param, folder, elsewhere)


@pytest.mark.parametrize(
    ("arguments", "called"),
    [
        (["up"], ["{C} up -d --wait"]),
        (["down"], ["{C} down"]),
        (["status"], ["{C} ps"]),
        (["logs"], ["{C} logs --tail 100"]),
        (["logs", "api", "-f"], ["{C} logs --tail 100 api -f"]),
        # A service compose does not know is compose's to report.
        (["logs", "nosuch"], ["{C} logs --tail 100 nosuch"]),
        (["restart"], ["{C} restart api scheduler"]),
        (["upgrade"], ["{C} pull", "{C} up -d --wait"]),
        (["load", "my_shop"], ["{C} run --rm app load my_shop"]),
        (["load", "my_shop", "--full-refresh"], ["{C} run --rm app load my_shop --full-refresh"]),
        (["pipeline", "run", "a b"], ['{C} run --rm app pipeline run "a b"']),
        (["profile", "my_shop"], ["{C} run --rm app profile my_shop"]),
        (["doctor"], ["{C} run --rm app doctor"]),
    ],
)
def test_each_command_prints_its_docker_calls(
    launcher: Launcher, arguments: list[str], called: list[str]
) -> None:
    result = launcher.run(*arguments)

    assert result.returncode == 0, result.stderr
    assert _calls(result) == [line.format(C=launcher.compose) for line in called]


def test_run_makes_the_same_calls_as_up(launcher: Launcher) -> None:
    up = launcher.run("up")
    run = launcher.run("run")

    assert run.returncode == 0, run.stderr
    assert _calls(run) == _calls(up) == [f"{launcher.compose} up -d --wait"]


def test_new_goes_to_the_scaffold_service(launcher: Launcher) -> None:
    result = launcher.run("new", "my_shop")

    # On Linux the file is written as the user, not as the container's root.
    user = ""
    if launcher.kind == "sh" and sys.platform == "linux":
        user = f" --user {os.getuid()}:{os.getgid()}"
    assert result.returncode == 0, result.stderr
    assert _calls(result) == [f"{launcher.compose} run --rm --no-deps{user} scaffold new my_shop"]


def test_backup_dumps_into_a_timestamped_file_beside_the_launcher(launcher: Launcher) -> None:
    result = launcher.run("backup")

    dump = re.escape(f"{launcher.shown}{launcher.sep}backups{launcher.sep}udp-")
    pattern = re.escape(f"{launcher.compose} exec -T postgres pg_dump -U udp -Fc udp > ")
    assert result.returncode == 0, result.stderr
    [called] = _calls(result)
    assert re.fullmatch(pattern + dump + r"\d{8}-\d{6}\.dump", called), called


def _opener(launcher: Launcher, url: str) -> str:
    if launcher.kind == "cmd":
        return f'start "" {url}'
    if sys.platform == "win32":
        return f'cmd.exe /c start "" {url}'
    if sys.platform == "darwin":
        return f"open {url}"
    # A machine with no desktop has no opener: the address is printed instead.
    return f"xdg-open {url}" if shutil.which("xdg-open") else url


def test_open_opens_the_dashboard_on_the_port_in_env(launcher: Launcher) -> None:
    default = launcher.run("open")
    launcher.env_file().write_text("POSTGRES_PASSWORD=p\nUDP_API_KEYS=k\nUDP_API_PORT=8123\n")
    moved = launcher.run("open")

    assert default.returncode == moved.returncode == 0, default.stderr + moved.stderr
    assert _calls(default) == [_opener(launcher, "http://127.0.0.1:8000")]
    assert _calls(moved) == [_opener(launcher, "http://127.0.0.1:8123")]


def test_the_first_run_writes_env_once_with_a_random_password_and_key(launcher: Launcher) -> None:
    first = launcher.run("status")
    written = launcher.env_file().read_text()
    second = launcher.run("status")

    assert first.returncode == second.returncode == 0, first.stderr + second.stderr
    values = dict(line.split("=", 1) for line in written.splitlines())
    assert set(values) == set(SECRETS)
    assert all(HEX32.fullmatch(value) for value in values.values()), values
    assert values["POSTGRES_PASSWORD"] != values["UDP_API_KEYS"]
    shown = f"{launcher.shown}{launcher.sep}.env"
    assert first.stdout.splitlines()[0] == (
        f"Saved a new password and API key in {shown}. Your API key: {values['UDP_API_KEYS']}"
    )
    # The second run finds .env and leaves it as it was.
    assert launcher.env_file().read_text() == written
    assert "Saved" not in second.stdout
    assert {"sources", "pipelines", "backups"} <= {p.name for p in launcher.folder.iterdir()}


def test_exported_secrets_are_written_instead_of_random_ones(launcher: Launcher) -> None:
    result = launcher.run("status", exported={"POSTGRES_PASSWORD": "mine-secret"})

    values = dict(line.split("=", 1) for line in launcher.env_file().read_text().splitlines())
    assert result.returncode == 0, result.stderr
    assert values["POSTGRES_PASSWORD"] == "mine-secret"
    assert HEX32.fullmatch(values["UDP_API_KEYS"])


def test_an_existing_env_is_never_rewritten(launcher: Launcher) -> None:
    launcher.env_file().write_text("POSTGRES_PASSWORD=kept\nUDP_API_KEYS=also-kept\n")

    result = launcher.run("up", exported={"POSTGRES_PASSWORD": "other"})

    assert result.returncode == 0, result.stderr
    assert launcher.env_file().read_text() == "POSTGRES_PASSWORD=kept\nUDP_API_KEYS=also-kept\n"


@pytest.mark.parametrize("extra", [["-v"], ["anything"], ["-v", "--remove-orphans"]])
def test_down_takes_no_arguments_so_it_can_never_delete_the_data(
    launcher: Launcher, extra: list[str]
) -> None:
    result = launcher.run("down", *extra)

    assert result.returncode == 2
    assert result.stderr.strip() == "usage: udp down"
    assert not [line for line in _calls(result) if line.startswith("docker")]


@pytest.mark.parametrize("arguments", [[], ["help"]])
def test_no_command_or_help_lists_the_commands(launcher: Launcher, arguments: list[str]) -> None:
    result = launcher.run(*arguments)

    listed = set(re.findall(r"^  udp (\w+)", result.stdout, re.MULTILINE))
    assert result.returncode == 0, result.stderr
    assert listed == {
        "up",
        "down",
        "status",
        "logs",
        "open",
        "restart",
        "upgrade",
        "backup",
        "new",
        "help",
    }
    assert "running the installer again" in result.stdout
    # help is the one command that writes nothing.
    assert not launcher.env_file().exists()


def test_without_docker_on_the_path_it_says_where_to_get_it(
    launcher: Launcher, tmp_path: Path
) -> None:
    empty = tmp_path / "no-docker"
    empty.mkdir()

    result = launcher.run("status", dry=False, path=str(empty))

    assert result.returncode == 1
    assert result.stderr.strip() == NO_DOCKER
    assert result.stdout == ""
    assert not launcher.env_file().exists()


@pytest.mark.skipif(sys.platform == "win32", reason="Windows has no file modes")
def test_env_is_readable_by_its_owner_only(launcher: Launcher) -> None:
    launcher.run("status")

    assert stat.S_IMODE(launcher.env_file().stat().st_mode) == 0o600
