"""deploy/install.sh and deploy/install.ps1: the one-line installers, run against a temporary home
folder with UDP_INSTALL_FROM pointing at deploy/, so nothing is downloaded. The PowerShell one
runs on Windows only, and always with UDP_SKIP_PATH=1: the tests never touch the real PATH."""

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

DEPLOY = Path(__file__).resolve().parent.parent / "deploy"
INSTALLED = ("compose.release.yaml", "udp")
KEPT = {
    ".env": "POSTGRES_PASSWORD='mine'\n",
    "sources/my_shop/source.yaml": "name: my_shop\n",
    "pipelines/daily.yaml": "name: daily\n",
    "backups/udp-1.dump": "dump\n",
}

windows_only = pytest.mark.skipif(sys.platform != "win32", reason="install.ps1 is for Windows")


def _tool(name: str) -> str:
    found = shutil.which(name)
    assert found is not None, f"{name} is not on the PATH"
    return found


def _env(**settings: str) -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if not k.startswith("UDP_")}
    env["UDP_INSTALL_FROM"] = DEPLOY.as_posix()
    return env | settings


def _sh(
    home: Path, *, script: str | None = None, **settings: str
) -> subprocess.CompletedProcess[str]:
    """Runs install.sh as `curl ... | sh` would, with HOME at the folder given; `script` runs
    first in the same shell, before the installer."""
    installer = (DEPLOY / "install.sh").read_text()
    sh = _tool("sh")
    return subprocess.run(
        [sh, "-s"] if script is None else [sh, "-c", f"{script}\n{installer}"],
        input=installer if script is None else None,
        env=_env(HOME=str(home), **settings),
        capture_output=True,
        text=True,
        timeout=60,
    )


def _ok(result: subprocess.CompletedProcess[str]) -> subprocess.CompletedProcess[str]:
    assert result.returncode == 0, result.stdout + result.stderr
    return result


def _seed(folder: Path) -> None:
    for name, text in KEPT.items():
        (folder / name).parent.mkdir(parents=True, exist_ok=True)
        (folder / name).write_text(text)


def _assert_kept(folder: Path) -> None:
    for name, text in KEPT.items():
        assert (folder / name).read_text() == text, name


def _on_path_after_sourcing(home: Path, rc: str) -> subprocess.CompletedProcess[str]:
    """What `command -v udp` finds in a new shell that read the shell file the installer wrote."""
    return subprocess.run(
        [_tool("sh"), "-c", '. "$HOME/$1" && command -v udp', "sh", rc],
        env=_env(HOME=str(home)),
        capture_output=True,
        text=True,
        timeout=60,
    )


def test_sh_puts_the_compose_file_and_an_executable_launcher_in_home_udp(tmp_path: Path) -> None:
    result = _ok(_sh(tmp_path, UDP_SKIP_PATH="1"))

    udp = tmp_path / "udp"
    for name in INSTALLED:
        assert (udp / name).read_bytes() == (DEPLOY / name).read_bytes(), name
    assert (udp / "sources").is_dir() and (udp / "pipelines").is_dir()
    executable = [_tool("sh"), "-c", '[ -x "$1" ]', "sh", (udp / "udp").as_posix()]
    _ok(subprocess.run(executable, capture_output=True, text=True))
    assert "udp up" in result.stdout
    assert not any(path.name.endswith(".download") for path in udp.iterdir())


def test_sh_two_runs_add_the_path_line_to_bashrc_once(tmp_path: Path) -> None:
    (tmp_path / ".bashrc").write_text("alias ll='ls -l'")  # no newline at the end

    _ok(_sh(tmp_path))
    result = _ok(_sh(tmp_path))

    lines = (tmp_path / ".bashrc").read_text().splitlines()
    assert lines[0] == "alias ll='ls -l'"
    assert [line for line in lines if line.endswith("# udp")] == lines[1:]
    assert len(lines) == 2
    assert not (tmp_path / ".zshrc").exists() and not (tmp_path / ".profile").exists()
    found = _ok(_on_path_after_sourcing(tmp_path, ".bashrc")).stdout.strip()
    assert found.endswith("/udp/udp")
    assert "Open a new terminal, then run: udp up" in result.stdout


def test_sh_writes_both_bashrc_and_zshrc_when_both_exist(tmp_path: Path) -> None:
    for rc in (".bashrc", ".zshrc"):
        (tmp_path / rc).write_text("# mine\n")

    _ok(_sh(tmp_path))

    for rc in (".bashrc", ".zshrc"):
        lines = (tmp_path / rc).read_text().splitlines()
        assert lines[0] == "# mine" and len(lines) == 2 and lines[1].endswith("# udp"), rc


def test_sh_with_neither_rc_file_writes_profile(tmp_path: Path) -> None:
    _ok(_sh(tmp_path))
    _ok(_sh(tmp_path))

    assert not (tmp_path / ".bashrc").exists() and not (tmp_path / ".zshrc").exists()
    assert len((tmp_path / ".profile").read_text().splitlines()) == 1
    _ok(_on_path_after_sourcing(tmp_path, ".profile"))


def test_sh_with_home_already_on_path_changes_no_shell_file(tmp_path: Path) -> None:
    (tmp_path / ".bashrc").write_text("# mine\n")
    (tmp_path / "udp").mkdir()

    result = _ok(_sh(tmp_path, script='PATH="$(cd "$HOME/udp" && pwd):$PATH"'))

    assert (tmp_path / ".bashrc").read_text() == "# mine\n"
    assert not (tmp_path / ".profile").exists()
    assert "Run: udp up" in result.stdout


def test_sh_second_run_replaces_the_launchers_and_keeps_the_data(tmp_path: Path) -> None:
    _ok(_sh(tmp_path, UDP_SKIP_PATH="1"))
    udp = tmp_path / "udp"
    _seed(udp)
    for name in INSTALLED:
        (udp / name).write_text("old\n")

    _ok(_sh(tmp_path, UDP_SKIP_PATH="1"))

    for name in INSTALLED:
        assert (udp / name).read_bytes() == (DEPLOY / name).read_bytes(), name
    _assert_kept(udp)


def test_sh_failed_fetch_leaves_the_files_from_before(tmp_path: Path) -> None:
    _ok(_sh(tmp_path, UDP_SKIP_PATH="1"))
    udp = tmp_path / "udp"
    _seed(udp)
    broken = tmp_path / "broken"
    broken.mkdir()
    (broken / "compose.release.yaml").write_text("new\n")  # and no udp to fetch

    result = _sh(tmp_path, UDP_SKIP_PATH="1", UDP_INSTALL_FROM=broken.as_posix())

    assert result.returncode != 0
    for name in INSTALLED:
        assert (udp / name).read_bytes() == (DEPLOY / name).read_bytes(), name
    _assert_kept(udp)
    assert not any(path.name.endswith(".download") for path in udp.iterdir())


def test_sh_home_with_a_space(tmp_path: Path) -> None:
    (tmp_path / ".bashrc").write_text("")
    home = tmp_path / "my udp"

    _ok(_sh(tmp_path, UDP_HOME=str(home)))

    for name in INSTALLED:
        assert (home / name).is_file(), name
    found = _ok(_on_path_after_sourcing(tmp_path, ".bashrc")).stdout.strip()
    assert found.endswith("/my udp/udp")


def _ps1(home: Path, source: Path = DEPLOY) -> subprocess.CompletedProcess[str]:
    """Runs install.ps1 as `irm ... | iex` would."""
    installer = (DEPLOY / "install.ps1").as_posix()
    return subprocess.run(
        [
            _tool("powershell"),
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-Command",
            f"Get-Content -LiteralPath '{installer}' -Raw | Invoke-Expression",
        ],
        env=_env(UDP_HOME=str(home), UDP_SKIP_PATH="1", UDP_INSTALL_FROM=str(source)),
        capture_output=True,
        text=True,
        timeout=120,
    )


@windows_only
def test_ps1_puts_both_launchers_and_the_compose_file_in_udp_home(tmp_path: Path) -> None:
    result = _ok(_ps1(tmp_path))

    for name in ("compose.release.yaml", "udp.cmd", "udp"):
        assert (tmp_path / name).read_bytes() == (DEPLOY / name).read_bytes(), name
    assert (tmp_path / "sources").is_dir() and (tmp_path / "pipelines").is_dir()
    assert "udp up" in result.stdout
    assert not any(path.name.endswith(".download") for path in tmp_path.iterdir())


@windows_only
def test_ps1_second_run_replaces_the_launchers_and_keeps_the_data(tmp_path: Path) -> None:
    _ok(_ps1(tmp_path))
    _seed(tmp_path)
    for name in ("compose.release.yaml", "udp.cmd", "udp"):
        (tmp_path / name).write_text("old\n")

    _ok(_ps1(tmp_path))

    for name in ("compose.release.yaml", "udp.cmd", "udp"):
        assert (tmp_path / name).read_bytes() == (DEPLOY / name).read_bytes(), name
    _assert_kept(tmp_path)


@windows_only
def test_ps1_writes_udp_cmd_with_crlf_when_it_arrives_with_lf(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    for name in ("compose.release.yaml", "udp"):
        shutil.copy(DEPLOY / name, raw / name)
    lf = (DEPLOY / "udp.cmd").read_bytes().replace(b"\r\n", b"\n")
    (raw / "udp.cmd").write_bytes(lf)
    home = tmp_path / "home"

    _ok(_ps1(home, raw))

    written = (home / "udp.cmd").read_bytes()
    assert written == lf.replace(b"\n", b"\r\n")
    assert (home / "udp").read_bytes() == (DEPLOY / "udp").read_bytes()  # sh keeps LF


@windows_only
def test_ps1_failed_fetch_leaves_the_files_from_before(tmp_path: Path) -> None:
    home = tmp_path / "my udp"  # a space in it, too
    _ok(_ps1(home))
    _seed(home)
    broken = tmp_path / "broken"
    broken.mkdir()
    (broken / "compose.release.yaml").write_text("new\n")  # and no launchers to fetch

    result = _ps1(home, broken)

    assert result.returncode != 0
    for name in ("compose.release.yaml", "udp.cmd", "udp"):
        assert (home / name).read_bytes() == (DEPLOY / name).read_bytes(), name
    _assert_kept(home)
    assert not any(path.name.endswith(".download") for path in home.iterdir())
