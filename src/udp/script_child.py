"""Running a custom script's code without importing it, and the child process a `python` step
runs its script in.

`compile_script` is the one way both kinds of custom code are loaded — a source's `transform.py`
(`udp.pipeline.custom`) and a pipeline's python step: the bytes that were hashed are compiled
under the script's own path and run in a fresh module, so nothing goes into `sys.modules` and no
`__pycache__` is written.

This module imports only polars and the standard library on purpose: a python step starts a new
interpreter for every run, and importing the rest of the platform would add seconds to each.

    python -c "from udp.script_child import main; main()" <code> <script name> <folder>

reads `in.arrow` from the folder, writes `out.arrow`, or writes `error.json` — the message a
person reads and the script line it came from — and exits with `EXPLAINED`.
"""

import json
import sys
import traceback
from collections.abc import Callable
from pathlib import Path
from types import ModuleType
from typing import cast

import polars as pl

FUNCTION = "transform"
# The exit code of a failure the child explained in error.json.
EXPLAINED = 3


class ScriptLoadError(Exception):
    """The script could not be compiled or run, or defines no `transform`."""


def compile_script(code: bytes, path: Path) -> Callable[..., object]:
    """The script's `transform`, from these exact bytes. A failure while compiling or running the
    module raises ScriptLoadError from the original error."""
    module = ModuleType("udp_custom_transform")
    module.__file__ = str(path)
    try:
        exec(compile(code, str(path), "exec", dont_inherit=True), module.__dict__)
    except (Exception, SystemExit) as error:
        raise ScriptLoadError(f"could not be loaded: {type(error).__name__}: {error}") from error
    function = module.__dict__.get(FUNCTION)
    if not callable(function):
        raise ScriptLoadError(f"defines no function '{FUNCTION}'")
    return cast(Callable[..., object], function)


def main() -> None:
    sys.exit(run(sys.argv[1:]))


def run(argv: list[str]) -> int:
    code_path, name, folder = argv
    work = Path(folder)
    path = Path(name)
    # The name the code is compiled under, which is what its tracebacks carry.
    compiled_as = str(path)
    try:
        function = compile_script(Path(code_path).read_bytes(), path)
    except ScriptLoadError as error:
        cause = error.__cause__
        return _explain(work, str(error), None if cause is None else _line(cause, compiled_as))
    frame = pl.read_ipc(work / "in.arrow", memory_map=False)
    try:
        result = function(frame)
    except (Exception, SystemExit) as error:
        message = f"transform failed: {type(error).__name__}: {error}"
        return _explain(work, message, _line(error, compiled_as))
    if not isinstance(result, pl.DataFrame):
        message = f"transform returned {type(result).__name__}, expected a polars DataFrame"
        return _explain(work, message, None)
    sys.stdout.flush()
    result.write_ipc(work / "out.arrow")
    return 0


def _line(error: BaseException, compiled_as: str) -> int | None:
    """The script line the error came from: the deepest frame that is the script's own."""
    if isinstance(error, SyntaxError) and error.filename == compiled_as:
        return error.lineno
    line = None
    for frame, number in traceback.walk_tb(error.__traceback__):
        if frame.f_code.co_filename == compiled_as:
            line = number
    return line


def _explain(work: Path, message: str, line: int | None) -> int:
    (work / "error.json").write_text(json.dumps({"message": message, "line": line}))
    return EXPLAINED
