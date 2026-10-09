"""A custom Python script as a pipeline step: `type: python`, `script: scripts/custom/x.py`.

The script defines `transform(df) -> pl.DataFrame`; `docs/custom-python-steps.md` is the contract.
Its bytes are read and hashed on every run, and those same bytes run, so the recorded hash says
exactly which code ran.

They run in a child Python process (`udp.script_child`), not in the pipeline's: the frame goes in
and comes back as an Arrow file, everything the script prints goes to a file kept with the step,
and when the step's timeout passes the child is killed. A thread could be neither stopped nor
kept from printing into another run's output. The child is the pipeline's own interpreter,
working folder, environment and network: it is not a sandbox.
"""

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import polars as pl
from pydantic import Field

from udp.errors import TransformError
from udp.pipeline.custom import TRANSFORM_TIME_LIMIT, TransformFile, check_columns, read_script
from udp.script_child import EXPLAINED
from udp.transformations.base import (
    Applied,
    ScriptRun,
    StepConfigError,
    StepContext,
    StepFailed,
    Transformation,
    changed_cells,
)
from udp.transformations.registry import register

# A script that prints in a loop must not fill the step record; the end of the output is kept,
# because what a script printed just before it failed is what explains the failure.
OUTPUT_LIMIT = 100_000
# A character is at most four bytes in UTF-8, so this much of the file's end always holds them.
_OUTPUT_BYTES = 4 * OUTPUT_LIMIT

_CHILD = "from udp.script_child import main; main()"


@register
class PythonStep(Transformation):
    type = "python"
    script: str = Field(min_length=1)
    timeout: float = Field(default=TRANSFORM_TIME_LIMIT, gt=0)

    def check(self, schema: pl.Schema) -> pl.Schema:
        """The script must be there. What columns it returns is only known once it has run, so
        the steps after it are checked again against its real result (`run_steps`)."""
        if not Path(self.script).is_file():
            raise StepConfigError("script", f"{self.script} is not a file")
        return schema

    def apply(self, frame: pl.DataFrame, context: StepContext) -> Applied:
        try:
            file = read_script(Path(self.script))
        except FileNotFoundError:
            raise StepFailed(f"{self.script}: the script is not there") from None
        except TransformError as error:
            raise StepFailed(str(error)) from None
        with tempfile.TemporaryDirectory(prefix="udp-python-step-") as folder:
            work = Path(folder)
            (work / "code.py").write_bytes(file.code)
            frame.write_ipc(work / "in.arrow")
            exit_code = self._run_child(work, file)
            output = _read_output(work / "output.txt")
            if exit_code is None:
                raise StepFailed(
                    f"{self.script}: took longer than {self.timeout:g}s and was stopped",
                    ScriptRun(file.sha256, output),
                )
            if exit_code != 0:
                message, line = _read_error(work / "error.json", exit_code)
                raise StepFailed(f"{self.script}: {message}", ScriptRun(file.sha256, output, line))
            result = pl.read_ipc(work / "out.arrow", memory_map=False)
        record = ScriptRun(file.sha256, output)
        try:
            check_columns(self.script, result.schema)
        except TransformError as error:
            raise StepFailed(str(error), record) from None
        return Applied(
            result,
            values_changed=_values_changed(frame, result),
            message=_schema_change(frame.schema, result.schema),
            script=record,
        )

    def _run_child(self, work: Path, file: TransformFile) -> int | None:
        """The child's exit code, or None when it ran out of time and was killed."""
        environment = {**os.environ, "PYTHONIOENCODING": "utf-8"}
        arguments = [str(work / "code.py"), file.name, str(work)]
        with (work / "output.txt").open("wb") as output:
            try:
                finished = subprocess.run(
                    [sys.executable, "-u", "-c", _CHILD, *arguments],
                    stdin=subprocess.DEVNULL,
                    stdout=output,
                    stderr=subprocess.STDOUT,
                    env=environment,
                    timeout=self.timeout,
                    check=False,
                )
            except subprocess.TimeoutExpired:
                return None
        return finished.returncode


def _read_error(path: Path, exit_code: int) -> tuple[str, int | None]:
    if exit_code == EXPLAINED and path.exists():
        error = json.loads(path.read_text())
        line = error["line"]
        message = error["message"] + (f" (line {line})" if line is not None else "")
        return message, line
    return f"the script's process ended with exit code {exit_code}", None


def _read_output(path: Path) -> str:
    """The end of what the script printed. Only the end of the file is read: a script printing
    in a loop until its timeout can write far more than the pipeline's memory holds."""
    size = path.stat().st_size
    with path.open("rb") as file:
        file.seek(max(0, size - _OUTPUT_BYTES))
        raw = file.read()
    # Windows writes a printed line end as \r\n; the record reads the same on every machine.
    # Postgres text cannot hold a NUL character, so a script printing binary loses those.
    text = raw.decode("utf-8", errors="replace").replace("\r\n", "\n").replace("\x00", "")
    if size <= _OUTPUT_BYTES and len(text) <= OUTPUT_LIMIT:
        return text
    return f"[earlier output left out; the script printed {size} bytes]\n" + text[-OUTPUT_LIMIT:]


def _values_changed(before: pl.DataFrame, after: pl.DataFrame) -> int:
    """Cells changed in the columns kept with the same type, when no row was added or removed;
    with a different row count, which row became which is unknown and nothing is counted."""
    if before.height != after.height:
        return 0
    return sum(
        changed_cells(before[name], after[name])
        for name, dtype in after.schema.items()
        if before.schema.get(name) == dtype
    )


def _schema_change(before: pl.Schema, after: pl.Schema) -> str | None:
    added = [name for name in after if name not in before]
    removed = [name for name in before if name not in after]
    retyped = [name for name in after if name in before and before[name] != after[name]]
    parts = [
        f"{label}: {', '.join(names)}"
        for label, names in (
            ("columns added", added),
            ("columns removed", removed),
            ("columns with a new type", retyped),
        )
        if names
    ]
    return "; ".join(parts) or None
