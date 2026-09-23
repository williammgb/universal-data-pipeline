"""What the API accepts and returns; these models are also what its OpenAPI document shows."""

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict

JsonValue = str | int | float | bool | None
RunStatus = Literal["running", "succeeded", "failed", "skipped"]
RunTrigger = Literal["manual", "scheduled"]


class Health(BaseModel):
    status: Literal["ok", "unavailable"]


class RunSummary(BaseModel):
    run_id: UUID
    status: RunStatus
    trigger: RunTrigger
    started_at: datetime
    ended_at: datetime | None
    rows_loaded: int | None = None


class SourceItem(BaseModel):
    source: str
    connector_type: str
    datasets: int
    recorded_at: datetime


class DatasetItem(BaseModel):
    source: str
    dataset: str
    connector_type: str
    table_name: str
    load_mode: str
    schedule: str | None
    recorded_at: datetime
    # Rows in the table now, counted exactly; None before the table exists.
    table_rows: int | None
    last_run: RunSummary | None


class SourceDetail(BaseModel):
    source: str
    connector_type: str
    connection: dict[str, Any]
    recorded_at: datetime
    datasets: list[DatasetItem]


class Column(BaseModel):
    name: str
    type: str


class SchemaVersion(BaseModel):
    version: int
    run_id: UUID
    recorded_at: datetime
    columns: list[Column]


class SavedState(BaseModel):
    watermark_column: str | None
    watermark_type: str | None
    watermark: str | None
    file_path: str | None
    file_sha256: str | None
    run_id: UUID
    saved_at: datetime


class DatasetDetail(DatasetItem):
    primary_key: list[str]
    watermark: str | None
    definition: dict[str, Any]
    columns: list[Column]
    versions: list[SchemaVersion]
    state: SavedState | None


class RowsPage(BaseModel):
    columns: list[Column]
    rows: list[dict[str, JsonValue]]
    limit: int
    offset: int
    has_more: bool


class ValueCount(BaseModel):
    value: JsonValue
    count: int


ProfileKind = Literal["number", "date", "text", "other"]


class ColumnProfile(BaseModel):
    name: str
    type: str
    kind: ProfileKind
    missing: int
    # number and date columns: the finite range, and a 20-bar histogram between its ends
    min: JsonValue = None
    max: JsonValue = None
    mean: JsonValue = None
    histogram: list[int] | None = None
    # text columns: every value when there are few, otherwise the most and least used
    distinct: int | None = None
    appear_once: int | None = None
    all_values: list[ValueCount] | None = None
    most_used: list[ValueCount] | None = None
    least_used: list[ValueCount] | None = None
    pattern: str | None = None
    pattern_share: float | None = None


class DatasetProfile(BaseModel):
    table_rows: int
    profiled_rows: int
    sampled: bool
    columns: list[ColumnProfile]


class CheckResult(BaseModel):
    position: int
    check_type: str
    columns: list[str]
    severity: Literal["warn", "error"]
    passed: bool
    failing_rows: int | None
    table_rows: int | None
    message: str
    settings: dict[str, Any]
    checked_at: datetime


class QualityReport(BaseModel):
    run_id: UUID | None
    results: list[CheckResult]


class RunItem(RunSummary):
    source: str
    dataset: str
    rows_extracted: int | None
    rows_loaded: int | None
    rows_quarantined: int | None
    error_class: str | None
    error_message: str | None


class RunsPage(BaseModel):
    runs: list[RunItem]
    limit: int
    offset: int
    has_more: bool


class RunDetail(RunItem):
    error_traceback: str | None
    quality: list[CheckResult]


class RunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: str
    dataset: str | None = None


class RunAccepted(BaseModel):
    source: str
    datasets: list[str]
    requested_at: datetime
