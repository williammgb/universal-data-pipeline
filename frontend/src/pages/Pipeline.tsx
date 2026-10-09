import { useEffect, useMemo, useState } from "react";
import { useSearchParams } from "react-router";

import {
  ApiError,
  useDatasets,
  usePipeline,
  usePipelineRun,
  useSavePipeline,
  useStartPipelineRun,
  type PipelineRun,
  type ProfileRecord,
  type SavedPipeline,
  type StepRecord,
} from "../api/client";
import { count, moment } from "../format";
import { Fact, Problem, Status, Waiting } from "./parts";
import {
  CONSTRAINT_KINDS,
  STEP_KINDS,
  draftProblems,
  mappingFrom,
  mappingText,
  sameDraft,
  started,
  typedValue,
  type DraftProblem,
  type Field,
  type Kind,
  type Settings,
} from "./pipelineSteps";
import Profile from "./Profile";

type ProfileChoice = SavedPipeline["profile"];
const PROFILE_CHOICES: ProfileChoice[] = ["none", "ends", "every_step"];

/** What is saved: the draft without the keys that only tell React which block is which. */
interface Body {
  profile: ProfileChoice;
  constraints: Settings[];
  steps: Settings[];
}

interface Item {
  key: number;
  settings: Settings;
}

interface Draft {
  profile: ProfileChoice;
  constraints: Item[];
  steps: Item[];
}

let nextKey = 1;

function draftOf(body: Body): Draft {
  const keyed = (settings: Settings): Item => ({ key: nextKey++, settings });
  return {
    profile: body.profile,
    constraints: body.constraints.map(keyed),
    steps: body.steps.map(keyed),
  };
}

/** The settings a block's form shows: a field hidden by another's value is not sent. */
function shown(kind: Kind | undefined, settings: Settings): Settings {
  if (!kind) return settings;
  const kept = { ...settings };
  for (const field of kind.fields) {
    if (field.when && !field.when(settings)) delete kept[field.name];
  }
  return kept;
}

function bodyOf(draft: Draft): Body {
  return {
    profile: draft.profile,
    constraints: draft.constraints.map(({ settings }) =>
      shown(CONSTRAINT_KINDS[String(settings["constraint"])], settings),
    ),
    steps: draft.steps.map(({ settings }) => shown(STEP_KINDS[String(settings["type"])], settings)),
  };
}

function savedBody(saved: SavedPipeline): Body {
  return { profile: saved.profile, constraints: saved.constraints, steps: saved.steps };
}

// An unsaved draft is kept in the browser's storage, one per source, dataset and pipeline name,
// until it is saved; a reload picks it up again. Storage that is full or switched off only
// means a reload loses the draft.
const DRAFTS = "udp.pipelineDraft";

function draftKey(source: string, dataset: string, name: string): string {
  return `${DRAFTS}.${source}.${dataset}.${name}`;
}

function storedDraft(key: string): Body | null {
  try {
    const text = window.localStorage.getItem(key);
    if (!text) return null;
    const body = JSON.parse(text) as Partial<Body>;
    if (!Array.isArray(body.steps) || !Array.isArray(body.constraints)) return null;
    const profile = PROFILE_CHOICES.includes(body.profile as ProfileChoice)
      ? (body.profile as ProfileChoice)
      : "ends";
    return { profile, constraints: body.constraints, steps: body.steps };
  } catch {
    return null;
  }
}

function keepDraft(key: string, body: Body | null): void {
  try {
    if (body) window.localStorage.setItem(key, JSON.stringify(body));
    else window.localStorage.removeItem(key);
  } catch {
    // Without storage the draft lives only as long as the page.
  }
}

export default function Pipeline() {
  const [params, setParams] = useSearchParams();
  const source = params.get("source") ?? "";
  const dataset = params.get("dataset") ?? "";
  const name = params.get("name") || dataset;
  const runId = params.get("run") ?? "";
  const datasets = useDatasets("", "");
  const pipeline = usePipeline(source, dataset, name);

  function choose(value: string) {
    const [nextSource = "", nextDataset = ""] = value.split("/");
    setParams(nextSource ? { source: nextSource, dataset: nextDataset } : {});
  }

  /** A new name takes the draft along, so renaming before saving is saving under another name. */
  function rename(next: string) {
    const carried =
      storedDraft(draftKey(source, dataset, name)) ??
      (pipeline.data?.version != null ? savedBody(pipeline.data) : null);
    const target = draftKey(source, dataset, next);
    if (carried && !storedDraft(target)) keepDraft(target, carried);
    setParams({ source, dataset, name: next });
  }

  function ran(executionId: string) {
    setParams({ source, dataset, name, run: executionId });
  }

  const chosen = source ? `${source}/${dataset}` : "";
  const known = (datasets.data ?? []).map((item) => `${item.source}/${item.dataset}`);

  return (
    <main>
      <div className="head">
        <h1>Pipeline</h1>
        {source ? (
          <div className="sub">
            {source}.{dataset}
          </div>
        ) : null}
      </div>
      <div className="controls">
        <label className="field">
          Dataset
          <select value={chosen} onChange={(event) => choose(event.target.value)}>
            <option value="">—</option>
            {chosen && !known.includes(chosen) ? <option value={chosen}>{chosen}</option> : null}
            {(datasets.data ?? []).map((item) => (
              <option key={`${item.source}/${item.dataset}`} value={`${item.source}/${item.dataset}`}>
                {item.source} · {item.dataset}
              </option>
            ))}
          </select>
        </label>
        {source ? <NameField name={name} onCommit={rename} /> : null}
      </div>
      {datasets.error ? <Problem error={datasets.error} /> : null}
      {pipeline.isLoading ? <Waiting /> : null}
      {pipeline.error ? <Problem error={pipeline.error} /> : null}
      {pipeline.data ? (
        <Builder
          key={`${source}/${dataset}/${name}`}
          saved={pipeline.data}
          runId={runId}
          onRun={ran}
        />
      ) : null}
    </main>
  );
}

function NameField({ name, onCommit }: { name: string; onCommit: (name: string) => void }) {
  const [text, setText] = useState(name);
  useEffect(() => setText(name), [name]);
  const commit = () => {
    const next = text.trim();
    if (next && next !== name) onCommit(next);
    else setText(name);
  };
  return (
    <label className="field">
      Name
      <input
        className="mono"
        value={text}
        onChange={(event) => setText(event.target.value)}
        onBlur={commit}
        onKeyDown={(event) => {
          if (event.key === "Enter") commit();
        }}
      />
    </label>
  );
}

function Builder({
  saved,
  runId,
  onRun,
}: {
  saved: SavedPipeline;
  runId: string;
  onRun: (executionId: string) => void;
}) {
  const { source, dataset, name } = saved;
  const columns = saved.columns.map((column) => column.name);
  const storage = draftKey(source, dataset, name);
  const [draft, setDraft] = useState<Draft>(() =>
    draftOf(storedDraft(storage) ?? savedBody(saved)),
  );
  const [problems, setProblems] = useState<DraftProblem[]>([]);
  const [showProfile, setShowProfile] = useState(false);
  const save = useSavePipeline(source, dataset, name);
  const start = useStartPipelineRun(source, dataset, name);
  const run = usePipelineRun(runId);

  const body = bodyOf(draft);
  const savedNow = useMemo(() => bodyOf(draftOf(savedBody(saved))), [saved]);
  const changed = !sameDraft(body, savedNow);
  // A pipeline never saved can be saved as it is; a saved one only once it has changed.
  const dirty = saved.version == null || changed;
  const bodyText = JSON.stringify(body);

  // A draft that differs from what is saved is kept for a reload; one that matches is not.
  useEffect(() => {
    keepDraft(storage, changed ? (JSON.parse(bodyText) as Body) : null);
  }, [storage, changed, bodyText]);

  function change(next: Draft) {
    setDraft(next);
    setProblems([]);
    save.reset();
  }

  function changeStep(index: number, settings: Settings) {
    change({
      ...draft,
      steps: draft.steps.map((item, at) => (at === index ? { ...item, settings } : item)),
    });
  }

  function moveStep(index: number, by: -1 | 1) {
    const steps = [...draft.steps];
    const [moved] = steps.splice(index, 1);
    steps.splice(index + by, 0, moved!);
    change({ ...draft, steps });
  }

  function removeStep(index: number) {
    change({ ...draft, steps: draft.steps.filter((_, at) => at !== index) });
  }

  function addStep(kind: string) {
    const settings = started(STEP_KINDS, "type", kind, columns);
    change({ ...draft, steps: [...draft.steps, { key: nextKey++, settings }] });
  }

  function changeConstraint(index: number, settings: Settings) {
    change({
      ...draft,
      constraints: draft.constraints.map((item, at) =>
        at === index ? { ...item, settings } : item,
      ),
    });
  }

  function removeConstraint(index: number) {
    change({ ...draft, constraints: draft.constraints.filter((_, at) => at !== index) });
  }

  function addConstraint(kind: string) {
    const settings = started(CONSTRAINT_KINDS, "constraint", kind, columns);
    change({ ...draft, constraints: [...draft.constraints, { key: nextKey++, settings }] });
  }

  function saveDraft() {
    save.mutate(body, {
      // What came back carries every setting with its default, so the draft becomes exactly it.
      onSuccess: (stored) => setDraft(draftOf(savedBody(stored))),
      onError: (error) => setProblems(error instanceof ApiError ? draftProblems(error.detail) : []),
    });
  }

  function startRun() {
    start.mutate(undefined, { onSuccess: (accepted) => onRun(accepted.execution_id) });
  }

  // A run's result is drawn on the blocks only while they are the version it ran.
  const result =
    run.data &&
    run.data.source === source &&
    run.data.dataset === dataset &&
    run.data.pipeline === name &&
    run.data.version === saved.version &&
    !dirty
      ? run.data
      : null;
  const running = run.data?.status === "running" || start.isPending;
  const table = `${source}__${dataset}`;

  return (
    <>
      <div className="actions">
        <label className="field">
          Profiles
          <select
            value={draft.profile}
            onChange={(event) =>
              change({ ...draft, profile: event.target.value as ProfileChoice })
            }
          >
            {PROFILE_CHOICES.map((choice) => (
              <option key={choice} value={choice}>
                {choice.replace("_", " ")}
              </option>
            ))}
          </select>
        </label>
        <span className="said">
          {changed ? <span className="tag">unsaved</span> : null}{" "}
          {saved.version != null
            ? `version ${saved.version} · saved ${moment(saved.saved_at)}`
            : "never saved"}
        </span>
        <button type="button" disabled={!dirty || save.isPending} onClick={saveDraft}>
          {save.isPending ? "Saving…" : "Save"}
        </button>
        <button
          type="button"
          className="primary"
          disabled={dirty || saved.version == null || running}
          onClick={startRun}
        >
          {running ? "Running…" : "Run"}
        </button>
      </div>
      <SaveRefusal error={save.error} problems={problems} draft={draft} />
      {start.error ? <Problem error={start.error} /> : null}
      {run.error ? <Problem error={run.error} /> : null}
      {run.data ? (
        <p className="note">
          Run of version {run.data.version} started {moment(run.data.started_at)}:{" "}
          <Status status={run.data.status} />
        </p>
      ) : null}

      <details className="profile-box" onToggle={(event) => setShowProfile(event.currentTarget.open)}>
        <summary>Profile</summary>
        {showProfile ? <Profile source={source} dataset={dataset} /> : null}
      </details>

      <ol className="flow" aria-label="Blocks">
        <li>
          <section className="block end" aria-label="Source">
            <header>
              <span className="block-kind">Source</span>
              <h3>{source}</h3>
            </header>
          </section>
        </li>
        <li>
          <section className="block end" aria-label="RAW">
            <header>
              <span className="block-kind">RAW</span>
              <h3>raw.{table}</h3>
            </header>
            {result ? (
              <dl className="block-facts">
                <Fact label="Rows">{count(result.rows_in)}</Fact>
              </dl>
            ) : null}
          </section>
        </li>
        {draft.steps.map((item, index) => (
          <li key={item.key}>
            <StepBlock
              position={index + 1}
              last={index === draft.steps.length - 1}
              settings={item.settings}
              columns={columns}
              problems={problems.filter(
                (problem) => problem.part === "steps" && problem.position === index + 1,
              )}
              result={result?.steps.find((step) => step.position === index + 1)}
              onChange={(settings) => changeStep(index, settings)}
              onMove={(by) => moveStep(index, by)}
              onRemove={() => removeStep(index)}
            />
          </li>
        ))}
        <li>
          <Adder label="Step type" button="Add step" kinds={STEP_KINDS} onAdd={addStep} />
        </li>
        <li>
          <section className="block" aria-label="Validate">
            <header>
              <span className="block-kind">Validate</span>
              <h3>Constraints</h3>
            </header>
            {draft.constraints.map((item, index) => (
              <ConstraintBox
                key={item.key}
                position={index + 1}
                settings={item.settings}
                columns={columns}
                problems={problems.filter(
                  (problem) => problem.part === "constraints" && problem.position === index + 1,
                )}
                result={result?.validation.find((check) => check.position === index + 1)}
                onChange={(settings) => changeConstraint(index, settings)}
                onRemove={() => removeConstraint(index)}
              />
            ))}
            <Adder
              label="Constraint type"
              button="Add constraint"
              kinds={CONSTRAINT_KINDS}
              onAdd={addConstraint}
            />
          </section>
        </li>
        <li>
          <section className="block end" aria-label="CLEAN">
            <header>
              <span className="block-kind">CLEAN</span>
              <h3>clean.{table}</h3>
              {result ? <Status status={result.status} /> : null}
            </header>
            {result && result.status !== "running" ? (
              <dl className="block-facts">
                <Fact label="Rows">{count(result.rows_out)}</Fact>
              </dl>
            ) : null}
            {result?.status === "failed" && result.failed_step == null ? (
              <p className="refusal">{result.error}</p>
            ) : null}
          </section>
        </li>
      </ol>

      {result && result.status !== "running" ? <BeforeAfter run={result} /> : null}
    </>
  );
}

function SaveRefusal({
  error,
  problems,
  draft,
}: {
  error: Error | null;
  problems: DraftProblem[];
  draft: Draft;
}) {
  if (!error) return null;
  if (problems.length === 0) return <p className="refusal">Not saved. {error.message}</p>;
  const where = (problem: DraftProblem) => {
    if (problem.part === "name") return "Name";
    const items = problem.part === "steps" ? draft.steps : draft.constraints;
    const settings = items[problem.position - 1]?.settings ?? {};
    const kind = problem.part === "steps" ? settings["type"] : settings["constraint"];
    const noun = problem.part === "steps" ? "Step" : "Constraint";
    return `${noun} ${problem.position} (${String(kind)}): ${problem.field}`;
  };
  return (
    <div className="refusal" role="alert">
      Not saved.
      {problems.map((problem, at) => (
        <div key={at}>
          {where(problem)}: {problem.message}
        </div>
      ))}
    </div>
  );
}

function StepBlock({
  position,
  last,
  settings,
  columns,
  problems,
  result,
  onChange,
  onMove,
  onRemove,
}: {
  position: number;
  last: boolean;
  settings: Settings;
  columns: string[];
  problems: DraftProblem[];
  result: StepRecord | undefined;
  onChange: (settings: Settings) => void;
  onMove: (by: -1 | 1) => void;
  onRemove: () => void;
}) {
  const type = String(settings["type"]);
  const kind = STEP_KINDS[type];
  const failed = result?.status === "failed";
  return (
    <section
      className={`block${failed ? " failed" : ""}`}
      aria-label={`Step ${position} · ${kind?.label ?? type}`}
    >
      <header>
        <span className="block-kind">Step {position}</span>
        <h3>{kind?.label ?? type}</h3>
        {result ? <StepStatus status={result.status} /> : null}
        <span className="block-tools">
          <button
            type="button"
            aria-label={`Move step ${position} up`}
            disabled={position === 1}
            onClick={() => onMove(-1)}
          >
            ↑
          </button>
          <button
            type="button"
            aria-label={`Move step ${position} down`}
            disabled={last}
            onClick={() => onMove(1)}
          >
            ↓
          </button>
          <button type="button" aria-label={`Remove step ${position}`} onClick={onRemove}>
            ✕
          </button>
        </span>
      </header>
      {kind ? (
        <Fields kind={kind} settings={settings} columns={columns} problems={problems} onChange={onChange} />
      ) : (
        <pre className="block-settings">{JSON.stringify(settings, null, 2)}</pre>
      )}
      {result && result.status !== "not_run" && result.status !== "running" ? (
        <dl className="block-facts">
          <Fact label="Rows in">{count(result.rows_in ?? null)}</Fact>
          <Fact label="Rows out">{count(result.rows_out ?? null)}</Fact>
          <Fact label="Values changed">{count(result.values_changed ?? null)}</Fact>
        </dl>
      ) : null}
      {failed && result?.error ? <p className="refusal">{result.error}</p> : null}
    </section>
  );
}

function StepStatus({ status }: { status: StepRecord["status"] }) {
  return <span className={`pill ${status}`}>{status.replace("_", " ")}</span>;
}

function ConstraintBox({
  position,
  settings,
  columns,
  problems,
  result,
  onChange,
  onRemove,
}: {
  position: number;
  settings: Settings;
  columns: string[];
  problems: DraftProblem[];
  result: PipelineRun["validation"][number] | undefined;
  onChange: (settings: Settings) => void;
  onRemove: () => void;
}) {
  const type = String(settings["constraint"]);
  const kind = CONSTRAINT_KINDS[type];
  const withCritical: Kind | undefined = kind && {
    ...kind,
    fields: [...kind.fields, { name: "critical", label: "Critical", kind: "flag" }],
  };
  return (
    <div className="config-box" aria-label={`Constraint ${position} · ${kind?.label ?? type}`} role="group">
      <h4>
        {kind?.label ?? type}
        {result ? <span className={`pill ${result.passed ? "passed" : "failed"}`}>{result.passed ? "passed" : "failed"}</span> : null}
        <button
          type="button"
          className="revert"
          aria-label={`Remove constraint ${position}`}
          onClick={onRemove}
        >
          remove
        </button>
      </h4>
      {withCritical ? (
        <Fields
          kind={withCritical}
          settings={settings}
          columns={columns}
          problems={problems}
          onChange={onChange}
        />
      ) : (
        <pre className="block-settings">{JSON.stringify(settings, null, 2)}</pre>
      )}
      {result && !result.passed ? <p className="said">{result.message}</p> : null}
    </div>
  );
}

function Fields({
  kind,
  settings,
  columns,
  problems,
  onChange,
}: {
  kind: Kind;
  settings: Settings;
  columns: string[];
  problems: DraftProblem[];
  onChange: (settings: Settings) => void;
}) {
  const named = new Set(kind.fields.map((field) => field.name));
  const elsewhere = problems.filter((problem) => !named.has(problem.field.split(".")[0]!));
  const set = (name: string, value: unknown) => {
    const next = { ...settings };
    if (value === undefined) delete next[name];
    else next[name] = value;
    onChange(next);
  };
  return (
    <div className="block-fields">
      {kind.fields
        .filter((field) => !field.when || field.when(settings))
        .map((field) => (
          <div className="config-row" key={field.name}>
            <FieldControl
              field={field}
              value={settings[field.name]}
              columns={columns}
              onChange={(value) => set(field.name, value)}
            />
            {problems
              .filter((problem) => problem.field.split(".")[0] === field.name)
              .map((problem, at) => (
                <p className="refusal" key={at}>
                  {problem.field}: {problem.message}
                </p>
              ))}
          </div>
        ))}
      {elsewhere.map((problem, at) => (
        <p className="refusal" key={at}>
          {problem.field}: {problem.message}
        </p>
      ))}
    </div>
  );
}

function FieldControl({
  field,
  value,
  columns,
  onChange,
}: {
  field: Field;
  value: unknown;
  columns: string[];
  onChange: (value: unknown) => void;
}) {
  switch (field.kind) {
    case "column": {
      const current = typeof value === "string" ? value : "";
      const offered = current && !columns.includes(current) ? [current, ...columns] : columns;
      return (
        <label className="config-label">
          {field.label}
          <select value={current} onChange={(event) => onChange(event.target.value)}>
            {current ? null : <option value="">—</option>}
            {offered.map((column) => (
              <option key={column} value={column}>
                {column}
              </option>
            ))}
          </select>
        </label>
      );
    }
    case "columns": {
      const picked = Array.isArray(value) ? value.map(String) : [];
      const offered = [...columns, ...picked.filter((column) => !columns.includes(column))];
      const toggle = (column: string, on: boolean) => {
        const next = on
          ? offered.filter((name) => name === column || picked.includes(name))
          : picked.filter((name) => name !== column);
        onChange(next.length === 0 && field.optional ? undefined : next);
      };
      return (
        <fieldset className="column-picks">
          <legend className="config-label">{field.label}</legend>
          {offered.map((column) => (
            <label key={column} className="pick">
              <input
                type="checkbox"
                checked={picked.includes(column)}
                onChange={(event) => toggle(column, event.target.checked)}
              />
              {column}
            </label>
          ))}
        </fieldset>
      );
    }
    case "choice":
      return (
        <label className="config-label">
          {field.label}
          <select
            value={value == null ? "" : String(value)}
            onChange={(event) => onChange(event.target.value === "" ? null : event.target.value)}
          >
            {(field.choices ?? []).map((choice) => (
              <option key={choice} value={choice}>
                {choice === "" ? "none" : choice}
              </option>
            ))}
          </select>
        </label>
      );
    case "number":
      return (
        <label className="config-label">
          {field.label}
          <input
            type="number"
            value={typeof value === "number" ? value : ""}
            onChange={(event) =>
              onChange(event.target.value === "" ? undefined : Number(event.target.value))
            }
          />
        </label>
      );
    case "flag":
      return (
        <label className="config-label">
          <input
            type="checkbox"
            checked={value === true}
            onChange={(event) => onChange(event.target.checked)}
          />
          {field.label}
        </label>
      );
    case "mapping":
      return <MappingField label={field.label} value={value} onChange={onChange} />;
    case "values":
      return <ValuesField label={field.label} value={value} onChange={onChange} />;
    case "value":
      return (
        <label className="config-label">
          {field.label}
          <input
            value={value == null ? "" : String(value)}
            onChange={(event) => onChange(typedValue(event.target.value))}
          />
        </label>
      );
    case "text":
      return (
        <label className="config-label">
          {field.label}
          <input
            className="mono"
            value={typeof value === "string" ? value : ""}
            onChange={(event) => onChange(event.target.value)}
          />
        </label>
      );
  }
}

/** Typed as lines, kept as the mapping they make; the text itself stays as typed. */
function MappingField({
  label,
  value,
  onChange,
}: {
  label: string;
  value: unknown;
  onChange: (value: unknown) => void;
}) {
  const [text, setText] = useState(() => mappingText(value));
  return (
    <label className="config-label stacked">
      {label}
      <textarea
        rows={2}
        value={text}
        onChange={(event) => {
          setText(event.target.value);
          onChange(mappingFrom(event.target.value));
        }}
      />
    </label>
  );
}

function ValuesField({
  label,
  value,
  onChange,
}: {
  label: string;
  value: unknown;
  onChange: (value: unknown) => void;
}) {
  const [text, setText] = useState(() => (Array.isArray(value) ? value.join(", ") : ""));
  return (
    <label className="config-label">
      {label}
      <input
        value={text}
        onChange={(event) => {
          setText(event.target.value);
          onChange(
            event.target.value
              .split(",")
              .filter((part) => part.trim() !== "")
              .map((part) => typedValue(part.trim())),
          );
        }}
      />
    </label>
  );
}

function Adder({
  label,
  button,
  kinds,
  onAdd,
}: {
  label: string;
  button: string;
  kinds: Record<string, Kind>;
  onAdd: (kind: string) => void;
}) {
  const names = Object.keys(kinds);
  const [kind, setKind] = useState(names[0]!);
  return (
    <div className="adder">
      <select aria-label={label} value={kind} onChange={(event) => setKind(event.target.value)}>
        {names.map((name) => (
          <option key={name} value={name}>
            {kinds[name]!.label}
          </option>
        ))}
      </select>
      <button type="button" onClick={() => onAdd(kind)}>
        {button}
      </button>
    </div>
  );
}

const QUANTITIES: [string, keyof ProfileRecord][] = [
  ["Rows", "table_rows"],
  ["Missing values", "missing_values"],
  ["Invalid values", "invalid_values"],
  ["Outliers", "outliers"],
  ["Duplicates", "duplicates"],
];

/** The profile of the rows the run started from against the CLEAN table it ended with. */
function BeforeAfter({ run }: { run: PipelineRun }) {
  const before = run.profiles.find((profile) => profile.stage === "raw");
  const after = run.profiles.find((profile) => profile.stage === "clean");
  if (!before || !after) return null;
  return (
    <section className="panel scroll" aria-label="Before and after">
      <table>
        <thead>
          <tr>
            <th />
            <th className="num">Before</th>
            <th className="num">After</th>
            <th className="num">Change</th>
          </tr>
        </thead>
        <tbody>
          {QUANTITIES.map(([label, field]) => {
            const was = before[field] as number;
            const is = after[field] as number;
            const change = is - was;
            return (
              <tr key={field}>
                <td>{label}</td>
                <td className="num">{count(was)}</td>
                <td className="num">{count(is)}</td>
                <td className="num">{change > 0 ? `+${count(change)}` : count(change)}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </section>
  );
}
