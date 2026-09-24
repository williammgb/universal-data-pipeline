import { useEffect, useState, type ReactNode } from "react";

import {
  ApiError,
  useConfig,
  useSaveConfig,
  useStartRun,
  type DatasetConfig,
} from "../api/client";
import { describeSchedule } from "../format";
import { Problem, Waiting } from "./parts";

/** The declared types a column can be given; `decimal` needs its digits, so it is typed out. */
const DECLARED_TYPES = ["text", "integer", "float", "boolean", "date", "timestamp", "json"];
const INFERRED = "";
const DECIMAL = "decimal(12,2)";

type Values = Record<string, unknown>;

function isDecimal(declared: string): boolean {
  return declared.startsWith("decimal");
}

/** The form's values as they start: what the dataset is using now. */
function starting(config: DatasetConfig): Values {
  return { ...config.effective, checks: config.checks_yaml };
}

function fileValues(config: DatasetConfig): Values {
  return { ...config.file, checks: config.file_checks_yaml };
}

function same(one: unknown, other: unknown): boolean {
  return JSON.stringify(one ?? null) === JSON.stringify(other ?? null);
}

function names(value: unknown): string[] {
  return Array.isArray(value) ? (value as string[]) : [];
}

function columnsOf(value: unknown): Record<string, string> {
  return (value ?? {}) as Record<string, string>;
}

function Group({ title, children }: { title: string; children: ReactNode }) {
  return (
    <section className="config-box">
      <h4>{title}</h4>
      {children}
    </section>
  );
}

/** One setting, with the label, whether it was edited, and how to put the file's value back. */
function Row({
  label,
  htmlFor,
  edited,
  onRevert,
  said,
  children,
}: {
  label: string;
  htmlFor?: string;
  edited: boolean;
  onRevert: () => void;
  said?: ReactNode;
  children: ReactNode;
}) {
  return (
    <div className={`config-row${edited ? " edited" : ""}`}>
      {/* The tag and the way back sit beside the label, not inside it: a button inside a label
          is a second thing that label names. */}
      <div className="config-label">
        <label htmlFor={htmlFor}>{label}</label>
        {edited ? <span className="tag">edited</span> : null}
        {edited ? (
          <button className="revert" type="button" onClick={onRevert}>
            use the file&rsquo;s value
          </button>
        ) : null}
      </div>
      {children}
      {said ? <div className="said">{said}</div> : null}
    </div>
  );
}

export default function Config({ source, dataset }: { source: string; dataset: string }) {
  const config = useConfig(source, dataset);
  const save = useSaveConfig(source, dataset);
  const rebuildAndRun = useStartRun(source, dataset, true);
  const [values, setValues] = useState<Values | null>(null);
  const [extra, setExtra] = useState("");

  // The form is filled once from what the platform answered, and again from what a save
  // answered. It is deliberately not refilled on every read: a read that lands while someone
  // is typing would throw away what they typed.
  useEffect(() => {
    setValues(null);
  }, [source, dataset]);
  useEffect(() => {
    if (config.data) setValues((held) => held ?? starting(config.data));
  }, [config.data]);
  useEffect(() => {
    if (save.data) setValues(starting(save.data));
  }, [save.data]);

  if (config.isPending) return <Waiting />;
  if (config.error) return <Problem error={config.error} />;
  if (!config.data || !values) return <Waiting />;

  const answer = config.data;
  const file = fileValues(answer);
  const editable = new Set(answer.editable);
  const set = (name: string, value: unknown) => setValues({ ...values, [name]: value });
  const revert = (name: string) => set(name, file[name]);
  const edited = (name: string) => !same(values[name], file[name]);
  const editedFields = answer.editable.filter(edited);

  const mode = String(values.load_mode ?? "full");
  const declared = columnsOf(values.columns);
  const excluded = names(values.exclude_columns);
  const stored = answer.columns.map((column) => column.name);
  const rows = [...new Set([...stored, ...Object.keys(declared)])];
  const schedule = String(values.schedule ?? "");
  const watermark = String(values.watermark ?? "");
  const refusal = save.error instanceof ApiError && save.error.status !== 409 ? save.error : null;
  const rebuild =
    save.error instanceof ApiError && save.error.status === 409
      ? save.error.message.split("\n")
      : null;

  /** A load mode carries its own fields: only append and merge have a watermark, only merge
   * has a primary key. They are cleared with the mode, so the tab never sends a pair the file
   * could not hold either. */
  function changeMode(chosen: string, held: Values) {
    setValues({
      ...held,
      load_mode: chosen,
      watermark: chosen === "full" ? null : (held.watermark ?? null),
      primary_key: chosen === "merge" ? (held.primary_key ?? []) : null,
    });
  }

  function setColumn(name: string, value: string) {
    const next = { ...declared };
    if (value === INFERRED) delete next[name];
    else next[name] = value;
    set("columns", next);
  }

  function addColumn() {
    const name = extra.trim();
    if (!name || name in declared) return;
    set("columns", { ...declared, [name]: "text" });
    setExtra("");
  }

  const submit = (acceptRebuild: boolean) =>
    save.mutate({ values, accept_rebuild: acceptRebuild });

  /** Save the change and rebuild the table in the same step, so the dataset is never left
   * needing a command line to work again. */
  const saveAndRebuild = () =>
    save.mutate({ values, accept_rebuild: true }, { onSuccess: () => rebuildAndRun.mutate() });

  return (
    <>
      <p className="note">
        sources/{source}/source.yaml is the starting point; edits made here are stored in the
        platform and take effect on this dataset&rsquo;s next run.
      </p>

      {refusal ? <div className="refusal">Not saved. {refusal.message}</div> : null}
      {rebuildAndRun.error ? (
        <div className="refusal">
          Saved, but the rebuild did not start. {(rebuildAndRun.error as Error).message}
        </div>
      ) : null}
      {rebuildAndRun.data ? (
        <p className="note">
          Saved. The table is being rebuilt; the run is on the Runs tab.
        </p>
      ) : null}

      {rebuild ? (
        <div className="confirm">
          <strong>This change needs the table rebuilt before it can be used.</strong>
          <ul>
            {rebuild.map((reason) => (
              <li key={reason}>{reason}</li>
            ))}
          </ul>
          <div className="actions">
            <div className="said">
              Rebuilding loads the dataset from the start. Saved without one, runs of this
              dataset fail until the table is rebuilt.
            </div>
            <button type="button" onClick={() => save.reset()}>
              Go back
            </button>
            <button type="button" onClick={() => submit(true)}>
              Save anyway
            </button>
            <button
              className="primary"
              type="button"
              disabled={save.isPending || rebuildAndRun.isPending}
              onClick={saveAndRebuild}
            >
              {rebuildAndRun.isPending ? "Starting…" : "Rebuild and run now"}
            </button>
          </div>
        </div>
      ) : null}

      <div className="config-boxes">
        <Group title="Schedule">
          <Row
            label="Cron"
            htmlFor="config-schedule"
            edited={edited("schedule")}
            onRevert={() => revert("schedule")}
            said={
              schedule
                ? describeSchedule(schedule)
                : "no schedule; this dataset runs only when it is started"
            }
          >
            <input
              id="config-schedule"
              type="text"
              value={schedule}
              placeholder="minute hour day month weekday"
              onChange={(event) => set("schedule", event.target.value || null)}
            />
          </Row>
        </Group>

        <Group title="Load">
          <Row
            label="Load mode"
            htmlFor="config-mode"
            edited={edited("load_mode")}
            onRevert={() => revert("load_mode")}
            said={
              mode === "full"
                ? "the whole table is replaced by every run"
                : mode === "append"
                  ? "rows above the watermark are added"
                  : "rows above the watermark are added or updated by their primary key"
            }
          >
            <select
              id="config-mode"
              value={mode}
              onChange={(event) => changeMode(event.target.value, values)}
            >
              <option value="full">full</option>
              <option value="append">append</option>
              <option value="merge">merge</option>
            </select>
          </Row>
          {mode === "full" ? null : (
            <Row
              label="Watermark"
              htmlFor="config-watermark"
              edited={edited("watermark")}
              onRevert={() => revert("watermark")}
            >
              <select
                id="config-watermark"
                value={watermark}
                onChange={(event) => set("watermark", event.target.value || null)}
              >
                <option value="">choose a column</option>
                {rows.map((name) => (
                  <option key={name} value={name}>
                    {name}
                  </option>
                ))}
              </select>
            </Row>
          )}
          {mode === "merge" ? (
            <Row
              label="Primary key"
              htmlFor="config-key"
              edited={edited("primary_key")}
              onRevert={() => revert("primary_key")}
              said="one column name per line"
            >
              <textarea
                id="config-key"
                rows={2}
                spellCheck={false}
                value={names(values.primary_key).join("\n")}
                onChange={(event) =>
                  set(
                    "primary_key",
                    event.target.value
                      .split("\n")
                      .map((name) => name.trim())
                      .filter(Boolean),
                  )
                }
              />
            </Row>
          ) : null}
        </Group>

        <Group title="Quarantine">
          <Row
            label="Threshold, percent of rows"
            htmlFor="config-threshold"
            edited={edited("quarantine_threshold_percent")}
            onRevert={() => revert("quarantine_threshold_percent")}
            said="a run fails once more than this share of its rows is quarantined"
          >
            <input
              id="config-threshold"
              type="number"
              min={0}
              max={100}
              step={0.5}
              value={String(values.quarantine_threshold_percent ?? 0)}
              onChange={(event) =>
                set("quarantine_threshold_percent", Number(event.target.value))
              }
            />
          </Row>
        </Group>
      </div>

      <section className="config-box wide">
        <h4>
          Columns
          {edited("columns") || edited("exclude_columns") ? <span className="tag">edited</span> : null}
          {edited("columns") ? (
            <button className="revert" type="button" onClick={() => revert("columns")}>
              use the file&rsquo;s declared types
            </button>
          ) : null}
        </h4>
        <div className="scroll">
          <table>
            <thead>
              <tr>
                <th>Column</th>
                <th>Stored as</th>
                <th>Declared</th>
                {editable.has("exclude_columns") ? <th>Read?</th> : null}
              </tr>
            </thead>
            <tbody>
              {rows.map((name) => {
                const type = answer.columns.find((column) => column.name === name)?.type;
                const value = declared[name] ?? INFERRED;
                return (
                  <tr key={name}>
                    <td className="mono">{name}</td>
                    <td className={type ? "mono" : "null"}>{type ?? "not in the table yet"}</td>
                    <td>
                      <select
                        aria-label={`Declared type for ${name}`}
                        value={isDecimal(value) ? "decimal" : value}
                        onChange={(event) =>
                          setColumn(
                            name,
                            event.target.value === "decimal" ? DECIMAL : event.target.value,
                          )
                        }
                      >
                        <option value={INFERRED}>inferred</option>
                        {DECLARED_TYPES.map((kind) => (
                          <option key={kind} value={kind}>
                            {kind}
                          </option>
                        ))}
                        <option value="decimal">decimal</option>
                      </select>
                      {isDecimal(value) ? (
                        <input
                          aria-label={`Digits for ${name}`}
                          type="text"
                          value={value}
                          onChange={(event) => setColumn(name, event.target.value)}
                        />
                      ) : null}
                    </td>
                    {editable.has("exclude_columns") ? (
                      <td>
                        <input
                          type="checkbox"
                          aria-label={`Read ${name}`}
                          checked={!excluded.includes(name)}
                          onChange={(event) =>
                            set(
                              "exclude_columns",
                              event.target.checked
                                ? excluded.filter((column) => column !== name)
                                : [...excluded, name],
                            )
                          }
                        />
                      </td>
                    ) : null}
                  </tr>
                );
              })}
              <tr>
                <td>
                  <input
                    type="text"
                    aria-label="Another column name"
                    placeholder="another column name"
                    value={extra}
                    onChange={(event) => setExtra(event.target.value)}
                  />
                </td>
                <td className="null">not in the table yet</td>
                <td>
                  <button type="button" onClick={addColumn} disabled={!extra.trim()}>
                    Declare it
                  </button>
                </td>
                {editable.has("exclude_columns") ? <td /> : null}
              </tr>
            </tbody>
          </table>
        </div>
      </section>

      <section className="config-box wide">
        <h4>
          Quality checks
          {edited("checks") ? <span className="tag">edited</span> : null}
          {edited("checks") ? (
            <button className="revert" type="button" onClick={() => revert("checks")}>
              use the file&rsquo;s value
            </button>
          ) : null}
        </h4>
        <label className="null" htmlFor="config-checks">
          written as YAML, exactly as the file holds them
        </label>
        <textarea
          id="config-checks"
          rows={8}
          spellCheck={false}
          value={String(values.checks ?? "")}
          onChange={(event) => set("checks", event.target.value)}
        />
      </section>

      <div className="actions">
        <div className="said">
          {editedFields.length === 0
            ? "Nothing is edited. This dataset runs exactly as its file says."
            : `${editedFields.length} of this dataset's settings differ from the file: ${editedFields.join(", ")}`}
        </div>
        <button
          type="button"
          disabled={editedFields.length === 0}
          onClick={() => setValues(fileValues(answer))}
        >
          Undo every edit
        </button>
        <button
          className="primary"
          type="button"
          disabled={save.isPending || same(values, starting(answer))}
          onClick={() => submit(false)}
        >
          {save.isPending ? "Saving…" : "Save"}
        </button>
      </div>

      <section className="config-box wide">
        <h4>Changes</h4>
        {answer.history.length === 0 ? (
          <p className="note">No edit has been saved for this dataset.</p>
        ) : (
          <div className="changes">
            {answer.history.map((edit) => (
              <div className="change" key={edit.changed_at}>
                <time dateTime={edit.changed_at}>
                  {edit.changed_at.replace("T", " ").slice(0, 19)}
                </time>
                <div className="what">
                  {Object.entries(edit.changed).map(([field, move]) => {
                    const step = move as { from: unknown; to: unknown };
                    return (
                      <div key={field}>
                        <b>{field}</b> <span>{JSON.stringify(step.from)}</span> to{" "}
                        <span>{JSON.stringify(step.to)}</span>
                      </div>
                    );
                  })}
                </div>
              </div>
            ))}
          </div>
        )}
      </section>
    </>
  );
}
