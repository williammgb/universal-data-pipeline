import { useState } from "react";
import { Link, useSearchParams } from "react-router";

import { useLineage, type LineageNode, type LineageRun, type StepRecord } from "../api/client";
import { count, duration, moment } from "../format";
import { Blank, Fact, Problem, Waiting } from "./parts";

const KIND_NAMES: Record<LineageNode["kind"], string> = {
  source: "Source",
  raw: "Raw",
  step: "Step",
  clean: "Clean",
  table: "Table",
};

/** The address of the run's page: the builder, opened on the run's pipeline and the run. */
function runPage(source: string, dataset: string, run: LineageRun): string {
  const params = { source, dataset, name: run.pipeline, run: run.execution_id };
  return `/pipeline?${new URLSearchParams(params).toString()}`;
}

function shortId(id: string): string {
  return id.slice(0, 8);
}

function runLabel(run: LineageRun): string {
  return `${moment(run.started_at)} · ${run.pipeline} v${run.version} · ${run.status}`;
}

export default function Lineage({ source, dataset }: { source: string; dataset: string }) {
  const [params, setParams] = useSearchParams();
  const asked = params.get("run") ?? "";
  const lineage = useLineage(source, dataset, asked);

  if (lineage.isPending) return <Waiting />;
  if (lineage.error) return <Problem error={lineage.error} />;
  const { run, runs, chain } = lineage.data;

  return (
    <section className="lineage">
      {run ? (
        <div className="controls">
          <label className="field">
            Run
            <select
              value={run.execution_id}
              onChange={(event) => setParams({ tab: "lineage", run: event.target.value })}
            >
              {runs.some((item) => item.execution_id === run.execution_id) ? null : (
                <option value={run.execution_id}>{runLabel(run)}</option>
              )}
              {runs.map((item) => (
                <option key={item.execution_id} value={item.execution_id}>
                  {runLabel(item)}
                </option>
              ))}
            </select>
          </label>
        </div>
      ) : null}
      {chain.length === 0 ? <p className="note">No lineage yet.</p> : null}
      {chain.length > 0 ? (
        <Chain
          key={run?.execution_id ?? ""}
          source={source}
          dataset={dataset}
          run={run}
          chain={chain}
        />
      ) : null}
    </section>
  );
}

function Chain({
  source,
  dataset,
  run,
  chain,
}: {
  source: string;
  dataset: string;
  run: LineageRun | null;
  chain: LineageNode[];
}) {
  const steps = chain.filter((node) => node.step);
  const first = steps.find((node) => node.step?.status === "failed") ?? steps[0];
  const [picked, setPicked] = useState(first?.step?.position ?? null);
  const shown = steps.find((node) => node.step?.position === picked);
  // A run that failed outside every step — reading its input, a constraint, publishing — says so
  // once, above the chain; a step's failure is shown on the step.
  const failedElsewhere =
    run?.error && !steps.some((node) => node.step?.status === "failed") ? run.error : null;

  return (
    <>
      {failedElsewhere ? <pre className="lineage-error">{failedElsewhere}</pre> : null}
      <div className="lineage-layout">
        <ol className="lineage-chain" aria-label="Lineage">
          {chain.map((node, index) => (
            <li key={`${node.kind}-${node.step?.position ?? index}`}>
              {node.step ? (
                <StepNode
                  node={node}
                  step={node.step}
                  picked={node.step.position === picked}
                  onPick={() => setPicked(node.step?.position ?? null)}
                />
              ) : (
                <PlainNode node={node} source={source} dataset={dataset} run={run} />
              )}
            </li>
          ))}
        </ol>
        {shown?.step ? <StepDetail node={shown} step={shown.step} /> : null}
      </div>
    </>
  );
}

function PlainNode({
  node,
  source,
  dataset,
  run,
}: {
  node: LineageNode;
  source: string;
  dataset: string;
  run: LineageRun | null;
}) {
  const rows = node.profile?.table_rows;
  return (
    <div className={`lineage-node ${node.kind}`}>
      <span className="block-kind">{KIND_NAMES[node.kind]}</span>
      <span className="lineage-name">{node.name}</span>
      {rows != null ? <span className="lineage-meta">{count(rows)} rows</span> : null}
      {node.kind === "clean" && run ? (
        <span className="lineage-meta">
          Run <Link to={runPage(source, dataset, run)}>{shortId(run.execution_id)}</Link> ·{" "}
          {run.pipeline} v{run.version}
        </span>
      ) : null}
    </div>
  );
}

function StepNode({
  node,
  step,
  picked,
  onPick,
}: {
  node: LineageNode;
  step: StepRecord;
  picked: boolean;
  onPick: () => void;
}) {
  const script = step.configuration["script"];
  return (
    <button
      type="button"
      className={`lineage-node step ${step.status}${picked ? " picked" : ""}`}
      aria-pressed={picked}
      onClick={onPick}
    >
      <span className="lineage-row">
        <span className="block-kind">Step {step.position}</span>
        <StepStatus status={step.status} />
      </span>
      <span className="lineage-name">
        {node.name}
        {typeof script === "string" ? ` · ${script}` : ""}
      </span>
      {step.rows_in != null ? (
        <span className="lineage-meta">
          {count(step.rows_in)} rows · {count(step.values_changed ?? null)} changed
        </span>
      ) : null}
      {step.error ? <span className="lineage-step-error">{step.error}</span> : null}
    </button>
  );
}

function StepStatus({ status }: { status: StepRecord["status"] }) {
  return <span className={`pill ${status}`}>{status.replace("_", " ")}</span>;
}

/** A setting as one line: a list as its items, anything else as it was written. */
function setting(value: unknown): string {
  if (Array.isArray(value)) return value.map((item) => String(item)).join(", ");
  if (typeof value === "object" && value !== null) return JSON.stringify(value);
  return String(value);
}

function StepDetail({ node, step }: { node: LineageNode; step: StepRecord }) {
  const settings = step.configuration;
  const column = settings["column"] ?? settings["columns"];
  const method = settings["method"];
  const profile = node.profile;
  return (
    <aside className="panel lineage-detail" aria-label={`Step ${step.position}`}>
      <h2>
        Step {step.position} · {step.type} <StepStatus status={step.status} />
      </h2>
      <dl className="facts">
        <Fact label="Transformation">{step.type}</Fact>
        <Fact label="Column">{column == null ? <Blank /> : setting(column)}</Fact>
        <Fact label="Method">{method == null ? <Blank /> : setting(method)}</Fact>
        <Fact label="Input rows">{count(step.rows_in ?? null)}</Fact>
        <Fact label="Output rows">{count(step.rows_out ?? null)}</Fact>
        <Fact label="Values changed">{count(step.values_changed ?? null)}</Fact>
        <Fact label="Status">{step.status.replace("_", " ")}</Fact>
        <Fact label="Duration">
          {step.started_at ? duration(step.started_at, step.ended_at ?? null) : <Blank />}
        </Fact>
        {step.error_line != null ? <Fact label="Failing line">{step.error_line}</Fact> : null}
      </dl>
      <pre className="block-settings lineage-settings">
        {Object.entries(settings)
          .map(([name, value]) => `${name}: ${setting(value)}`)
          .join("\n")}
      </pre>
      {step.error ? <pre className="lineage-error">{step.error}</pre> : null}
      {profile ? (
        <>
          <h3>Profile after step {step.position}</h3>
          <dl className="facts">
            <Fact label="Rows">{count(profile.table_rows)}</Fact>
            <Fact label="Missing">{count(profile.missing_values)}</Fact>
            <Fact label="Invalid">{count(profile.invalid_values)}</Fact>
            <Fact label="Outliers">{count(profile.outliers)}</Fact>
            <Fact label="Duplicates">{count(profile.duplicates)}</Fact>
          </dl>
        </>
      ) : null}
    </aside>
  );
}
