import { useState } from "react";
import { Link, useParams, useSearchParams } from "react-router";

import {
  PREVIEW_ROWS,
  useDataset,
  useQuality,
  useRows,
  useRuns,
  useStartRun,
  type DatasetDetail,
} from "../api/client";
import { cell, connectorName, count, describeSchedule, moment } from "../format";
import { runsQuery } from "../runFilters";
import { Blank, Fact, Problem, Status, Waiting } from "./parts";
import Profile from "./Profile";

const TABS = ["schema", "profile", "preview", "quality", "runs"] as const;
type Tab = (typeof TABS)[number];
const TAB_NAMES: Record<Tab, string> = {
  schema: "Schema & metadata",
  profile: "Profile",
  preview: "Preview",
  quality: "Quality",
  runs: "Runs",
};

export default function Dataset() {
  const { source = "", dataset = "" } = useParams();
  const [params] = useSearchParams();
  const asked = params.get("tab");
  const tab: Tab = TABS.includes(asked as Tab) ? (asked as Tab) : "schema";
  const detail = useDataset(source, dataset);
  const start = useStartRun(source, dataset);

  return (
    <main>
      <div className="head">
        <h1>
          {source} · {dataset}
        </h1>
        <div className="sub">datasets.{detail.data?.table_name ?? `${source}__${dataset}`}</div>
        <div className="right">
          {detail.data?.last_run ? <Status status={detail.data.last_run.status} /> : null}
          <button
            type="button"
            className="primary"
            disabled={start.isPending}
            onClick={() => start.mutate()}
          >
            {start.isPending ? "Starting…" : "Run now"}
          </button>
        </div>
      </div>
      {start.error ? <Problem error={start.error} /> : null}
      {start.data ? <p className="note">Run requested at {moment(start.data.requested_at)}.</p> : null}

      <div className="tabs">
        {TABS.map((name) => (
          <Link
            key={name}
            to={`/datasets/${source}/${dataset}?tab=${name}`}
            className={name === tab ? "current" : ""}
          >
            {TAB_NAMES[name]}
          </Link>
        ))}
      </div>

      {detail.isPending ? <Waiting /> : null}
      {detail.error ? <Problem error={detail.error} /> : null}
      {detail.data ? (
        <>
          {tab === "schema" ? <Schema detail={detail.data} /> : null}
          {tab === "profile" ? <Profile source={source} dataset={dataset} /> : null}
          {tab === "preview" ? <Preview source={source} dataset={dataset} /> : null}
          {tab === "quality" ? <Quality source={source} dataset={dataset} /> : null}
          {tab === "runs" ? <DatasetRuns source={source} dataset={dataset} /> : null}
        </>
      ) : null}
    </main>
  );
}

function Schema({ detail }: { detail: DatasetDetail }) {
  const declared = detail.definition["columns"];
  const declaredTypes = (declared ?? {}) as Record<string, string>;
  const version = detail.versions.at(-1);
  const plainSchedule = detail.schedule ? describeSchedule(detail.schedule) : null;
  return (
    <>
      <dl className="strip">
        <Fact label="Type">{connectorName(detail.connector_type)}</Fact>
        <Fact label="Load mode">
          {detail.load_mode}
          {detail.primary_key.length > 0 ? ` on ${detail.primary_key.join(", ")}` : ""}
        </Fact>
        <Fact label="Schedule">
          {detail.schedule ?? "none"}
          {plainSchedule ? ` (${plainSchedule})` : ""}
        </Fact>
        <Fact label="Rows">{count(detail.table_rows ?? null)}</Fact>
        <Fact label="Schema">
          {version ? `v${version.version} · ${moment(version.recorded_at)}` : "none yet"}
        </Fact>
        {detail.state?.file_path ? <Fact label="File">{detail.state.file_path}</Fact> : null}
        <Fact label="Watermark">
          {detail.watermark ? `${detail.watermark} at ${detail.state?.watermark ?? "—"}` : "none"}
        </Fact>
        <Fact label="Declared">
          {Object.keys(declaredTypes).length} of {detail.columns.length} columns
        </Fact>
      </dl>
      <div className="panel scroll">
        <table>
          <thead>
            <tr>
              <th>Column</th>
              <th>Stored type</th>
              <th>Declared in source.yaml</th>
            </tr>
          </thead>
          <tbody>
            {detail.columns.map((column) => (
              <tr key={column.name}>
                <td className="mono">{column.name}</td>
                <td className="mono">{column.type}</td>
                <td className="mono">
                  {declaredTypes[column.name] ?? (
                    <span className="null">
                      {column.name.startsWith("_") ? "platform column" : "inferred"}
                    </span>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  );
}

function Preview({ source, dataset }: { source: string; dataset: string }) {
  const [offset, setOffset] = useState(0);
  const page = useRows(source, dataset, offset);
  if (page.isPending) return <Waiting />;
  if (page.error) return <Problem error={page.error} />;
  if (!page.data) return null;
  const { columns, rows, has_more: hasMore } = page.data;
  return (
    <>
      <div className="panel scroll">
        <table>
          <thead>
            <tr>
              {columns.map((column) => (
                <th key={column.name} className="mono">
                  {column.name}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row, index) => (
              <tr key={index}>
                {columns.map((column) => {
                  const shown = cell(row[column.name] ?? null);
                  return (
                    <td key={column.name} className="mono">
                      {shown.isNull ? (
                        <span className="null">null</span>
                      ) : (
                        <span className="clip" title={shown.text}>
                          {shown.text}
                        </span>
                      )}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <div className="paging">
        <span>
          Rows {rows.length === 0 ? 0 : offset + 1}–{offset + rows.length} of this table
        </span>
        <span className="spacer" />
        <button
          type="button"
          disabled={offset === 0}
          onClick={() => setOffset(Math.max(0, offset - PREVIEW_ROWS))}
        >
          Previous
        </button>
        <button type="button" disabled={!hasMore} onClick={() => setOffset(offset + PREVIEW_ROWS)}>
          Next
        </button>
      </div>
    </>
  );
}

function Quality({ source, dataset }: { source: string; dataset: string }) {
  const report = useQuality(source, dataset);
  if (report.isPending) return <Waiting />;
  if (report.error) return <Problem error={report.error} />;
  if (!report.data) return null;
  if (report.data.run_id === null) return <p className="note">No run has checked this dataset.</p>;
  return (
    <>
      <p className="note">
        From run <code>{report.data.run_id}</code>.
      </p>
      <div className="panel scroll">
        <table>
          <thead>
            <tr>
              <th>Check</th>
              <th>Columns</th>
              <th>Severity</th>
              <th>Result</th>
              <th>Detail</th>
            </tr>
          </thead>
          <tbody>
            {report.data.results.map((result) => (
              <tr key={result.position}>
                <td className="mono">{result.check_type}</td>
                <td className="mono">
                  {result.columns.length > 0 ? result.columns.join(", ") : <Blank />}
                </td>
                <td>{result.severity}</td>
                <td>
                  <span
                    className={`pill ${result.passed ? "passed" : result.severity === "warn" ? "warn" : "failed"}`}
                  >
                    {result.passed ? "passed" : "failed"}
                  </span>
                </td>
                <td>{result.message}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  );
}

function DatasetRuns({ source, dataset }: { source: string; dataset: string }) {
  const runs = useRuns(runsQuery({ source, dataset }));
  if (runs.isPending) return <Waiting />;
  if (runs.error) return <Problem error={runs.error} />;
  if (!runs.data) return null;
  return (
    <div className="panel scroll">
      <table>
        <thead>
          <tr>
            <th>Started</th>
            <th>Trigger</th>
            <th>Status</th>
            <th className="mono">Extracted</th>
            <th className="mono">Loaded</th>
            <th className="mono">Quarantined</th>
          </tr>
        </thead>
        <tbody>
          {runs.data.runs.map((run) => (
            <tr key={run.run_id}>
              <td className="mono">
                <Link to={`/runs/${run.run_id}`}>{moment(run.started_at)}</Link>
              </td>
              <td>{run.trigger}</td>
              <td>
                <Status status={run.status} />
              </td>
              <td className="num">{count(run.rows_extracted)}</td>
              <td className="num">{count(run.rows_loaded)}</td>
              <td className="num">{count(run.rows_quarantined)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
