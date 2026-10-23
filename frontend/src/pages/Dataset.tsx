import { useEffect, useState } from "react";
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
import {
  cell,
  connectorName,
  count,
  datasetTitle,
  moment,
  sentence,
  typeName,
} from "../format";
import { runsQuery } from "../runFilters";
import Config from "./Config";
import Lineage from "./Lineage";
import { Blank, Fact, Problem, RunError, Schedule, Status, Waiting } from "./parts";
import Profile from "./Profile";

// Exported so the guide names the same tabs this page renders: renaming one here renames it
// there too, rather than leaving the guide describing a tab that no longer exists.
export const TABS = [
  "schema",
  "profile",
  "preview",
  "quality",
  "runs",
  "config",
  "lineage",
] as const;
type Tab = (typeof TABS)[number];
export const TAB_NAMES: Record<Tab, string> = {
  schema: "Schema",
  profile: "Profile",
  preview: "Preview",
  quality: "Quality",
  runs: "Runs",
  config: "Configuration",
  lineage: "Lineage",
};

/** How long the "Run requested" pop-up stays before it leaves by itself. */
const POPUP_MS = 6000;

export default function Dataset() {
  const { source = "", dataset = "" } = useParams();
  const [params] = useSearchParams();
  const asked = params.get("tab");
  const tab: Tab = TABS.includes(asked as Tab) ? (asked as Tab) : "schema";
  const detail = useDataset(source, dataset);
  const start = useStartRun(source, dataset);

  // The time in the pop-up under Run now, or null while there is none to show. Each accepted
  // start shows its own time for 6 seconds; pressing again, or leaving the page, clears the
  // timer of the one before. A start that failed has no time, so it shows no pop-up.
  const [requested, setRequested] = useState<string | null>(null);
  useEffect(() => {
    const at = start.data?.requested_at ?? null;
    setRequested(at);
    if (!at) return;
    const timer = setTimeout(() => setRequested(null), POPUP_MS);
    return () => clearTimeout(timer);
  }, [start.data]);

  return (
    <main>
      <nav className="crumbs" aria-label="Breadcrumb">
        <Link to="/">Datasets</Link>
        <span aria-hidden="true">/</span>
        <Link to={`/?source=${encodeURIComponent(source)}`}>{source}</Link>
      </nav>
      <div className="head">
        <div className="title-block">
          <div className="title-line">
            <h1>{datasetTitle(dataset)}</h1>
            {detail.data?.last_run ? <Status status={detail.data.last_run.status} /> : null}
          </div>
          <div className="title-line">
            <span className="mono-id">
              datasets.{detail.data?.table_name ?? `${source}__${dataset}`}
            </span>
            {detail.data ? (
              <span className="kind">{connectorName(detail.data.connector_type)}</span>
            ) : null}
          </div>
        </div>
        <div className="right">
          <Link
            className="button"
            to={`/pipeline?${new URLSearchParams({ source, dataset }).toString()}`}
          >
            Build pipeline
          </Link>
          <button
            type="button"
            className="primary"
            disabled={start.isPending}
            onClick={() => start.mutate()}
          >
            {start.isPending ? "Starting…" : "Run now"}
          </button>
          {requested ? (
            <div className="popup" role="status">
              Run requested at <span className="when">{moment(requested)}</span>
            </div>
          ) : null}
        </div>
      </div>
      {start.error ? <Problem error={start.error} /> : null}

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
          {tab === "preview" ? (
            <Preview source={source} dataset={dataset} total={detail.data.table_rows} />
          ) : null}
          {tab === "quality" ? <Quality source={source} dataset={dataset} /> : null}
          {tab === "runs" ? <DatasetRuns source={source} dataset={dataset} /> : null}
          {tab === "config" ? <Config source={source} dataset={dataset} /> : null}
          {tab === "lineage" ? <Lineage source={source} dataset={dataset} /> : null}
        </>
      ) : null}
    </main>
  );
}

function Schema({ detail }: { detail: DatasetDetail }) {
  const declared = detail.definition["columns"];
  const declaredTypes = (declared ?? {}) as Record<string, string>;
  // Before a load has stored the table, the declared columns are all there is to list.
  const stored = detail.columns.length > 0;
  const rows: { name: string; type: string | null }[] = stored
    ? detail.columns
    : Object.keys(declaredTypes).map((name) => ({ name, type: null }));
  const version = detail.versions.at(-1);
  const own = rows.filter((column) => !column.name.startsWith("_"));
  const platform = rows.filter((column) => column.name.startsWith("_"));
  const declaredCount = Object.keys(declaredTypes).length;
  return (
    <>
      <dl className="strip">
        <Fact label="Rows">{count(detail.table_rows ?? null)}</Fact>
        <Fact label="Last run">
          {detail.last_run ? (
            <>
              {moment(detail.last_run.started_at)}
              <small>{sentence(detail.last_run.trigger)}</small>
            </>
          ) : (
            "Never"
          )}
        </Fact>
        <Fact label="Load mode">
          {sentence(detail.load_mode)}
          {detail.primary_key.length > 0 ? (
            <small>on {detail.primary_key.join(", ")}</small>
          ) : null}
        </Fact>
        <Fact label="Schedule">
          <Schedule schedule={detail.schedule} />
        </Fact>
        <Fact label="Schema">
          {version ? (
            <>
              Version {version.version}
              <small>{moment(version.recorded_at)}</small>
            </>
          ) : (
            "Not stored yet"
          )}
        </Fact>
        {detail.state?.file_path ? (
          <div className="fact wide">
            <dt>File</dt>
            <dd className="mono-id">{detail.state.file_path}</dd>
          </div>
        ) : null}
        {detail.watermark ? (
          <Fact label="Watermark">
            {detail.watermark}
            <small>at {detail.state?.watermark ?? "—"}</small>
          </Fact>
        ) : null}
      </dl>
      <div className="section-head">
        <h2>Columns</h2>
        <span>
          {stored ? `${own.length} · ${declaredCount} declared` : `${declaredCount} declared`}
        </span>
      </div>
      <div className="panel scroll">
        <table>
          <thead>
            <tr>
              <th className="num">#</th>
              <th>Column</th>
              <th>Type</th>
              <th>Stored as</th>
              <th>Origin</th>
            </tr>
          </thead>
          <tbody>
            {own.map((column, index) => {
              const chosen = declaredTypes[column.name];
              return (
                <tr key={column.name}>
                  <td className="num muted">{index + 1}</td>
                  <td className="mono">{column.name}</td>
                  <td>{typeName(chosen ?? column.type)}</td>
                  <td className="mono muted">
                    {column.type ?? <span className="null">Not stored yet</span>}
                  </td>
                  <td>
                    {chosen ? (
                      <span className="kind declared">Declared</span>
                    ) : (
                      <span className="kind">Inferred</span>
                    )}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
        {platform.length > 0 ? (
          <details className="platform-columns">
            <summary>Platform columns · {platform.length}</summary>
            <table>
              <tbody>
                {platform.map((column) => (
                  <tr key={column.name}>
                    <td className="mono">{column.name}</td>
                    <td className="mono muted">{column.type}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </details>
        ) : null}
      </div>
    </>
  );
}

function Paging({
  first,
  last,
  total,
  offset,
  hasMore,
  go,
}: {
  first: number;
  last: number;
  total: number | null;
  offset: number;
  hasMore: boolean;
  go: (offset: number) => void;
}) {
  return (
    <div className="paging">
      <span>
        Rows {first}–{last} of this table
        {total === null ? "" : ` (total ${count(total)} rows)`}
      </span>
      <span className="spacer" />
      <button
        type="button"
        disabled={offset === 0}
        onClick={() => go(Math.max(0, offset - PREVIEW_ROWS))}
      >
        Previous
      </button>
      <button type="button" disabled={!hasMore} onClick={() => go(offset + PREVIEW_ROWS)}>
        Next
      </button>
    </div>
  );
}

function Preview({
  source,
  dataset,
  total,
}: {
  source: string;
  dataset: string;
  total: number | null;
}) {
  const [offset, setOffset] = useState(0);
  const page = useRows(source, dataset, offset);
  if (page.isPending) return <Waiting />;
  if (page.error) return <Problem error={page.error} />;
  if (!page.data) return null;
  const { columns, rows, has_more: hasMore } = page.data;
  // The same line and the same two buttons above the table and below it, so they cannot differ.
  const paging = (
    <Paging
      first={rows.length === 0 ? 0 : offset + 1}
      last={offset + rows.length}
      total={total}
      offset={offset}
      hasMore={hasMore}
      go={setOffset}
    />
  );
  return (
    <>
      {paging}
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
                      ) : shown.full !== shown.text ? (
                        <span className="clip json" title={shown.text}>
                          <span className="json-line">{shown.text}</span>
                          <span className="json-full">{shown.full}</span>
                        </span>
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
      {paging}
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
            <th>Error</th>
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
              <td className="why">
                <RunError run={run} />
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
