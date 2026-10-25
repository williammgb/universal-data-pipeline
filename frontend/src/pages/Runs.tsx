import { Link, useSearchParams } from "react-router";

import { useRuns, useSources, type RunItem, type RunKind } from "../api/client";
import { count, datasetTitle, fromInput, moment, sentence, toInput } from "../format";
import {
  RUNS_PER_PAGE,
  RUN_KINDS,
  RUN_STATUSES,
  RUN_TRIGGERS,
  parse,
  runsQuery,
  serialize,
} from "../runFilters";
import { Problem, RunError, Status, Waiting } from "./parts";

const KIND_NAMES: Record<RunKind, string> = { load: "Loads", pipeline: "Pipelines" };

/** A load opens its run page; a pipeline run opens the builder showing that run. */
function runAddress(run: RunItem): string {
  if (run.kind !== "pipeline" || !run.pipeline) return `/runs/${run.run_id}`;
  const params = new URLSearchParams({
    source: run.source,
    dataset: run.dataset,
    name: run.pipeline,
    run: run.run_id,
  });
  return `/pipeline?${params.toString()}`;
}

export default function Runs() {
  const [params, setParams] = useSearchParams();
  const filters = parse(params);
  const runs = useRuns(runsQuery(filters, true));
  const sources = useSources();
  const offset = filters.offset ?? 0;

  function change(field: string, value: string) {
    const next = new URLSearchParams(params);
    if (value) next.set(field, value);
    else next.delete(field);
    next.delete("offset");
    setParams(next, { replace: true });
  }

  function page(to: number) {
    const next = serialize({ ...filters, offset: to });
    setParams(next);
  }

  return (
    <main>
      <div className="head">
        <h1>Runs</h1>
        <span className="live" title="Refreshes every 5 seconds">
          Live
        </span>
      </div>

      <div className="controls">
        <label className="field" htmlFor="filter-source">
          Source
          <select
            id="filter-source"
            value={filters.source ?? ""}
            onChange={(event) => change("source", event.target.value)}
          >
            <option value="">All sources</option>
            {(sources.data ?? []).map((item) => (
              <option key={item.source} value={item.source}>
                {item.source}
              </option>
            ))}
          </select>
        </label>
        <label className="field" htmlFor="filter-kind">
          Kind
          <select
            id="filter-kind"
            value={filters.kind ?? ""}
            onChange={(event) => change("kind", event.target.value)}
          >
            <option value="">Loads and pipelines</option>
            {RUN_KINDS.map((kind) => (
              <option key={kind} value={kind}>
                {KIND_NAMES[kind]}
              </option>
            ))}
          </select>
        </label>
        <label className="field" htmlFor="filter-status">
          Status
          <select
            id="filter-status"
            value={filters.status ?? ""}
            onChange={(event) => change("status", event.target.value)}
          >
            <option value="">Any status</option>
            {RUN_STATUSES.map((status) => (
              <option key={status} value={status}>
                {sentence(status)}
              </option>
            ))}
          </select>
        </label>
        <label className="field" htmlFor="filter-trigger">
          Trigger
          <select
            id="filter-trigger"
            value={filters.trigger ?? ""}
            onChange={(event) => change("trigger", event.target.value)}
          >
            <option value="">Any trigger</option>
            {RUN_TRIGGERS.map((trigger) => (
              <option key={trigger} value={trigger}>
                {sentence(trigger)}
              </option>
            ))}
          </select>
        </label>
        <label className="field" htmlFor="filter-since">
          From
          <input
            id="filter-since"
            type="datetime-local"
            value={toInput(filters.since ?? "")}
            onChange={(event) => change("since", fromInput(event.target.value))}
          />
        </label>
        <label className="field" htmlFor="filter-until">
          To
          <input
            id="filter-until"
            type="datetime-local"
            value={toInput(filters.until ?? "")}
            onChange={(event) => change("until", fromInput(event.target.value))}
          />
        </label>
      </div>

      {runs.isPending ? <Waiting /> : null}
      {runs.error ? <Problem error={runs.error} /> : null}

      {runs.data ? (
        <>
          <div className="panel scroll">
            <table>
              <thead>
                <tr>
                  <th>Started</th>
                  <th>Dataset</th>
                  <th>Kind</th>
                  <th>Trigger</th>
                  <th>Status</th>
                  <th className="num">Rows</th>
                  <th>Error</th>
                </tr>
              </thead>
              <tbody>
                {runs.data.runs.map((run) => (
                  <tr key={run.run_id}>
                    <td className="mono">
                      <Link to={runAddress(run)}>{moment(run.started_at)}</Link>
                    </td>
                    <td>
                      <div className="named">
                        <span>{datasetTitle(run.dataset)}</span>
                        <span className="mono-id">
                          {run.source}.{run.dataset}
                        </span>
                      </div>
                    </td>
                    <td>
                      {run.kind === "pipeline" ? (
                        <div className="named">
                          <span className="kind pipeline">Pipeline</span>
                          <span className="mono-id">
                            {run.pipeline} v{run.version}
                          </span>
                        </div>
                      ) : (
                        <span className="kind">Load</span>
                      )}
                    </td>
                    <td>{sentence(run.trigger)}</td>
                    <td>
                      <Status status={run.status} />
                    </td>
                    <td className="num">
                      {run.kind === "pipeline" && run.rows_loaded !== null
                        ? `${count(run.rows_extracted)} → ${count(run.rows_loaded)}`
                        : count(run.rows_loaded)}
                    </td>
                    <td className="why">
                      <RunError run={run} />
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className="paging">
            <span>
              {runs.data.runs.length > 0
                ? `${offset + 1}–${offset + runs.data.runs.length}`
                : "No runs"}
            </span>
            <span className="spacer" />
            <button
              type="button"
              disabled={offset === 0}
              onClick={() => page(Math.max(0, offset - RUNS_PER_PAGE))}
            >
              Previous
            </button>
            <button
              type="button"
              disabled={!runs.data.has_more}
              onClick={() => page(offset + RUNS_PER_PAGE)}
            >
              Next
            </button>
          </div>
        </>
      ) : null}
    </main>
  );
}
