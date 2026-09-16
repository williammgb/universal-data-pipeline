import { Link, useSearchParams } from "react-router";

import { useRuns, useSources } from "../api/client";
import { count, moment } from "../format";
import { RUNS_PER_PAGE, RUN_STATUSES, RUN_TRIGGERS, parse, runsQuery, serialize } from "../runFilters";
import { Blank, Problem, Status, Waiting } from "./parts";

export default function Runs() {
  const [params, setParams] = useSearchParams();
  const filters = parse(params);
  const runs = useRuns(runsQuery(filters));
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
        <div className="sub">filters are kept in the address</div>
      </div>

      <div className="controls">
        <label className="field" htmlFor="filter-source">
          Source
          <select
            id="filter-source"
            value={filters.source ?? ""}
            onChange={(event) => change("source", event.target.value)}
          >
            <option value="">all</option>
            {(sources.data ?? []).map((item) => (
              <option key={item.source} value={item.source}>
                {item.source}
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
            <option value="">all</option>
            {RUN_STATUSES.map((status) => (
              <option key={status} value={status}>
                {status}
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
            <option value="">all</option>
            {RUN_TRIGGERS.map((trigger) => (
              <option key={trigger} value={trigger}>
                {trigger}
              </option>
            ))}
          </select>
        </label>
        <label className="field" htmlFor="filter-since">
          From (UTC)
          <input
            id="filter-since"
            type="datetime-local"
            value={(filters.since ?? "").slice(0, 16)}
            onChange={(event) =>
              change("since", event.target.value ? `${event.target.value}:00Z` : "")
            }
          />
        </label>
        <label className="field" htmlFor="filter-until">
          To (UTC)
          <input
            id="filter-until"
            type="datetime-local"
            value={(filters.until ?? "").slice(0, 16)}
            onChange={(event) =>
              change("until", event.target.value ? `${event.target.value}:00Z` : "")
            }
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
                  <th>Source</th>
                  <th>Dataset</th>
                  <th>Trigger</th>
                  <th>Status</th>
                  <th className="mono">Loaded</th>
                  <th>Error</th>
                </tr>
              </thead>
              <tbody>
                {runs.data.runs.map((run) => (
                  <tr key={run.run_id}>
                    <td className="mono">
                      <Link to={`/runs/${run.run_id}`}>{moment(run.started_at)}</Link>
                    </td>
                    <td className="mono">{run.source}</td>
                    <td className="mono">{run.dataset}</td>
                    <td>{run.trigger}</td>
                    <td>
                      <Status status={run.status} />
                    </td>
                    <td className="num">{count(run.rows_loaded)}</td>
                    <td className="mono">
                      {run.error_class ? (
                        <span className="clip" title={run.error_message ?? run.error_class}>
                          {run.error_class}
                        </span>
                      ) : (
                        <Blank />
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className="paging">
            <span>{RUNS_PER_PAGE} per page · refreshing every 5s</span>
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
