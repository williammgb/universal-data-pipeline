import { Link, useSearchParams } from "react-router";

import { useDatasets, useSources } from "../api/client";
import { count, moment } from "../format";
import { Blank, Problem, Status, Waiting } from "./parts";

export default function Datasets() {
  const [params, setParams] = useSearchParams();
  const q = params.get("q") ?? "";
  const source = params.get("source") ?? "";
  const datasets = useDatasets(q, source);
  const sources = useSources();

  function set(field: string, value: string) {
    const next = new URLSearchParams(params);
    if (value) next.set(field, value);
    else next.delete(field);
    setParams(next, { replace: true });
  }

  return (
    <main>
      <div className="head">
        <h1>Datasets</h1>
        <div className="sub">
          {datasets.data ? `${datasets.data.length} datasets` : ""}
          {sources.data ? ` · ${sources.data.length} sources` : ""}
        </div>
      </div>

      <div className="controls">
        <input
          id="dataset-search"
          type="search"
          placeholder="Search source or dataset"
          value={q}
          onChange={(event) => set("q", event.target.value)}
        />
        <label className="field" htmlFor="source-filter">
          Source
          <select
            id="source-filter"
            value={source}
            onChange={(event) => set("source", event.target.value)}
          >
            <option value="">all</option>
            {(sources.data ?? []).map((item) => (
              <option key={item.source} value={item.source}>
                {item.source}
              </option>
            ))}
          </select>
        </label>
      </div>

      {datasets.isPending ? <Waiting /> : null}
      {datasets.error ? <Problem error={datasets.error} /> : null}

      {datasets.data ? (
        <div className="panel scroll">
          <table>
            <thead>
              <tr>
                <th>Source</th>
                <th>Dataset</th>
                <th>Load mode</th>
                <th>Schedule</th>
                <th>Last run</th>
                <th>Started</th>
                <th className="mono">Rows</th>
              </tr>
            </thead>
            <tbody>
              {datasets.data.map((item) => (
                <tr key={`${item.source}.${item.dataset}`}>
                  <td className="mono">{item.source}</td>
                  <td>
                    <Link to={`/datasets/${item.source}/${item.dataset}`}>{item.dataset}</Link>
                  </td>
                  <td>{item.load_mode}</td>
                  <td className="mono">{item.schedule ?? <span className="null">none</span>}</td>
                  <td>{item.last_run ? <Status status={item.last_run.status} /> : <Blank />}</td>
                  <td className="mono">
                    {item.last_run ? moment(item.last_run.started_at) : <Blank />}
                  </td>
                  <td className="num">{count(item.last_run?.rows_loaded ?? null)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : null}

      <p className="note">
        A dataset appears here after its first run, because the list is the copy each run makes of
        its <code>source.yaml</code>.
      </p>
    </main>
  );
}
