import { Link, useSearchParams } from "react-router";

import { useDatasets, useSources } from "../api/client";
import { connectorName, count, datasetTitle, moment, sentence } from "../format";
import { Blank, Problem, Schedule, Status, Waiting } from "./parts";

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
        <div className="title-block">
          <h1>Datasets</h1>
          <div className="sub">
            {datasets.data ? `${datasets.data.length} datasets` : ""}
            {sources.data ? ` in ${sources.data.length} sources` : ""}
          </div>
        </div>
      </div>

      <div className="controls">
        <label className="field" htmlFor="dataset-search">
          Search
          <input
            id="dataset-search"
            type="search"
            placeholder="Source or dataset"
            value={q}
            onChange={(event) => set("q", event.target.value)}
          />
        </label>
        <label className="field" htmlFor="source-filter">
          Source
          <select
            id="source-filter"
            value={source}
            onChange={(event) => set("source", event.target.value)}
          >
            <option value="">All sources</option>
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
                <th>Dataset</th>
                <th>Source</th>
                <th>Load mode</th>
                <th>Schedule</th>
                <th>Last run</th>
                <th className="num" title="Rows in the table now">
                  Rows
                </th>
              </tr>
            </thead>
            <tbody>
              {datasets.data.map((item) => (
                <tr key={`${item.source}.${item.dataset}`}>
                  <td>
                    <div className="named">
                      <Link to={`/datasets/${item.source}/${item.dataset}`}>
                        {datasetTitle(item.dataset)}
                      </Link>
                      <span className="mono-id">
                        {item.source}.{item.dataset}
                      </span>
                    </div>
                  </td>
                  <td>
                    <div className="with-kind">
                      <span>{item.source}</span>
                      <span className="kind">{connectorName(item.connector_type)}</span>
                    </div>
                  </td>
                  <td>{sentence(item.load_mode)}</td>
                  <td>
                    <Schedule schedule={item.schedule} />
                  </td>
                  <td>
                    {item.last_run ? (
                      <div className="named">
                        <Status status={item.last_run.status} />
                        <span className="mono-id">{moment(item.last_run.started_at)}</span>
                      </div>
                    ) : (
                      <Blank />
                    )}
                  </td>
                  <td className="num">{count(item.table_rows ?? null)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : null}
    </main>
  );
}
