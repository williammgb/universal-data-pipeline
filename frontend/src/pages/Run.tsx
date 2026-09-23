import { Link, useParams } from "react-router";

import { useRun } from "../api/client";
import { count, duration, moment } from "../format";
import { Blank, Fact, Problem, Status, Waiting } from "./parts";

export default function Run() {
  const { runId = "" } = useParams();
  const run = useRun(runId);

  if (run.isPending)
    return (
      <main>
        <Waiting />
      </main>
    );
  if (run.error)
    return (
      <main>
        <Problem error={run.error} />
      </main>
    );
  if (!run.data) return null;
  const detail = run.data;

  return (
    <main>
      <div className="head">
        <h1>
          <Link to={`/datasets/${detail.source}/${detail.dataset}`}>
            {detail.source} · {detail.dataset}
          </Link>
        </h1>
        <div className="sub">{detail.run_id}</div>
        <div className="right">
          <Status status={detail.status} />
        </div>
      </div>

      {detail.error_class ? (
        <div className="banner">
          <strong>{detail.error_class}</strong> — {detail.error_message}
        </div>
      ) : null}

      <dl className="facts">
        <Fact label="Trigger">{detail.trigger}</Fact>
        <Fact label="Started">{moment(detail.started_at)}</Fact>
        <Fact label="Ended">
          {detail.ended_at
            ? `${moment(detail.ended_at)} · ${duration(detail.started_at, detail.ended_at)}`
            : "still running"}
        </Fact>
        <Fact label="Rows extracted">{count(detail.rows_extracted)}</Fact>
        <Fact label="Rows loaded">{count(detail.rows_loaded)}</Fact>
        <Fact label="Rows quarantined">{count(detail.rows_quarantined)}</Fact>
      </dl>

      {detail.error_traceback ? <pre className="trace">{detail.error_traceback}</pre> : null}

      {detail.quality.length > 0 ? (
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
              {detail.quality.map((result) => (
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
      ) : null}
    </main>
  );
}
