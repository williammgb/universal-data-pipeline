import { useProfile, type ColumnProfile, type ValueCount } from "../api/client";
import { count } from "../format";
import { Problem, Waiting } from "./parts";

type Value = string | number | boolean | null | undefined;

/** A range end as text: timestamps lose their "T" and seconds' fraction, numbers stay exact. */
function shown(value: Value): string {
  if (value === null || value === undefined) return "—";
  if (typeof value === "string" && /^\d{4}-\d{2}-\d{2}T/.test(value)) {
    return value.replace("T", " ").slice(0, 16);
  }
  return String(value);
}

function Histogram({ bars, label }: { bars: number[]; label: string }) {
  const width = 200 / bars.length;
  const top = Math.max(1, ...bars);
  return (
    <svg
      className="hist"
      viewBox="0 0 200 66"
      preserveAspectRatio="none"
      role="img"
      aria-label={label}
    >
      <line x1="0" y1="64.5" x2="200" y2="64.5" className="hist-base" />
      {bars.map((rows, index) => {
        const height = rows === 0 ? 0 : Math.max(1.5, (rows / top) * 60);
        return (
          <rect
            key={index}
            x={index * width + 1}
            y={64 - height}
            width={Math.max(0.5, width - 2)}
            height={height}
            rx="1"
            className="hist-bar"
          >
            <title>{`${count(rows)} rows`}</title>
          </rect>
        );
      })}
    </svg>
  );
}

function Missing({ missing, rows }: { missing: number; rows: number }) {
  const share = rows === 0 ? 0 : (missing / rows) * 100;
  return (
    <div className="missing">
      <span>
        {missing === 0 ? "none missing" : `${count(missing)} missing (${share.toFixed(2)}%)`}
      </span>
      <span className="track">
        <span className="fill" style={{ width: `${missing ? Math.max(1, share) : 0}%` }} />
      </span>
    </div>
  );
}

function Values({ title, values }: { title: string; values: ValueCount[] }) {
  return (
    <div>
      <h4>{title}</h4>
      <ol>
        {values.map((item) => (
          <li key={String(item.value)}>
            <span title={String(item.value)}>{String(item.value)}</span>
            <span>{count(item.count)}</span>
          </li>
        ))}
      </ol>
    </div>
  );
}

function Card({ column, rows }: { column: ColumnProfile; rows: number }) {
  const ranged = column.kind === "number" || column.kind === "date";
  const once = column.appear_once ?? 0;
  return (
    <article className="card" aria-label={`column ${column.name}`}>
      <header>
        <h3>{column.name}</h3>
        <span className="kind">{column.type}</span>
      </header>
      <Missing missing={column.missing} rows={rows} />
      {ranged ? (
        <>
          <div className="range">
            <div>
              {column.kind === "date" ? "earliest" : "min"}
              <span>{shown(column.min)}</span>
            </div>
            <div>
              {column.kind === "date" ? "latest" : "max"}
              <span>{shown(column.max)}</span>
            </div>
            {column.kind === "number" ? (
              <div>
                mean<span>{shown(column.mean)}</span>
              </div>
            ) : null}
          </div>
          {column.histogram ? (
            <>
              <Histogram bars={column.histogram} label={`histogram of ${column.name}`} />
              <div className="axis">
                <span>{shown(column.min)}</span>
                <span>{shown(column.max)}</span>
              </div>
            </>
          ) : null}
        </>
      ) : null}
      {column.pattern ? (
        <div>
          <div className="pattern">{column.pattern}</div>
          <small className="muted">
            {((column.pattern_share ?? 0) * 100).toFixed(1).replace(/\.0$/, "")}% match
          </small>
        </div>
      ) : null}
      {column.all_values ? (
        <div className="values one">
          <Values title="Every value" values={column.all_values} />
        </div>
      ) : null}
      {column.most_used ? (
        <div className={column.least_used ? "values" : "values one"}>
          {/* With every count equal there is no ranking, so the list is not called "most used". */}
          <Values title={column.least_used ? "Most used" : "Values"} values={column.most_used} />
          {column.least_used ? <Values title="Least used" values={column.least_used} /> : null}
        </div>
      ) : null}
      {column.kind === "text" ? (
        <p className="muted">
          {count(column.distinct ?? 0)} distinct values
          {once > 0 && once !== column.distinct ? ` · ${count(once)} appear once` : ""}
        </p>
      ) : null}
    </article>
  );
}

export default function Profile({ source, dataset }: { source: string; dataset: string }) {
  const profile = useProfile(source, dataset);
  if (profile.isPending) return <Waiting />;
  if (profile.error) return <Problem error={profile.error} />;
  if (!profile.data) return null;
  const { columns, profiled_rows: rows, table_rows: total, sampled } = profile.data;
  const withMissing = columns.filter((column) => column.missing > 0).length;
  return (
    <>
      <div className="summary">
        <div>
          <b>{count(total)}</b> rows
        </div>
        <div>
          <b>{columns.length}</b> columns profiled
        </div>
        <div>
          <b>{withMissing}</b> {withMissing === 1 ? "column" : "columns"} with missing values
        </div>
        <div>
          {sampled
            ? `profiled on a random ${count(rows)} of ${count(total)} rows`
            : "every row profiled"}
        </div>
      </div>
      <div className="cards">
        {columns.map((column) => (
          <Card key={column.name} column={column} rows={rows} />
        ))}
      </div>
    </>
  );
}
