import type { ReactNode } from "react";

import type { RunItem } from "../api/client";
import { moment } from "../format";

export function Status({ status }: { status: RunItem["status"] }) {
  return <span className={`pill ${status}`}>{status}</span>;
}

export function Problem({ error }: { error: unknown }) {
  const message = error instanceof Error ? error.message : String(error);
  return <p className="note">{message}</p>;
}

export function Waiting() {
  return <p className="note">Loading…</p>;
}

export function Fact({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div className="fact">
      <dt>{label}</dt>
      <dd>{children}</dd>
    </div>
  );
}

export function Moment({ value }: { value: string | null }) {
  return <>{moment(value)}</>;
}

export function Blank() {
  return <span className="null">—</span>;
}

/** Why a run failed, for a cell in a run list: the error's name, then the first line of its
 * message. The whole message is the tooltip; the run's own page shows it with the traceback. */
export function RunError({ run }: { run: Pick<RunItem, "error_class" | "error_message"> }) {
  if (!run.error_class) return <Blank />;
  const first = (run.error_message ?? "").split("\n")[0];
  return (
    <span className="why-text" title={run.error_message || run.error_class}>
      <b>{run.error_class}</b>
      {first ? ` ${first}` : null}
    </span>
  );
}
