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
