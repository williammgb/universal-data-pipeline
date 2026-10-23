/** The runs list's filters, kept in the address so a filtered view can be shared or reloaded. */

import type { RunKind, RunStatus, RunTrigger } from "./api/client";

export const RUN_STATUSES: RunStatus[] = ["running", "succeeded", "failed", "skipped"];
export const RUN_TRIGGERS: RunTrigger[] = ["manual", "scheduled"];
export const RUN_KINDS: RunKind[] = ["load", "pipeline"];

export type RunFilters = {
  source?: string;
  dataset?: string;
  kind?: RunKind;
  status?: RunStatus;
  trigger?: RunTrigger;
  since?: string;
  until?: string;
  offset?: number;
};

const TEXT_FIELDS = ["source", "dataset", "since", "until"] as const;

/** Filters as URL parameters. A filter with no value is left out: the API matches exactly,
 * so an empty `source` would ask for runs of a source named "". */
export function serialize(filters: RunFilters): URLSearchParams {
  const params = new URLSearchParams();
  for (const field of TEXT_FIELDS) {
    const value = filters[field];
    if (value !== undefined && value !== "") params.set(field, value);
  }
  if (filters.kind !== undefined) params.set("kind", filters.kind);
  if (filters.status !== undefined) params.set("status", filters.status);
  if (filters.trigger !== undefined) params.set("trigger", filters.trigger);
  if (filters.offset !== undefined && filters.offset > 0) params.set("offset", String(filters.offset));
  return params;
}

/** The API only accepts a time with a zone, so text that is not one is not a filter: sending
 * it would refuse the whole request and leave the runs list showing an error instead. */
function isMoment(value: string): boolean {
  return /[+-]\d\d:?\d\d$|Z$/.test(value) && !Number.isNaN(Date.parse(value));
}

export function parse(params: URLSearchParams): RunFilters {
  const filters: RunFilters = {};
  for (const field of TEXT_FIELDS) {
    const value = params.get(field);
    if (value === null || value === "") continue;
    if ((field === "since" || field === "until") && !isMoment(value)) continue;
    filters[field] = value;
  }
  const kind = params.get("kind");
  if (kind !== null && (RUN_KINDS as string[]).includes(kind)) filters.kind = kind as RunKind;
  const status = params.get("status");
  if (status !== null && (RUN_STATUSES as string[]).includes(status)) {
    filters.status = status as RunStatus;
  }
  const trigger = params.get("trigger");
  if (trigger !== null && (RUN_TRIGGERS as string[]).includes(trigger)) {
    filters.trigger = trigger as RunTrigger;
  }
  const offset = Number(params.get("offset"));
  if (Number.isSafeInteger(offset) && offset > 0) filters.offset = offset;
  return filters;
}

export const RUNS_PER_PAGE = 50;

/** The same filters as the API query, which pages by offset. With `everyKind`, no kind filter
 * asks for loads and pipeline runs both; without it the API lists loads only. */
export function runsQuery(filters: RunFilters, everyKind = false): URLSearchParams {
  const params = serialize(filters);
  if (everyKind && filters.kind === undefined) params.set("kind", "all");
  params.set("limit", String(RUNS_PER_PAGE));
  return params;
}
