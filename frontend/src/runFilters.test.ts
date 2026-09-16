import fc from "fast-check";
import { describe, expect, it } from "vitest";

import { RUN_STATUSES, RUN_TRIGGERS, parse, serialize, type RunFilters } from "./runFilters";

/** Text a filter could carry, including the characters that break a careless query string. */
const text = fc.oneof(
  fc.constantFrom("", " ", "  ", "&", "=", "+", "%", "#", "?", "a&b=c", "café", "日本", "demo_csv"),
  fc.string({ maxLength: 12 }),
);

const moment = fc.constantFrom(
  "2026-09-16T00:00:00Z",
  "2026-09-16T07:14:00.250Z",
  "2026-09-16T09:00:00+02:00",
  "2026-01-01T00:00:00-05:30",
);

const filters: fc.Arbitrary<RunFilters> = fc.record(
  {
    source: text,
    dataset: text,
    status: fc.constantFrom(...RUN_STATUSES),
    trigger: fc.constantFrom(...RUN_TRIGGERS),
    since: moment,
    until: moment,
    offset: fc.oneof(fc.constant(0), fc.integer({ min: 0, max: 100000 })),
  },
  { requiredKeys: [] },
);

/** What serialize is expected to keep: a filter with no value is not a filter. */
function kept(given: RunFilters): RunFilters {
  const result: RunFilters = {};
  if (given.source) result.source = given.source;
  if (given.dataset) result.dataset = given.dataset;
  if (given.status !== undefined) result.status = given.status;
  if (given.trigger !== undefined) result.trigger = given.trigger;
  if (given.since) result.since = given.since;
  if (given.until) result.until = given.until;
  if (given.offset !== undefined && given.offset > 0) result.offset = given.offset;
  return result;
}

describe("runs filters in the address", () => {
  it("read back exactly as they were written", () => {
    fc.assert(
      fc.property(filters, (given) => {
        expect(parse(serialize(given))).toEqual(kept(given));
      }),
    );
  });

  it("never write a parameter with an empty value", () => {
    fc.assert(
      fc.property(filters, (given) => {
        for (const [, value] of serialize(given)) expect(value).not.toBe("");
      }),
    );
  });

  it("ignore a status or trigger the API does not accept", () => {
    const params = new URLSearchParams({ status: "done", trigger: "cron", offset: "-3" });
    expect(parse(params)).toEqual({});
  });

  it("ignore a time the API would refuse, and keep one it accepts", () => {
    for (const since of ["notadate", "2026-09-16", "2026-09-16T07:14:00", ""]) {
      expect(parse(new URLSearchParams({ since }))).toEqual({});
    }
    expect(parse(new URLSearchParams({ since: "2026-09-16T07:14:00Z" }))).toEqual({
      since: "2026-09-16T07:14:00Z",
    });
    expect(parse(new URLSearchParams({ until: "2026-09-16T09:00:00+02:00" }))).toEqual({
      until: "2026-09-16T09:00:00+02:00",
    });
  });
});
