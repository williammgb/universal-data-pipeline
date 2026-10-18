import fc from "fast-check";
import { describe, expect, it } from "vitest";

import {
  cell,
  connectorName,
  count,
  describeSchedule,
  duration,
  fromInput,
  moment,
  toInput,
} from "./format";

/** Every shape a preview cell can arrive in, per the API's JSON rule. */
const values = fc.oneof(
  fc.constant(null),
  fc.boolean(),
  fc.constantFrom(0, -0, 1.5, -2.25, 2 ** 53 - 1, -(2 ** 53)),
  fc.integer(),
  fc.constantFrom("", " ", "null", "NaN", "Infinity", "0", "false", "café", "x".repeat(10000)),
  fc.string({ maxLength: 40 }),
);

/** A jsonb column's value: any JSON object or array, nested, as it arrives — parsed from the
 * response's JSON, so never holding what JSON cannot say, like -0. */
const documents = fc
  .oneof(
    fc.array(fc.jsonValue({ maxDepth: 3 })),
    fc.dictionary(fc.string(), fc.jsonValue({ maxDepth: 3 })),
  )
  .map((value) => JSON.parse(JSON.stringify(value)) as { [key: string]: unknown } | unknown[]);

describe("preview cells", () => {
  it("show any value the API can send, exactly, and mark only null as null", () => {
    fc.assert(
      fc.property(values, (value) => {
        const shown = cell(value);
        expect(shown.isNull).toBe(value === null);
        if (typeof value === "string") expect(shown.text).toBe(value);
        else expect(shown.text).toBe(value === null ? "null" : String(value));
        expect(shown.full).toBe(shown.text);
      }),
    );
  });

  it("show a JSON value as the same JSON: on one line, and indented in full", () => {
    fc.assert(
      fc.property(documents, (value) => {
        const shown = cell(value);
        expect(shown.isNull).toBe(false);
        expect(shown.text).not.toContain("\n");
        expect(JSON.parse(shown.text)).toEqual(value);
        expect(JSON.parse(shown.full)).toEqual(value);
      }),
    );
    expect(cell({ a: [1, 2] }).full).toBe('{\n  "a": [\n    1,\n    2\n  ]\n}');
  });
});

describe("times and counts", () => {
  it("show a timestamp in Amsterdam time whatever offset it arrives in", () => {
    expect(moment("2026-09-16T07:14:00.250Z")).toBe("2026-09-16 09:14:00");
    expect(moment("2026-09-16T09:00:00+02:00")).toBe("2026-09-16 09:00:00");
    expect(moment("2026-01-15T07:14:00Z")).toBe("2026-01-15 08:14:00");
    expect(moment("2026-12-31T23:30:00Z")).toBe("2027-01-01 00:30:00");
    expect(moment(null)).toBe("—");
    expect(moment("not a time")).toBe("not a time");
  });

  it("read a typed Amsterdam time as a UTC moment, March's missing and October's twice hours too", () => {
    expect(fromInput("2026-09-16T09:00")).toBe("2026-09-16T07:00:00Z");
    expect(fromInput("2026-01-15T09:00")).toBe("2026-01-15T08:00:00Z");
    expect(fromInput("2026-03-29T02:30")).toBe("2026-03-29T01:30:00Z");
    expect(fromInput("2026-10-25T02:30")).toBe("2026-10-25T01:30:00Z");
    expect(fromInput("")).toBe("");
    expect(fromInput("not a time")).toBe("");
  });

  it("put a UTC moment in a time box as Amsterdam time", () => {
    expect(toInput("2026-09-16T07:00:00Z")).toBe("2026-09-16T09:00");
    expect(toInput("2026-09-16T09:00:00+02:00")).toBe("2026-09-16T09:00");
    expect(toInput("")).toBe("");
    expect(toInput("not a time")).toBe("");
  });

  it("give back the same moment after a round trip through the time box, every minute of a year", () => {
    const start = Date.UTC(2026, 0, 1);
    fc.assert(
      fc.property(fc.integer({ min: 0, max: 366 * 1440 }), (minute) => {
        const instant = new Date(start + minute * 60000).toISOString().slice(0, 16) + ":00Z";
        const shown = toInput(instant);
        // The repeated hour in October shows two moments the same way; the box gives the later.
        const back = fromInput(shown);
        expect(toInput(back)).toBe(shown);
        if (back !== instant) expect(Date.parse(back) - Date.parse(instant)).toBe(3600000);
      }),
    );
  });

  it("measure how long a run took, and say so only when it ended", () => {
    expect(duration("2026-09-16T07:14:00Z", "2026-09-16T07:14:07.100Z")).toBe("7.1s");
    expect(duration("2026-09-16T07:14:00Z", "2026-09-16T07:16:30Z")).toBe("2m 30s");
    expect(duration("2026-09-16T07:14:00Z", null)).toBe("—");
  });

  it("show a missing count as a dash", () => {
    expect(count(null)).toBe("—");
    expect(count(2000)).toBe("2,000");
  });
});

describe("sources and schedules", () => {
  it("name each connector the way a person does", () => {
    expect(["csv", "excel", "database", "rest_api", "json"].map(connectorName)).toEqual([
      "CSV",
      "Excel",
      "Database",
      "API",
      "JSON",
    ]);
    expect(connectorName("parquet")).toBe("parquet");
  });

  const SUMMER = new Date("2026-07-01T12:00:00Z");
  const WINTER = new Date("2026-01-15T12:00:00Z");

  it.each([
    ["* * * * *", "every minute"],
    ["*/15 * * * *", "every 15 minutes"],
    ["*/1 * * * *", "every minute"],
    ["5 * * * *", "every hour at :05"],
    ["30 */6 * * *", "every 6 hours at :30"],
    ["0 2 * * *", "every day at 04:00"],
    ["30 8 * * 1-5", "every weekday at 10:30"],
    ["0 9 * * 1", "every Monday at 11:00"],
    ["0 9 * * 7", "every Sunday at 11:00"],
    ["0 0 1 * *", "on day 1 of every month at 02:00"],
    ["0 23 * * *", "every day at 01:00"],
    ["0 23 * * 1", "every Tuesday at 01:00"],
    ["0 23 * * 6", "every Sunday at 01:00"],
    ["0 22 * * 1", "every Tuesday at 00:00"],
    ["30 23 * * 1-5", "every weekday at 23:30 UTC"],
    ["0 23 1 * *", "on day 1 of every month at 23:00 UTC"],
  ])("describe %s in summer as %s", (schedule, plain) => {
    expect(describeSchedule(schedule, SUMMER)).toBe(plain);
  });

  it.each([
    ["* * * * *", "every minute"],
    ["*/15 * * * *", "every 15 minutes"],
    ["5 * * * *", "every hour at :05"],
    ["30 */6 * * *", "every 6 hours at :30"],
    ["0 2 * * *", "every day at 03:00"],
    ["0 22 * * 1", "every Monday at 23:00"],
  ])("describe %s in winter as %s", (schedule, plain) => {
    expect(describeSchedule(schedule, WINTER)).toBe(plain);
  });

  it.each([
    "0 0 1 1 *",
    "0 9 1 * 1",
    "1,2 * * * *",
    "60 * * * *",
    "0 24 * * *",
    "*/0 * * * *",
    "0 9 * * 1-3",
    "* * * *",
  ])("leave %s undescribed rather than describe it wrongly", (schedule) => {
    expect(describeSchedule(schedule, SUMMER)).toBeNull();
    expect(describeSchedule(schedule, WINTER)).toBeNull();
  });
});
