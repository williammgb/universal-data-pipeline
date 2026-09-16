import fc from "fast-check";
import { describe, expect, it } from "vitest";

import { cell, count, duration, moment } from "./format";

/** Every shape a preview cell can arrive in, per the API's JSON rule. */
const values = fc.oneof(
  fc.constant(null),
  fc.boolean(),
  fc.constantFrom(0, -0, 1.5, -2.25, 2 ** 53 - 1, -(2 ** 53)),
  fc.integer(),
  fc.constantFrom("", " ", "null", "NaN", "Infinity", "0", "false", "café", "x".repeat(10000)),
  fc.string({ maxLength: 40 }),
);

describe("preview cells", () => {
  it("show any value the API can send, exactly, and mark only null as null", () => {
    fc.assert(
      fc.property(values, (value) => {
        const shown = cell(value);
        expect(shown.isNull).toBe(value === null);
        if (typeof value === "string") expect(shown.text).toBe(value);
        else expect(shown.text).toBe(value === null ? "null" : String(value));
      }),
    );
  });
});

describe("times and counts", () => {
  it("show a timestamp in UTC whatever offset it arrives in", () => {
    expect(moment("2026-09-16T07:14:00.250Z")).toBe("2026-09-16 07:14:00");
    expect(moment("2026-09-16T09:00:00+02:00")).toBe("2026-09-16 07:00:00");
    expect(moment(null)).toBe("—");
    expect(moment("not a time")).toBe("not a time");
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
