import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type {
  DatasetDetail,
  DatasetItem,
  DatasetProfile,
  QualityReport,
  RowsPage,
  RunsPage,
  SourceItem,
} from "../api/client";
import { rememberKey } from "../api/key";
import App from "../App";
import { FULL_CLASS } from "../settings";

const SOURCES: SourceItem[] = [
  { source: "demo_csv", connector_type: "csv", datasets: 1, recorded_at: "2026-09-16T07:14:00Z" },
  { source: "demo_db", connector_type: "database", datasets: 1, recorded_at: "2026-09-16T07:02:31Z" },
];

const DATASETS: DatasetItem[] = [
  {
    source: "demo_csv",
    dataset: "customers",
    connector_type: "csv",
    table_name: "demo_csv__customers",
    load_mode: "full",
    schedule: "* * * * *",
    recorded_at: "2026-09-16T07:14:00Z",
    // The file had not changed, so the last run loaded nothing; the table still has 20 rows.
    table_rows: 20,
    last_run: {
      run_id: "01a0a3de-9cb0-73ec-be37-1caa01588b64",
      status: "succeeded",
      trigger: "scheduled",
      started_at: "2026-09-16T07:14:00Z",
      ended_at: "2026-09-16T07:14:02Z",
      rows_loaded: 0,
    },
  },
  {
    source: "demo_db",
    dataset: "orders",
    connector_type: "database",
    table_name: "demo_db__orders",
    load_mode: "merge",
    schedule: null,
    recorded_at: "2026-09-16T07:02:31Z",
    table_rows: 1000,
    last_run: {
      run_id: "01a0a3c1-77be-7d41-9f2e-6b0a6d4b18cc",
      status: "failed",
      trigger: "manual",
      started_at: "2026-09-16T07:02:31Z",
      ended_at: "2026-09-16T07:02:38Z",
      rows_loaded: null,
    },
  },
];

const DETAIL: DatasetDetail = {
  ...DATASETS[0]!,
  primary_key: [],
  watermark: null,
  definition: { name: "customers", columns: { customer_id: "integer" } },
  columns: [
    { name: "customer_id", type: "bigint" },
    { name: "city", type: "text" },
  ],
  versions: [
    {
      version: 1,
      run_id: "01a0a3de-9cb0-73ec-be37-1caa01588b64",
      recorded_at: "2026-09-14T19:22:08Z",
      columns: [
        { name: "customer_id", type: "bigint" },
        { name: "city", type: "text" },
      ],
    },
  ],
  state: {
    watermark_column: null,
    watermark_type: null,
    watermark: null,
    file_path: "data/customers.csv",
    file_sha256: "3f9a1c77e21bc4",
    run_id: "01a0a3de-9cb0-73ec-be37-1caa01588b64",
    saved_at: "2026-09-16T07:14:02Z",
  },
};

const ROWS: RowsPage = {
  columns: [
    { name: "customer_id", type: "bigint" },
    { name: "city", type: "text" },
  ],
  rows: [
    { customer_id: 1, city: "Delft" },
    { customer_id: 2, city: null },
  ],
  limit: 50,
  offset: 0,
  has_more: false,
};

const QUALITY: QualityReport = {
  run_id: "01a0a3de-9cb0-73ec-be37-1caa01588b64",
  results: [
    {
      position: 0,
      check_type: "regex",
      columns: ["city"],
      severity: "warn",
      passed: false,
      failing_rows: 1,
      table_rows: null,
      message: "1 rows failed",
      settings: { pattern: "[A-Z][a-z]+" },
      checked_at: "2026-09-16T07:14:02Z",
    },
  ],
};

const PROFILE: DatasetProfile = {
  table_rows: 3_000_000,
  profiled_rows: 1_000_000,
  sampled: true,
  columns: [
    {
      name: "amount",
      type: "numeric(12,2)",
      kind: "number",
      missing: 11,
      min: "0.44",
      max: "22638.48",
      mean: "229.8580",
      histogram: [9618, 258, 68, 23, 12, 1, 1, 5, 3, 2, 0, 0, 1, 0, 0, 1, 0, 0, 0, 1],
    },
    {
      name: "ordered",
      type: "timestamp with time zone",
      kind: "date",
      missing: 0,
      min: "2015-01-03T00:00:00+00:00",
      max: "2018-12-30T00:00:00+00:00",
      histogram: Array.from({ length: 20 }, () => 5),
    },
    {
      name: "colour",
      type: "text",
      kind: "text",
      missing: 0,
      distinct: 3,
      appear_once: 0,
      all_values: [
        { value: "red", count: 10 },
        { value: "blue", count: 5 },
        { value: "green", count: 5 },
      ],
    },
    {
      name: "code",
      type: "text",
      kind: "text",
      missing: 0,
      distinct: 793,
      appear_once: 5,
      most_used: [
        { value: "WB-21850", count: 37 },
        { value: "JL-15835", count: 34 },
        { value: "MA-17560", count: 34 },
      ],
      least_used: [
        { value: "AO-10810", count: 1 },
        { value: "CJ-11875", count: 1 },
        { value: "JR-15700", count: 1 },
      ],
      pattern: "^[A-Z]{2}\\-[0-9]{5}$",
      pattern_share: 0.9957,
    },
  ],
};

const RUNS: RunsPage = {
  runs: [
    {
      run_id: "01a0a3c1-77be-7d41-9f2e-6b0a6d4b18cc",
      source: "demo_db",
      dataset: "orders",
      trigger: "manual",
      status: "failed",
      started_at: "2026-09-16T07:02:31Z",
      ended_at: "2026-09-16T07:02:38Z",
      rows_extracted: 0,
      rows_loaded: null,
      rows_quarantined: 0,
      error_class: "ExtractError",
      error_message: "could not connect to the source database",
    },
  ],
  limit: 50,
  offset: 0,
  has_more: false,
};

type Answer = { body: unknown; status?: number };

function serve(answers: Record<string, Answer>) {
  const asked: string[] = [];
  const fetcher = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = new URL(String(input), "http://console.test");
    const key = `${init?.method ?? "GET"} ${url.pathname}`;
    asked.push(`${key}${url.search}`);
    const answer = answers[key] ?? { body: { detail: `no fixture for ${key}` }, status: 404 };
    return new Response(JSON.stringify(answer.body), {
      status: answer.status ?? 200,
      headers: { "Content-Type": "application/json" },
    });
  });
  vi.stubGlobal("fetch", fetcher);
  return asked;
}

function show(path: string) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={client}>
      <MemoryRouter initialEntries={[path]}>
        <App />
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

beforeEach(() => {
  vi.unstubAllGlobals();
});

describe("the dashboard", () => {
  it("lists every dataset with its last run", async () => {
    serve({ "GET /api/datasets": { body: DATASETS }, "GET /api/sources": { body: SOURCES } });

    show("/");

    expect(await screen.findByRole("link", { name: "customers" })).toBeTruthy();
    expect(screen.getByRole("link", { name: "orders" })).toBeTruthy();
    expect(screen.getByText("* * * * *")).toBeTruthy();
    expect(screen.getByText("succeeded")).toBeTruthy();
    expect(screen.getByText("2026-09-16 07:14:00")).toBeTruthy();
    // Rows are the table's own count, not what the last run added.
    expect(screen.getByText("20")).toBeTruthy();
    expect(screen.getByText("1,000")).toBeTruthy();
    expect(screen.queryByText("0")).toBeNull();
    expect(screen.getByText("CSV")).toBeTruthy();
    expect(screen.getByText("Database")).toBeTruthy();
    expect(screen.getByText("every minute")).toBeTruthy();
    expect(screen.queryByText(/appears here after its first run/)).toBeNull();
  });

  it("asks the API for the typed search text", async () => {
    const asked = serve({
      "GET /api/datasets": { body: DATASETS },
      "GET /api/sources": { body: SOURCES },
    });

    show("/?q=cust");

    await screen.findByRole("link", { name: "customers" });
    expect(asked).toContain("GET /api/datasets?q=cust");
  });

  it("shows a dataset's stored columns and what source.yaml declared", async () => {
    serve({ "GET /api/datasets/demo_csv/customers": { body: DETAIL } });

    show("/datasets/demo_csv/customers");

    expect(await screen.findByText("datasets.demo_csv__customers")).toBeTruthy();
    expect(await screen.findByText("integer")).toBeTruthy();
    expect(screen.getByText("inferred")).toBeTruthy();
    expect(screen.getByText("bigint")).toBeTruthy();
    // One line of facts: the file's path without its sha256, the type and the row count.
    expect(screen.getByText("data/customers.csv")).toBeTruthy();
    expect(screen.queryByText(/sha256/)).toBeNull();
    expect(screen.getByText("CSV")).toBeTruthy();
    expect(screen.getByText("20")).toBeTruthy();
    expect(screen.getByText(/\(every minute\)/)).toBeTruthy();
  });

  it("profiles each kind of column and says when a sample was used", async () => {
    serve({
      "GET /api/datasets/demo_csv/customers": { body: DETAIL },
      "GET /api/datasets/demo_csv/customers/profile": { body: PROFILE },
    });

    show("/datasets/demo_csv/customers?tab=profile");

    expect(await screen.findByText("profiled on a random 1,000,000 of 3,000,000 rows")).toBeTruthy();
    const amount = screen.getByRole("article", { name: "column amount" });
    expect(amount.textContent).toContain("11 missing (0.00%)");
    expect(amount.textContent).toContain("22638.48");
    expect(amount.querySelectorAll("rect")).toHaveLength(20);
    const colour = screen.getByRole("article", { name: "column colour" });
    expect(colour.textContent).toContain("Every value");
    expect(colour.textContent).not.toContain("Least used");
    const code = screen.getByRole("article", { name: "column code" });
    expect(code.textContent).toContain("^[A-Z]{2}\\-[0-9]{5}$");
    expect(code.textContent).toContain("99.6% of values match this pattern");
    expect(code.textContent).toContain("Most used");
    expect(code.textContent).toContain("5 appear once; the first 3 by value are shown");
    const day = screen.getByRole("article", { name: "column ordered" });
    expect(day.textContent).toContain("earliest");
    expect(day.textContent).toContain("2015-01-03 00:00");
  });

  it("shows long values in full once the setting says so, and keeps working without storage", async () => {
    serve({});
    show("/settings");

    (await screen.findByLabelText(/Show them in full/)).click();
    await waitFor(() => expect(document.documentElement.classList.contains(FULL_CLASS)).toBe(true));
    expect(window.localStorage.getItem("udp.longValues")).toBe("full");

    vi.spyOn(Storage.prototype, "setItem").mockImplementation(() => {
      throw new Error("storage is switched off");
    });
    screen.getByLabelText(/Shorten them/).click();
    await waitFor(() => expect(document.documentElement.classList.contains(FULL_CLASS)).toBe(false));
    vi.restoreAllMocks();
    window.localStorage.removeItem("udp.longValues");
  });

  it("previews rows, marks a null cell and stops Next on the last page", async () => {
    serve({
      "GET /api/datasets/demo_csv/customers": { body: DETAIL },
      "GET /api/datasets/demo_csv/customers/rows": { body: ROWS },
    });

    show("/datasets/demo_csv/customers?tab=preview");

    expect(await screen.findByText("Delft")).toBeTruthy();
    expect(screen.getByText("null")).toBeTruthy();
    expect(screen.getByText("Rows 1–2 of this table")).toBeTruthy();
    expect(screen.getByRole("button", { name: "Next" }).hasAttribute("disabled")).toBe(true);
  });

  it("shows a warn-level check that failed", async () => {
    serve({
      "GET /api/datasets/demo_csv/customers": { body: DETAIL },
      "GET /api/datasets/demo_csv/customers/quality": { body: QUALITY },
    });

    show("/datasets/demo_csv/customers?tab=quality");

    expect(await screen.findByText("1 rows failed")).toBeTruthy();
    expect(screen.getByText("failed")).toBeTruthy();
    expect(screen.getByText("regex")).toBeTruthy();
  });

  it("starts a run when Run now is pressed", async () => {
    const asked = serve({
      "GET /api/datasets/demo_csv/customers": { body: DETAIL },
      "POST /api/runs": {
        body: { source: "demo_csv", datasets: ["customers"], requested_at: "2026-09-16T08:00:00Z" },
        status: 202,
      },
    });

    show("/datasets/demo_csv/customers");
    (await screen.findByRole("button", { name: "Run now" })).click();

    await waitFor(() => expect(asked).toContain("POST /api/runs"));
    expect(await screen.findByText(/Run requested at 2026-09-16 08:00:00/)).toBeTruthy();
  });

  it("shows a failed run's error class and keeps the status filter in the address", async () => {
    const asked = serve({ "GET /api/runs": { body: RUNS }, "GET /api/sources": { body: SOURCES } });

    show("/runs?status=failed");

    expect(await screen.findByText("ExtractError")).toBeTruthy();
    expect(asked.some((call) => call.includes("status=failed"))).toBe(true);
    const status = screen.getByLabelText("Status") as HTMLSelectElement;
    expect(status.value).toBe("failed");
  });

  it("shows a run's error, its counts and its checks", async () => {
    serve({
      "GET /api/runs/01a0a3c1-77be-7d41-9f2e-6b0a6d4b18cc": {
        body: {
          ...RUNS.runs[0]!,
          error_traceback: "Traceback (most recent call last):\n  ...\nExtractError",
          quality: QUALITY.results,
        },
      },
    });

    show("/runs/01a0a3c1-77be-7d41-9f2e-6b0a6d4b18cc");

    expect(await screen.findByText(/could not connect to the source database/)).toBeTruthy();
    expect(screen.getByText("ExtractError")).toBeTruthy();
    expect(screen.getByText("2026-09-16 07:02:38 · 7.0s")).toBeTruthy();
    expect(screen.getByText("—")).toBeTruthy();
    expect(screen.getByText(/Traceback \(most recent call last\)/)).toBeTruthy();
    expect(screen.getByText("1 rows failed")).toBeTruthy();
  });

  // The note that used to explain an empty list was removed at the user's request.
  it("shows no checks and no note for a run that has none", async () => {
    serve({
      "GET /api/runs/01a0a3c1-77be-7d41-9f2e-6b0a6d4b18cc": {
        body: { ...RUNS.runs[0]!, error_traceback: null, quality: [] },
      },
    });

    show("/runs/01a0a3c1-77be-7d41-9f2e-6b0a6d4b18cc");

    expect(await screen.findByText("ExtractError")).toBeTruthy();
    expect(screen.queryByText(/Quality results appear here/)).toBeNull();
  });

  it("puts a refused request into one readable line", async () => {
    serve({
      "GET /api/runs/abc": {
        status: 422,
        body: {
          detail: [
            {
              loc: ["path", "run_id"],
              msg: "Input should be a valid UUID, invalid length",
              type: "uuid_parsing",
            },
          ],
        },
      },
    });

    show("/runs/abc");

    expect(
      await screen.findByText("run_id: Input should be a valid UUID, invalid length"),
    ).toBeTruthy();
  });

  it("drops a time filter the API would refuse instead of failing the whole list", async () => {
    const asked = serve({ "GET /api/runs": { body: RUNS }, "GET /api/sources": { body: SOURCES } });

    show("/runs?since=notadate&status=failed");

    expect(await screen.findByText("ExtractError")).toBeTruthy();
    expect(asked.some((call) => call.includes("since="))).toBe(false);
    expect(asked.some((call) => call.includes("status=failed"))).toBe(true);
  });

  it("asks for an API key when the API refuses the request", async () => {
    window.localStorage.setItem("udp.apiKey", "wrong");
    serve({
      "GET /api/datasets": { body: { detail: "an API key is required" }, status: 401 },
      "GET /api/sources": { body: SOURCES },
    });

    show("/");

    expect(await screen.findByLabelText("API key")).toBeTruthy();
    expect(window.localStorage.getItem("udp.apiKey")).toBe(null);
    rememberKey("");
  });

  it("shows what the API said when it cannot answer", async () => {
    serve({
      "GET /api/datasets": { body: { detail: "the database is unavailable" }, status: 503 },
      "GET /api/sources": { body: SOURCES },
    });

    show("/");

    expect(await screen.findByText("the database is unavailable")).toBeTruthy();
  });
});
