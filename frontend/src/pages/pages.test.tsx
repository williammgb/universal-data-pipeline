import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { MemoryRouter } from "react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type {
  DatasetConfig,
  DatasetDetail,
  DatasetItem,
  DatasetLineage,
  DatasetProfile,
  LineageNode,
  LineageRun,
  ProfileRecord,
  QualityReport,
  RowsPage,
  RunsPage,
  SourceItem,
  StepRecord,
} from "../api/client";
import { rememberKey } from "../api/key";
import App from "../App";
import { FULL_CLASS } from "../settings";
import { TAB_NAMES, TABS } from "./Dataset";

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
    {
      name: "city",
      type: "text",
      kind: "text",
      missing: 0,
      distinct: 20,
      appear_once: 20,
      most_used: [
        { value: "city 1", count: 1 },
        { value: "city 10", count: 1 },
        { value: "city 11", count: 1 },
      ],
    },
    { name: "details", type: "jsonb", kind: "other", missing: 4 },
  ],
};

/** A dataset whose schedule is edited and whose other settings are still the file's. */
const CONFIG: DatasetConfig = {
  source: "demo_csv",
  dataset: "customers",
  connector_type: "csv",
  editable: [
    "schedule",
    "load_mode",
    "watermark",
    "primary_key",
    "columns",
    "checks",
    "quarantine_threshold_percent",
  ],
  file: {
    schedule: null,
    load_mode: "full",
    watermark: null,
    primary_key: null,
    columns: { customer_id: "integer" },
    checks: [],
    quarantine_threshold_percent: 1,
  },
  effective: {
    schedule: "0 6 * * *",
    load_mode: "full",
    watermark: null,
    primary_key: null,
    columns: { customer_id: "integer" },
    checks: [],
    quarantine_threshold_percent: 1,
  },
  overridden: ["schedule"],
  columns: [
    { name: "customer_id", type: "bigint" },
    { name: "city", type: "text" },
  ],
  checks_yaml: "",
  file_checks_yaml: "",
  history: [
    {
      changed: { schedule: { from: null, to: "0 6 * * *" } },
      changed_at: "2026-09-22T16:08:51Z",
    },
  ],
};

const SAVED: DatasetConfig = {
  ...CONFIG,
  effective: { ...CONFIG.effective, schedule: "15 */2 * * *" },
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

/** What the pages sent with a POST or a PUT, newest last, cleared before every test. */
const sent: string[] = [];

function serve(answers: Record<string, Answer>) {
  const asked: string[] = [];
  const fetcher = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = new URL(String(input), "http://console.test");
    const key = `${init?.method ?? "GET"} ${url.pathname}`;
    asked.push(`${key}${url.search}`);
    if (typeof init?.body === "string") sent.push(init.body);
    // An answer for the address with its query wins over one for the address alone.
    const answer = answers[`${key}${url.search}`] ??
      answers[key] ?? { body: { detail: `no fixture for ${key}` }, status: 404 };
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
  sent.length = 0;
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
    expect(code.textContent).toContain("99.6% match");
    expect(code.textContent).toContain("Most used");
    expect(code.textContent).toContain("Least used");
    expect(code.textContent).toContain("793 distinct values · 5 appear once");
    // The dashboard states facts; how the three were picked is not one of them.
    expect(code.textContent).not.toContain("the first 3 by value");
    const city = screen.getByRole("article", { name: "column city" });
    // Every value appears once: one list, and not called most used.
    expect(city.textContent).toContain("Values");
    expect(city.textContent).not.toContain("Most used");
    expect(city.textContent).not.toContain("Least used");
    expect(city.textContent).toContain("20 distinct values");
    expect(city.textContent).not.toContain("appear once");
    const day = screen.getByRole("article", { name: "column ordered" });
    expect(day.textContent).toContain("earliest");
    expect(day.textContent).toContain("2015-01-03 00:00");
    // A JSON column has its missing count and says its values are not profiled.
    const details = screen.getByRole("article", { name: "column details" });
    expect(details.textContent).toContain("jsonb");
    expect(details.textContent).toContain("4 missing");
    expect(details.textContent).toContain("values not profiled");
    expect(code.textContent).not.toContain("values not profiled");
  });

  it("shows long values in full once the setting says so, and keeps working without storage", async () => {
    serve({});
    show("/settings");

    (await screen.findByLabelText(/Show them in full/)).click();
    expect(screen.queryByText(/Kept in this browser/)).toBeNull();
    expect(screen.queryByText(/Cut at 22 characters/)).toBeNull();
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
    // The same line and the same two buttons above the table and below it.
    expect(screen.getAllByText("Rows 1–2 of this table (total 20 rows)")).toHaveLength(2);
    const next = screen.getAllByRole("button", { name: "Next" });
    const previous = screen.getAllByRole("button", { name: "Previous" });
    expect(next).toHaveLength(2);
    expect(previous).toHaveLength(2);
    for (const button of [...next, ...previous]) {
      expect(button.hasAttribute("disabled")).toBe(true);
    }
  });

  it("shows a JSON value on one line, and indented once values are shown in full", async () => {
    const details = { size: "L", tags: ["new", "sale"] };
    serve({
      "GET /api/datasets/demo_csv/customers": { body: DETAIL },
      "GET /api/datasets/demo_csv/customers/rows": {
        body: {
          ...ROWS,
          columns: [...ROWS.columns, { name: "details", type: "jsonb" }],
          rows: [{ customer_id: 1, city: "Delft", details }],
        },
      },
    });

    show("/datasets/demo_csv/customers?tab=preview");

    const line = await screen.findByText(JSON.stringify(details));
    expect(line.className).toBe("json-line");
    const whole = line.parentElement?.querySelector(".json-full");
    expect(whole?.textContent).toBe(JSON.stringify(details, null, 2));
    expect(line.parentElement?.getAttribute("title")).toBe(JSON.stringify(details));
  });

  it("pages with the buttons above the table", async () => {
    serve({
      "GET /api/datasets/demo_csv/customers": { body: DETAIL },
      "GET /api/datasets/demo_csv/customers/rows": { body: { ...ROWS, has_more: true } },
    });

    show("/datasets/demo_csv/customers?tab=preview");

    const [topNext] = await screen.findAllByRole("button", { name: "Next" });
    topNext?.click();
    expect(await screen.findAllByText("Rows 51–52 of this table (total 20 rows)")).toHaveLength(2);
    const [topPrevious] = screen.getAllByRole("button", { name: "Previous" });
    topPrevious?.click();
    expect(await screen.findAllByText("Rows 1–2 of this table (total 20 rows)")).toHaveLength(2);
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

  it("marks an edited setting, and saves every field the tab holds", async () => {
    const asked = serve({
      "GET /api/datasets/demo_csv/customers": { body: DETAIL },
      "GET /api/datasets/demo_csv/customers/config": { body: CONFIG },
      "PUT /api/datasets/demo_csv/customers/config": { body: SAVED },
    });

    show("/datasets/demo_csv/customers?tab=config");

    const cron = (await screen.findByLabelText(/Cron/)) as HTMLInputElement;
    expect(cron.value).toBe("0 6 * * *");
    // The schedule is edited: the file says nothing, so the tag and the way back both appear.
    expect(screen.getByText("edited")).toBeTruthy();
    expect(screen.getByRole("button", { name: /use the file/ })).toBeTruthy();
    expect(screen.getByText("every day at 06:00 UTC")).toBeTruthy();

    fireEvent.change(cron, { target: { value: "15 */2 * * *" } });
    fireEvent.click(screen.getByRole("button", { name: "Save" }));

    await waitFor(() => expect(asked).toContain("PUT /api/datasets/demo_csv/customers/config"));
    const body = JSON.parse(sent[0]!) as {
      values: Record<string, unknown>;
      accept_rebuild: boolean;
    };
    expect(body.values.schedule).toBe("15 */2 * * *");
    expect(body.values.columns).toEqual({ customer_id: "integer" });
    expect(body.accept_rebuild).toBe(false);
  });

  it("refuses to save behind your back when the table would have to be rebuilt", async () => {
    serve({
      "GET /api/datasets/demo_csv/customers": { body: DETAIL },
      "GET /api/datasets/demo_csv/customers/config": { body: CONFIG },
      "PUT /api/datasets/demo_csv/customers/config": {
        body: { detail: "dataset 'customers' changed load_mode from 'full' to 'append'" },
        status: 409,
      },
    });

    show("/datasets/demo_csv/customers?tab=config");

    const mode = (await screen.findByLabelText("Load mode")) as HTMLSelectElement;
    fireEvent.change(mode, { target: { value: "append" } });
    fireEvent.click(screen.getByRole("button", { name: "Save" }));

    expect(await screen.findByText(/needs the table rebuilt/)).toBeTruthy();
    expect(screen.getByText(/changed load_mode from 'full' to 'append'/)).toBeTruthy();
    expect(screen.getByRole("button", { name: "Save anyway" })).toBeTruthy();
  });

  it("shows what the API said when it cannot answer", async () => {
    serve({
      "GET /api/datasets": { body: { detail: "the database is unavailable" }, status: 503 },
      "GET /api/sources": { body: SOURCES },
    });

    show("/");

    expect(await screen.findByText("the database is unavailable")).toBeTruthy();
  });

  it("explains every tab, every loading mode, quarantine, the key and schedules in the guide", () => {
    const asked = serve({});

    show("/guide");

    const guide = screen.getByRole("main").textContent ?? "";
    for (const tab of TABS) expect(guide).toContain(TAB_NAMES[tab]);
    const named = screen.getAllByRole("term").map((node) => node.textContent);
    for (const mode of ["full", "append", "merge"]) expect(named).toContain(mode);
    expect(guide).toContain("quarantine");
    expect(guide).toContain("API key");
    expect(guide).toContain("schedule");
    expect(guide).toContain("cron fields in UTC");
    // A page that reads nothing cannot fail on a machine with no API to read.
    expect(asked).toEqual([]);
  });

  it("shows the guide with no key stored, and when the API has refused one", async () => {
    expect(window.localStorage.getItem("udp.apiKey")).toBe(null);
    serve({});

    const first = show("/guide");

    expect(screen.getByRole("heading", { level: 1, name: "Guide" })).toBeTruthy();
    expect(screen.queryByLabelText("API key")).toBeNull();
    first.unmount();

    // The key prompt replaces every other page once the API refuses a request. The guide is the
    // one page it must not replace, because it is what explains the key.
    window.localStorage.setItem("udp.apiKey", "wrong");
    serve({
      "GET /api/datasets": { body: { detail: "an API key is required" }, status: 401 },
      "GET /api/sources": { body: SOURCES },
    });
    const refused = show("/");
    expect(await screen.findByLabelText("API key")).toBeTruthy();
    refused.unmount();

    serve({});
    show("/guide");

    expect(screen.getByRole("heading", { level: 1, name: "Guide" })).toBeTruthy();
    expect(screen.queryByLabelText("API key")).toBeNull();
    rememberKey("");
  });

  it("puts a Guide link in the bar that opens the guide and marks itself the current page", async () => {
    serve({ "GET /api/datasets": { body: DATASETS }, "GET /api/sources": { body: SOURCES } });

    show("/");
    const link = await screen.findByRole("link", { name: "Guide" });
    expect(link.className).not.toContain("current");

    fireEvent.click(link);

    expect(screen.getByRole("heading", { level: 1, name: "Guide" })).toBeTruthy();
    expect(screen.getByRole("link", { name: "Guide" }).className).toContain("current");
  });

  it("points every link in the guide's contents at a section that is on the page", () => {
    serve({});

    show("/guide");

    const headings = screen.getAllByRole("heading", { level: 2 }).map((node) => node.id);
    const contents = screen
      .getAllByRole("link")
      .map((node) => node.getAttribute("href") ?? "")
      .filter((href) => href.startsWith("#"))
      .map((href) => href.slice(1));
    expect(contents.length).toBe(headings.length);
    expect(contents.length).toBeGreaterThan(5);
    for (const target of contents) expect(headings).toContain(target);
  });
});

const LINEAGE_ADDRESS = "GET /api/datasets/demo_csv/customers/lineage";

const MADE_CLEAN: LineageRun = {
  execution_id: "01a1b2c3-0000-7000-8000-000000000001",
  pipeline: "customers_tidy",
  version: 4,
  status: "succeeded",
  started_at: "2026-10-04T14:02:00Z",
};

const BROKE: LineageRun = {
  execution_id: "01a1d4e5-0000-7000-8000-000000000002",
  pipeline: "customers_tidy",
  version: 3,
  status: "failed",
  started_at: "2026-10-03T09:15:00Z",
};

function profiled(
  stage: ProfileRecord["stage"],
  after_step: number | null,
  table_rows: number,
): ProfileRecord {
  return {
    profile_id: table_rows + (after_step ?? 0),
    stage,
    after_step,
    table_rows,
    missing_values: 2,
    invalid_values: 0,
    outliers: 1,
    duplicates: 0,
    profiled_at: "2026-10-04T14:02:01Z",
  };
}

const SOURCE_NODE: LineageNode = { kind: "source", name: "csv: data/customers.csv" };
const RAW_NODE: LineageNode = {
  kind: "raw",
  name: "raw.demo_csv__customers",
  profile: profiled("raw", null, 20),
};
const TRIMMED: StepRecord = {
  position: 1,
  type: "normalize_values",
  configuration: { columns: ["city"], trim: true },
  status: "succeeded",
  started_at: "2026-10-04T14:02:01Z",
  ended_at: "2026-10-04T14:02:02Z",
  rows_in: 20,
  rows_out: 20,
  values_changed: 3,
};

const PREPARED: DatasetLineage = {
  source: "demo_csv",
  dataset: "customers",
  run: MADE_CLEAN,
  runs: [MADE_CLEAN, BROKE],
  chain: [
    SOURCE_NODE,
    RAW_NODE,
    { kind: "step", name: "normalize_values", step: TRIMMED },
    {
      kind: "step",
      name: "fill_missing",
      step: {
        position: 2,
        type: "fill_missing",
        configuration: { columns: ["lifetime_value"], method: "median" },
        status: "succeeded",
        started_at: "2026-10-04T14:02:02Z",
        ended_at: "2026-10-04T14:02:03Z",
        rows_in: 20,
        rows_out: 20,
        values_changed: 1,
      },
      profile: profiled("staging", 2, 20),
    },
    { kind: "clean", name: "clean.demo_csv__customers", profile: profiled("clean", null, 20) },
  ],
};

const FAILED_LINEAGE: DatasetLineage = {
  ...PREPARED,
  run: { ...BROKE, error: "step 2 (python) with {script: fix.py}: ValueError: cannot read row 3" },
  chain: [
    SOURCE_NODE,
    RAW_NODE,
    { kind: "step", name: "normalize_values", step: TRIMMED },
    {
      kind: "step",
      name: "python",
      step: {
        position: 2,
        type: "python",
        configuration: { script: "scripts/fix.py" },
        status: "failed",
        started_at: "2026-10-03T09:15:01Z",
        ended_at: "2026-10-03T09:15:02Z",
        rows_in: 20,
        error: "ValueError: cannot read row 3",
        error_line: 2,
      },
    },
    {
      kind: "step",
      name: "fill_missing",
      step: {
        position: 3,
        type: "fill_missing",
        configuration: { columns: ["lifetime_value"], method: "median" },
        status: "not_run",
      },
    },
  ],
};

const PLAIN_LINEAGE: DatasetLineage = {
  source: "demo_csv",
  dataset: "customers",
  run: null,
  runs: [],
  chain: [SOURCE_NODE, RAW_NODE, { kind: "table", name: "datasets.demo_csv__customers" }],
};

function showLineage(answers: Record<string, Answer>) {
  const asked = serve({ "GET /api/datasets/demo_csv/customers": { body: DETAIL }, ...answers });
  show("/datasets/demo_csv/customers?tab=lineage");
  return asked;
}

/** The blocks of the chain, top to bottom, as the text each one shows. */
async function chainShown(): Promise<string[]> {
  const chain = await screen.findByRole("list", { name: "Lineage" });
  return within(chain)
    .getAllByRole("listitem")
    .map((item) => item.textContent ?? "");
}

function factIn(panel: HTMLElement, label: string): string {
  return within(panel).getByText(label).closest(".fact")?.querySelector("dd")?.textContent ?? "";
}

describe("the lineage tab", () => {
  it("draws a run's chain in order, from the source file to CLEAN", async () => {
    showLineage({ [LINEAGE_ADDRESS]: { body: PREPARED } });

    const blocks = await chainShown();

    expect(blocks).toHaveLength(5);
    const names = [
      "csv: data/customers.csv",
      "raw.demo_csv__customers",
      "normalize_values",
      "fill_missing",
      "clean.demo_csv__customers",
    ];
    names.forEach((name, index) => expect(blocks[index]).toContain(name));
    expect(blocks[0]).toContain("Source");
    expect(blocks[2]).toContain("20 rows · 3 changed");
    expect(screen.getByRole("link", { name: "Lineage" }).className).toContain("current");
  });

  it("shows a step's transformation, column, method, input rows, values changed and status", async () => {
    showLineage({ [LINEAGE_ADDRESS]: { body: PREPARED } });
    await chainShown();

    fireEvent.click(screen.getByRole("button", { name: /Step 2/ }));

    const panel = screen.getByRole("complementary", { name: "Step 2" });
    expect(factIn(panel, "Transformation")).toBe("fill_missing");
    expect(factIn(panel, "Column")).toBe("lifetime_value");
    expect(factIn(panel, "Method")).toBe("median");
    expect(factIn(panel, "Input rows")).toBe("20");
    expect(factIn(panel, "Values changed")).toBe("1");
    expect(factIn(panel, "Status")).toBe("succeeded");
    expect(within(panel).getByText("Profile after step 2")).toBeTruthy();
    expect(screen.getByRole("button", { name: /Step 2/ }).getAttribute("aria-pressed")).toBe("true");
    expect(screen.getByRole("button", { name: /Step 1/ }).getAttribute("aria-pressed")).toBe("false");
  });

  it("marks a failed step apart from the ones that ran, and shows its error", async () => {
    showLineage({ [LINEAGE_ADDRESS]: { body: FAILED_LINEAGE } });
    await chainShown();

    const failed = screen.getByRole("button", { name: /Step 2/ });
    expect(failed.className).toContain("failed");
    expect(screen.getByRole("button", { name: /Step 1/ }).className).not.toContain("failed");
    expect(screen.getByRole("button", { name: /Step 3/ }).className).toContain("not_run");
    expect(within(failed).getByText("ValueError: cannot read row 3")).toBeTruthy();
    // The failed step is the one shown first, with its error and the line it failed on.
    const panel = screen.getByRole("complementary", { name: "Step 2" });
    expect(factIn(panel, "Status")).toBe("failed");
    expect(factIn(panel, "Failing line")).toBe("2");
    expect(within(panel).getByText("ValueError: cannot read row 3")).toBeTruthy();
    expect(screen.queryByText(/clean\.demo_csv__customers/)).toBeNull();
    // A step's failure is shown on the step, not again above the chain.
    expect(screen.queryByText(/step 2 \(python\) with/)).toBeNull();
  });

  it("shows a run that failed outside every step once, above its chain", async () => {
    const stopped: DatasetLineage = {
      ...PREPARED,
      run: { ...BROKE, error: "critical constraint not_null on customer_id failed for 2 rows" },
      chain: PREPARED.chain.slice(0, 4),
    };
    showLineage({ [LINEAGE_ADDRESS]: { body: stopped } });
    await chainShown();

    expect(screen.getByText(/critical constraint not_null/).className).toBe("lineage-error");
    expect(screen.queryByText(/clean\.demo_csv__customers/)).toBeNull();
  });

  it("shows a dataset no pipeline has run over as its plain lineage", async () => {
    showLineage({ [LINEAGE_ADDRESS]: { body: PLAIN_LINEAGE } });

    const blocks = await chainShown();

    expect(blocks).toHaveLength(3);
    expect(blocks[0]).toContain("csv: data/customers.csv");
    expect(blocks[1]).toContain("raw.demo_csv__customers");
    expect(blocks[2]).toContain("datasets.demo_csv__customers");
    expect(screen.queryByLabelText("Run")).toBeNull();
    expect(screen.queryByRole("complementary")).toBeNull();
  });

  it("shows nothing but a short note for a dataset with no lineage yet", async () => {
    showLineage({ [LINEAGE_ADDRESS]: { body: { ...PLAIN_LINEAGE, chain: [] } } });

    expect(await screen.findByText("No lineage yet.")).toBeTruthy();
    expect(screen.queryByRole("list", { name: "Lineage" })).toBeNull();
  });

  it("shows the chosen run's chain when another run is picked", async () => {
    const asked = showLineage({
      [LINEAGE_ADDRESS]: { body: PREPARED },
      [`${LINEAGE_ADDRESS}?run=${BROKE.execution_id}`]: { body: FAILED_LINEAGE },
    });
    expect((await chainShown())[4]).toContain("clean.demo_csv__customers");

    fireEvent.change(screen.getByLabelText("Run"), { target: { value: BROKE.execution_id } });

    await waitFor(() => expect(screen.queryByText(/clean\.demo_csv__customers/)).toBeNull());
    const blocks = await chainShown();
    expect(blocks[3]).toContain("python");
    expect(blocks[4]).toContain("not run");
    expect(asked).toContain(`${LINEAGE_ADDRESS}?run=${BROKE.execution_id}`);
    expect((screen.getByLabelText("Run") as HTMLSelectElement).value).toBe(BROKE.execution_id);
  });

  it("names the run that made CLEAN and links to that run's page", async () => {
    showLineage({ [LINEAGE_ADDRESS]: { body: PREPARED } });

    const chain = await screen.findByRole("list", { name: "Lineage" });
    const clean = chain.lastElementChild as HTMLElement;

    expect(clean.textContent).toContain("clean.demo_csv__customers");
    expect(clean.textContent).toContain("customers_tidy v4");
    const link = within(clean).getByRole("link", { name: "01a1b2c3" });
    expect(link.getAttribute("href")).toBe(
      `/pipeline?source=demo_csv&dataset=customers&name=customers_tidy&run=${MADE_CLEAN.execution_id}`,
    );
  });
});
