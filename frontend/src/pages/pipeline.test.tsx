import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { MemoryRouter } from "react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type { PipelineRun, SavedPipeline, StepRecord } from "../api/client";
import App from "../App";

const PIPELINE = "/api/datasets/demo_csv/customers/pipelines/customers_built";
const PAGE = "/pipeline?source=demo_csv&dataset=customers&name=customers_built";
const EXECUTION = "01a0a3de-9cb0-73ec-be37-1caa01588b64";

const SAVED: SavedPipeline = {
  name: "customers_built",
  source: "demo_csv",
  dataset: "customers",
  version: 1,
  saved_at: "2026-10-05T08:00:00Z",
  profile: "ends",
  constraints: [{ critical: true, constraint: "not_null", column: "customer_id" }],
  steps: [
    { type: "normalize_values", columns: ["city"], trim: true, case: null, mapping: {} },
    { type: "fill_missing", columns: ["lifetime_value"], method: "median", value: null },
    {
      type: "outliers",
      column: "lifetime_value",
      action: "flag",
      lower_percentile: 1,
      upper_percentile: 99,
    },
  ],
  columns: [
    { name: "customer_id", type: "bigint" },
    { name: "city", type: "text" },
    { name: "lifetime_value", type: "numeric" },
  ],
};

function step(position: number, type: string, done: Partial<StepRecord>): StepRecord {
  return { position, type, configuration: {}, status: "not_run", ...done };
}

function profile(stage: "raw" | "clean", counts: number[]) {
  const [table_rows, missing_values, invalid_values, outliers, duplicates] = counts as [
    number,
    number,
    number,
    number,
    number,
  ];
  return {
    profile_id: stage === "raw" ? 1 : 2,
    stage,
    after_step: null,
    table_rows,
    missing_values,
    invalid_values,
    outliers,
    duplicates,
    profiled_at: "2026-10-05T08:01:00Z",
  };
}

const SUCCEEDED: PipelineRun = {
  execution_id: EXECUTION,
  pipeline: "customers_built",
  pipeline_id: 1,
  version: 1,
  source: "demo_csv",
  dataset: "customers",
  trigger: "manual",
  status: "succeeded",
  started_at: "2026-10-05T08:01:00Z",
  ended_at: "2026-10-05T08:01:02Z",
  rows_in: 20,
  rows_out: 18,
  input_run_id: EXECUTION,
  failed_step: null,
  error_class: null,
  error: null,
  steps: [
    step(1, "normalize_values", { status: "succeeded", rows_in: 20, rows_out: 20, values_changed: 3 }),
    step(2, "fill_missing", { status: "succeeded", rows_in: 20, rows_out: 20, values_changed: 1 }),
    step(3, "outliers", { status: "succeeded", rows_in: 20, rows_out: 18, values_changed: 0 }),
  ],
  profiles: [profile("raw", [20, 4, 2, 3, 1]), profile("clean", [18, 1, 2, 0, 0])],
  validation: [
    {
      position: 1,
      constraint: "not_null",
      columns: ["customer_id"],
      critical: true,
      passed: true,
      failing_rows: 0,
      failing_values: 0,
      message: "no customer_id is null",
    },
  ],
  lineage: [],
};

const FAILED: PipelineRun = {
  ...SUCCEEDED,
  status: "failed",
  rows_out: null,
  failed_step: 2,
  error_class: "StepFailed",
  error: "step 2 (fill_missing): median needs a numeric column; lifetime_value is text",
  steps: [
    SUCCEEDED.steps[0]!,
    step(2, "fill_missing", {
      status: "failed",
      rows_in: 20,
      error: "median needs a numeric column; lifetime_value is text",
    }),
    step(3, "outliers", {}),
  ],
  profiles: [profile("raw", [20, 4, 2, 3, 1])],
  validation: [],
};

type Answer = { body: unknown; status?: number };

/** What the page sent with a PUT or a POST, newest last. */
const sent: string[] = [];

function serve(answers: Record<string, Answer>) {
  const asked: string[] = [];
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = new URL(String(input), "http://console.test");
      const key = `${init?.method ?? "GET"} ${url.pathname}`;
      asked.push(key);
      if (typeof init?.body === "string") sent.push(init.body);
      const answer = answers[key] ?? { body: { detail: `no fixture for ${key}` }, status: 404 };
      return new Response(JSON.stringify(answer.body), {
        status: answer.status ?? 200,
        headers: { "Content-Type": "application/json" },
      });
    }),
  );
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

const BASE: Record<string, Answer> = {
  "GET /api/datasets": { body: [] },
  [`GET ${PIPELINE}`]: { body: SAVED },
};

/** Nothing but reads: no save, and above all no run. */
function onlyReads(asked: string[]) {
  return asked.every((request) => request.startsWith("GET "));
}

function blockNames(): string[] {
  return within(screen.getByRole("list", { name: "Blocks" }))
    .queryAllByRole("region")
    .map((region) => region.getAttribute("aria-label") ?? "");
}

function block(name: string): HTMLElement {
  return screen.getByRole("region", { name });
}

beforeEach(() => {
  vi.unstubAllGlobals();
  sent.length = 0;
  window.localStorage.clear();
});

describe("the pipeline builder", () => {
  it("draws a pipeline of several steps as blocks from SOURCE through RAW to CLEAN", async () => {
    const asked = serve(BASE);

    show(PAGE);

    await screen.findByRole("region", { name: "Step 3 · Outliers" });
    expect(blockNames()).toEqual([
      "Source",
      "RAW",
      "Step 1 · Normalize values",
      "Step 2 · Fill missing values",
      "Step 3 · Outliers",
      "Validate",
      "CLEAN",
    ]);
    expect(within(block("RAW")).getByText("raw.demo_csv__customers")).toBeTruthy();
    expect(within(block("CLEAN")).getByText("clean.demo_csv__customers")).toBeTruthy();
    expect(screen.getByText(/version 1 · saved/)).toBeTruthy();
    expect(onlyReads(asked)).toBe(true);
  });

  it("adds, edits, reorders and removes steps in the draft and runs nothing", async () => {
    const asked = serve(BASE);
    show(PAGE);
    await screen.findByRole("region", { name: "Step 3 · Outliers" });
    const run = screen.getByRole("button", { name: "Run" }) as HTMLButtonElement;
    expect(run.disabled).toBe(false);

    // Adding: a new block at the end, its column already one the dataset has.
    fireEvent.change(screen.getByLabelText("Step type"), { target: { value: "convert_type" } });
    fireEvent.click(screen.getByRole("button", { name: "Add step" }));
    const added = block("Step 4 · Convert type");
    expect((within(added).getByLabelText("Column") as HTMLSelectElement).value).toBe("customer_id");
    expect(within(added).getAllByRole("option").map((option) => option.textContent)).toContain(
      "lifetime_value",
    );

    // Editing a parameter.
    const upper = within(block("Step 3 · Outliers")).getByLabelText(
      "Upper percentile",
    ) as HTMLInputElement;
    fireEvent.change(upper, { target: { value: "95" } });
    expect(
      (within(block("Step 3 · Outliers")).getByLabelText("Upper percentile") as HTMLInputElement)
        .value,
    ).toBe("95");

    // Reordering.
    fireEvent.click(screen.getByRole("button", { name: "Move step 1 down" }));
    expect(blockNames().slice(2, 4)).toEqual([
      "Step 1 · Fill missing values",
      "Step 2 · Normalize values",
    ]);
    expect((screen.getByRole("button", { name: "Move step 1 up" }) as HTMLButtonElement).disabled).toBe(
      true,
    );

    // Removing.
    fireEvent.click(screen.getByRole("button", { name: "Remove step 2" }));
    expect(blockNames().slice(2, -2)).toEqual([
      "Step 1 · Fill missing values",
      "Step 2 · Outliers",
      "Step 3 · Convert type",
    ]);

    expect(screen.getByText("unsaved")).toBeTruthy();
    expect(run.disabled).toBe(true);
    expect(onlyReads(asked)).toBe(true);
  });

  it("saves the draft as a version and starts a run only when Run is pressed", async () => {
    const stored = { ...SAVED, version: 2, steps: SAVED.steps.slice(1) };
    const asked = serve({
      ...BASE,
      [`PUT ${PIPELINE}`]: { body: stored },
      [`POST ${PIPELINE}/runs`]: {
        body: {
          execution_id: EXECUTION,
          pipeline: "customers_built",
          version: 2,
          source: "demo_csv",
          dataset: "customers",
          requested_at: "2026-10-05T08:01:00Z",
        },
        status: 202,
      },
      [`GET /api/pipeline-runs/${EXECUTION}`]: { body: { ...SUCCEEDED, version: 2 } },
    });
    show(PAGE);
    await screen.findByRole("region", { name: "Step 3 · Outliers" });

    fireEvent.click(screen.getByRole("button", { name: "Remove step 1" }));
    fireEvent.click(screen.getByRole("button", { name: "Save" }));

    await screen.findByText(/version 2 · saved/);
    expect(asked).toContain(`PUT ${PIPELINE}`);
    expect(asked).not.toContain(`POST ${PIPELINE}/runs`);
    const body = JSON.parse(sent[0]!) as { profile: string; steps: { type: string }[] };
    expect(body.profile).toBe("ends");
    expect(body.steps.map((item) => item.type)).toEqual(["fill_missing", "outliers"]);
    // A hidden field is not sent: the fill value only belongs to the "value" method.
    expect(body.steps[0]).not.toHaveProperty("value");
    expect(screen.queryByText("unsaved")).toBe(null);
    expect(window.localStorage.length).toBe(0);

    fireEvent.click(screen.getByRole("button", { name: "Run" }));
    await waitFor(() => expect(asked).toContain(`POST ${PIPELINE}/runs`));
    expect(await within(block("CLEAN")).findByText("succeeded")).toBeTruthy();
  });

  it("refuses to save an invalid parameter with a message naming the step and the field", async () => {
    const asked = serve({
      ...BASE,
      [`PUT ${PIPELINE}`]: {
        status: 422,
        body: {
          detail: [
            {
              loc: ["steps", 3, "upper_percentile"],
              msg: "Input should be less than or equal to 100",
            },
          ],
        },
      },
    });
    show(PAGE);
    await screen.findByRole("region", { name: "Step 3 · Outliers" });

    fireEvent.change(within(block("Step 3 · Outliers")).getByLabelText("Upper percentile"), {
      target: { value: "150" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Save" }));

    const refusal = await screen.findByRole("alert");
    expect(refusal.textContent).toContain(
      "Step 3 (outliers): upper_percentile: Input should be less than or equal to 100",
    );
    expect(refusal.textContent).toContain("Not saved.");
    expect(
      within(block("Step 3 · Outliers")).getByText(
        "upper_percentile: Input should be less than or equal to 100",
      ),
    ).toBeTruthy();
    expect(screen.getByText(/version 1 · saved/)).toBeTruthy();
    expect(screen.getByText("unsaved")).toBeTruthy();
    expect((screen.getByRole("button", { name: "Run" }) as HTMLButtonElement).disabled).toBe(true);
    expect(asked.filter((request) => request.startsWith("POST "))).toEqual([]);
  });

  it("shows each step's result after a run, and on the failed step why it failed", async () => {
    serve({ ...BASE, [`GET /api/pipeline-runs/${EXECUTION}`]: { body: FAILED } });

    show(`${PAGE}&run=${EXECUTION}`);

    const first = await screen.findByRole("region", { name: "Step 1 · Normalize values" });
    await within(first).findByText("succeeded");
    expect(within(first).getByText("Rows in").nextSibling?.textContent).toBe("20");
    expect(within(first).getByText("Rows out").nextSibling?.textContent).toBe("20");
    expect(within(first).getByText("Values changed").nextSibling?.textContent).toBe("3");
    const second = block("Step 2 · Fill missing values");
    expect(within(second).getByText("failed")).toBeTruthy();
    expect(
      within(second).getByText("median needs a numeric column; lifetime_value is text"),
    ).toBeTruthy();
    expect(within(block("Step 3 · Outliers")).getByText("not run")).toBeTruthy();
    expect(within(block("CLEAN")).getByText("failed")).toBeTruthy();
    // A run that ended at a step has no CLEAN profile to compare with.
    expect(screen.queryByRole("region", { name: "Before and after" })).toBe(null);
  });

  it("compares rows, missing, invalid, outliers and duplicates before and after a finished run", async () => {
    serve({ ...BASE, [`GET /api/pipeline-runs/${EXECUTION}`]: { body: SUCCEEDED } });

    show(`${PAGE}&run=${EXECUTION}`);

    const table = await screen.findByRole("region", { name: "Before and after" });
    const rows = within(table)
      .getAllByRole("row")
      .slice(1)
      .map((row) => [...row.querySelectorAll("td")].map((cell) => cell.textContent));
    expect(rows).toEqual([
      ["Rows", "20", "18", "-2"],
      ["Missing values", "4", "1", "-3"],
      ["Invalid values", "2", "2", "0"],
      ["Outliers", "3", "0", "-3"],
      ["Duplicates", "1", "0", "-1"],
    ]);
    expect(within(block("Validate")).getByText("passed")).toBeTruthy();
  });

  it("keeps an unsaved draft across a reload", async () => {
    serve(BASE);
    const first = show(PAGE);
    await screen.findByRole("region", { name: "Step 3 · Outliers" });
    fireEvent.change(screen.getByLabelText("Step type"), { target: { value: "drop_missing" } });
    fireEvent.click(screen.getByRole("button", { name: "Add step" }));
    first.unmount();

    show(PAGE);

    expect(
      await screen.findByRole("region", { name: "Step 4 · Drop rows with missing values" }),
    ).toBeTruthy();
    expect(screen.getByText("unsaved")).toBeTruthy();
  });

  it("starts a never-saved pipeline empty, with the dataset picked and nothing to run", async () => {
    const asked = serve({
      "GET /api/datasets": {
        body: [{ source: "demo_csv", dataset: "customers" }],
      },
      [`GET ${PIPELINE}`]: {
        body: { ...SAVED, version: null, saved_at: null, steps: [], constraints: [] },
      },
    });

    show(PAGE);

    await screen.findByText("never saved");
    expect(blockNames()).toEqual(["Source", "RAW", "Validate", "CLEAN"]);
    expect((screen.getByRole("button", { name: "Run" }) as HTMLButtonElement).disabled).toBe(true);
    expect((screen.getByRole("button", { name: "Save" }) as HTMLButtonElement).disabled).toBe(false);
    expect((screen.getByLabelText("Dataset") as HTMLSelectElement).value).toBe("demo_csv/customers");
    expect(onlyReads(asked)).toBe(true);
  });

  it("is reached from a dataset's page", async () => {
    serve({});

    show("/datasets/demo_csv/customers");

    const link = await screen.findByRole("link", { name: "Build pipeline" });
    expect(link.getAttribute("href")).toBe("/pipeline?source=demo_csv&dataset=customers");
  });
});
