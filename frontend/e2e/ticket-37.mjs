// The browser check for ticket #37: the "Run requested" pop-up, the error text in the run lists
// and the restyled dropdowns, in the light and the dark theme. The built dashboard is served by
// vite's own preview server and the API is answered here, so the pages can show what the demo
// data has none of: a failed run with a two-line message. Usage: ./run ui
import { mkdirSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { chromium } from "playwright";
import { preview } from "vite";

const root = fileURLToPath(new URL("..", import.meta.url));
const screens = fileURLToPath(new URL("../../.work/screens/", import.meta.url));
mkdirSync(screens, { recursive: true });

const RUN_ID = "01a0a3c1-77be-7d41-9f2e-6b0a6d4b18cc";
const failed = {
  run_id: RUN_ID,
  source: "pagila",
  dataset: "actor",
  trigger: "manual",
  status: "failed",
  started_at: "2026-10-12T17:41:07Z",
  ended_at: "2026-10-12T17:41:09Z",
  rows_extracted: 0,
  rows_loaded: null,
  rows_quarantined: 0,
  error_class: "ExtractError",
  error_message:
    "could not read table 'actor' from postgresql+psycopg://source:***@source-postgres:5432/source: (psycopg.OperationalError) failed to resolve host 'source-postgres': [Errno -5] No address associated with hostname\n(Background on this error at: https://sqlalche.me/e/20/e3q8)",
};
const RUNS = {
  runs: [
    failed,
    {
      ...failed,
      run_id: "01a0a3c1-77be-7d41-9f2e-6b0a6d4b18cd",
      started_at: "2026-10-12T17:30:00Z",
      error_message: `https://example.test/${"averylongaddresswithnospacesinit".repeat(8)}`,
    },
    {
      ...failed,
      run_id: "01a0a3c1-77be-7d41-9f2e-6b0a6d4b18ce",
      started_at: "2026-10-12T17:20:00Z",
      error_message: null,
    },
    {
      ...failed,
      run_id: "01a0a3c1-77be-7d41-9f2e-6b0a6d4b18cf",
      status: "succeeded",
      started_at: "2026-10-12T17:10:00Z",
      rows_extracted: 200,
      rows_loaded: 200,
      error_class: null,
      error_message: null,
    },
  ],
  limit: 50,
  offset: 0,
  has_more: false,
};
const COLUMNS = [
  { name: "actor_id", type: "bigint" },
  { name: "first_name", type: "text" },
];
const DETAIL = {
  source: "pagila",
  dataset: "actor",
  connector_type: "database",
  table_name: "pagila__actor",
  load_mode: "full",
  schedule: null,
  recorded_at: "2026-10-12T17:10:00Z",
  table_rows: 200,
  last_run: {
    run_id: RUN_ID,
    status: "failed",
    trigger: "manual",
    started_at: failed.started_at,
    ended_at: failed.ended_at,
    rows_loaded: null,
  },
  primary_key: [],
  watermark: null,
  definition: { name: "actor", columns: { actor_id: "integer" } },
  columns: COLUMNS,
  versions: [{ version: 1, run_id: RUN_ID, recorded_at: "2026-10-12T17:10:00Z", columns: COLUMNS }],
  state: {
    watermark_column: null,
    watermark_type: null,
    watermark: null,
    file_path: null,
    file_sha256: null,
    run_id: RUN_ID,
    saved_at: "2026-10-12T17:10:02Z",
  },
};
const LINEAGE_RUN = {
  execution_id: "01a1b2c3-0000-7000-8000-000000000001",
  pipeline: "actor_names_trimmed_deduplicated_and_normalised_for_the_quarterly_report",
  version: 4,
  status: "succeeded",
  started_at: "2026-10-12T17:10:00Z",
};
const SAVED = {
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
    { type: "outliers", column: "lifetime_value", action: "flag", lower_percentile: 1, upper_percentile: 99 },
  ],
  columns: [
    { name: "customer_id", type: "bigint" },
    { name: "city", type: "text" },
    { name: "lifetime_value", type: "numeric" },
  ],
};
const ANSWERS = {
  "GET /api/sources": { body: [{ source: "pagila", connector_type: "database", datasets: 1, recorded_at: DETAIL.recorded_at }] },
  "GET /api/datasets": { body: [] },
  "GET /api/datasets/pagila/actor": { body: DETAIL },
  "GET /api/runs": { body: RUNS },
  "POST /api/runs": {
    body: { source: "pagila", datasets: ["actor"], requested_at: "2026-10-12T17:49:21Z" },
    status: 202,
  },
  "GET /api/datasets/pagila/actor/lineage": {
    body: { run: LINEAGE_RUN, runs: [LINEAGE_RUN], chain: [] },
  },
  "GET /api/datasets/demo_csv/customers/pipelines/customers_built": { body: SAVED },
};

const failures = [];
let checks = 0;
function check(ok, line) {
  checks += 1;
  console.log(`${ok ? "ok" : "NOT OK"}: ${line}`);
  if (!ok) failures.push(line);
}

const server = await preview({
  root,
  configFile: false,
  logLevel: "error",
  preview: { host: "127.0.0.1", port: 45493 },
});
const base = server.resolvedUrls.local[0].replace(/\/$/, "");
const browser = await chromium.launch();

async function context(theme, more = {}) {
  const made = await browser.newContext({
    colorScheme: theme,
    viewport: { width: 1280, height: 900 },
    ...more,
  });
  await made.route(
    (url) => url.pathname.startsWith("/api/"),
    async (route) => {
      const request = route.request();
      const key = `${request.method()} ${new URL(request.url()).pathname}`;
      const found = ANSWERS[key];
      if (!found) check(false, `the page asked for ${key}, which this check has no answer for`);
      await route.fulfill({
        status: found?.status ?? (found ? 200 : 404),
        json: found?.body ?? { detail: "no answer" },
      });
    },
  );
  return made;
}

async function open(inside, address) {
  const page = await inside.newPage();
  page.on("console", (message) => {
    if (message.type() === "error") check(false, `console error on ${address}: ${message.text()}`);
  });
  page.on("pageerror", (error) => check(false, `page error on ${address}: ${error.message}`));
  await page.goto(base + address, { waitUntil: "networkidle", timeout: 30_000 });
  return page;
}

/** A theme token as the browser resolves it, to compare with a computed colour. */
function token(page, name) {
  return page.evaluate((wanted) => {
    const probe = document.createElement("span");
    probe.style.color = `var(${wanted})`;
    document.body.append(probe);
    const colour = getComputedStyle(probe).color;
    probe.remove();
    return colour;
  }, name);
}

function sideways(page) {
  return page.evaluate(
    () => document.documentElement.scrollWidth - document.documentElement.clientWidth,
  );
}

async function datasetPage(inside, theme) {
  const page = await open(inside, "/datasets/pagila/actor?tab=runs");
  const button = page.getByRole("button", { name: "Run now" });
  await page.locator(".why-text").first().waitFor();
  const tabsBefore = await page.locator(".tabs").boundingBox();

  await button.click();
  const note = page.getByRole("status");
  await note.waitFor();
  const shown = Date.now();
  const sliding = await note.evaluate((node) => getComputedStyle(node).animationName);
  check(sliding === "popup-in", `${theme}: the note slides in (animation ${sliding})`);
  await page.waitForTimeout(400);

  const text = await note.textContent();
  check(text === "Run requested at 2026-10-12 19:49:21", `${theme}: the note reads "${text}"`);
  const noteBox = await note.boundingBox();
  const groupBox = await page.locator(".head .right").boundingBox();
  const buttonBox = await button.boundingBox();
  const tabsWith = await page.locator(".tabs").boundingBox();
  const under = noteBox.y - (groupBox.y + groupBox.height);
  check(Math.abs(under - 8) < 1, `${theme}: the note starts ${under}px under the buttons`);
  const edge = noteBox.x + noteBox.width - (buttonBox.x + buttonBox.width);
  check(Math.abs(edge) < 1, `${theme}: the note's right edge is ${edge}px from the Run now button's`);
  check(
    tabsWith.y === tabsBefore.y,
    `${theme}: the tabs are at ${tabsWith.y}px with the note shown, ${tabsBefore.y}px before it`,
  );
  const over = await note.evaluate((node) => {
    const box = node.getBoundingClientRect();
    return node.contains(
      document.elementFromPoint(box.left + box.width / 2, box.bottom - 3),
    );
  });
  check(
    over && noteBox.y + noteBox.height > tabsWith.y,
    `${theme}: the note is drawn over the tabs (its bottom at ${noteBox.y + noteBox.height}px, the tabs from ${tabsWith.y}px)`,
  );

  const drawn = await note.evaluate((node) => {
    const style = getComputedStyle(node);
    const dot = getComputedStyle(node, "::before");
    return {
      ground: style.backgroundColor,
      ink: style.color,
      line: style.borderTopColor,
      shadow: style.boxShadow,
      dot: dot.backgroundColor,
      when: getComputedStyle(node.querySelector(".when")).fontFamily,
    };
  });
  check(drawn.ground === (await token(page, "--panel")), `${theme}: the note's ground is ${drawn.ground}, the panel colour`);
  check(drawn.ink === (await token(page, "--ink")), `${theme}: the note's text is ${drawn.ink}, the text colour`);
  check(drawn.line === (await token(page, "--line")), `${theme}: the note's border is ${drawn.line}, the line colour`);
  check(drawn.dot === (await token(page, "--ok")), `${theme}: the note's dot is ${drawn.dot}, the ok colour`);
  check(drawn.shadow !== "none", `${theme}: the note's shadow is ${drawn.shadow}`);
  check(/mono/i.test(drawn.when), `${theme}: the time is in the mono face`);

  const reasons = await page.evaluate(() =>
    [...document.querySelectorAll("td.why .why-text")].map((node) => {
      const style = getComputedStyle(node);
      const name = getComputedStyle(node.querySelector("b"));
      const panel = node.closest(".panel");
      return {
        text: node.textContent,
        title: node.title,
        lines: node.getBoundingClientRect().height / parseFloat(style.lineHeight),
        cut: node.scrollHeight > node.clientHeight,
        ink: style.color,
        name: name.color,
        weight: name.fontWeight,
        face: name.fontFamily,
        spill: panel.scrollWidth - panel.clientWidth,
      };
    }),
  );
  const [twoLines, oneWord, nameOnly] = reasons;
  check(reasons.length === 3, `${theme}: three of the four runs have a reason, the fourth a dash`);
  check(
    twoLines.text.startsWith("ExtractError could not read table 'actor'") &&
      !twoLines.text.includes("Background on this error") &&
      twoLines.title.includes("Background on this error"),
    `${theme}: the Runs tab shows the name and the first line; the second line is only in the tooltip`,
  );
  // The mono name makes a line a fraction taller than the text's own line height, so the
  // height is rounded to whole lines.
  check(
    Math.round(twoLines.lines) === 2 && twoLines.cut,
    `${theme}: a long reason is cut after ${Math.round(twoLines.lines)} lines`,
  );
  check(
    Math.round(oneWord.lines) === 2 && oneWord.cut && oneWord.spill === 0,
    `${theme}: a reason of one long word breaks and is cut after ${Math.round(oneWord.lines)} lines, the table ${oneWord.spill}px wider than its panel`,
  );
  check(nameOnly.text === "ExtractError", `${theme}: a run with no message shows "${nameOnly.text}"`);
  check(twoLines.ink === (await token(page, "--ink-soft")), `${theme}: the reason is ${twoLines.ink}, the soft text colour`);
  check(
    twoLines.name === (await token(page, "--ink")) && twoLines.weight === "600" && /mono/i.test(twoLines.face),
    `${theme}: the error's name is ${twoLines.name}, weight ${twoLines.weight}, in the mono face`,
  );
  await page.screenshot({ path: `${screens}ticket-37-dataset-note-${theme}.png` });

  await note.waitFor({ state: "detached", timeout: 10_000 });
  const stayed = Date.now() - shown;
  check(stayed > 5500 && stayed < 7000, `${theme}: the note left by itself ${stayed} ms after it appeared`);
  const tabsAfter = await page.locator(".tabs").boundingBox();
  check(tabsAfter.y === tabsBefore.y, `${theme}: the tabs are still at ${tabsAfter.y}px once it has gone`);
  await page.close();
}

async function runsPage(inside, theme) {
  const page = await open(inside, "/runs?status=failed");
  await page.locator(".why-text").first().waitFor();

  // The fields grew from 30px to 38px after #37, and the header gained the Live mark: both
  // expectations follow the dashboard as it is now.
  const height = await page.evaluate(() => getComputedStyle(document.querySelector("select")).height);
  check(height === "38px", `${theme}: getComputedStyle(document.querySelector("select")).height is ${height}`);
  const boxes = await page.evaluate(() =>
    [...document.querySelectorAll(".controls select, .controls input")].map(
      (node) => `${node.id} ${getComputedStyle(node).height} ${getComputedStyle(node).fontSize}`,
    ),
  );
  check(
    boxes.every((box) => box.endsWith(" 38px 13px")),
    `${theme}: every filter is 38px tall with 13px text (${boxes.join(", ")})`,
  );
  const head = await page.locator(".head").evaluate((node) => node.textContent);
  check(head === "RunsLive", `${theme}: the Runs header reads "${head}": the title and the Live mark`);
  const reason = await page.locator("td.why .why-text").first().textContent();
  check(
    reason.startsWith("ExtractError could not read table 'actor'") && !reason.includes("Background"),
    `${theme}: the Runs table shows the name and the first line of the message`,
  );
  await page.screenshot({ path: `${screens}ticket-37-runs-${theme}.png` });

  const styled = await page.evaluate(() => CSS.supports("appearance: base-select"));
  check(styled, `${theme}: this browser styles the opened list (appearance: base-select)`);
  const status = page.locator("#filter-status");
  await status.click();
  await page.waitForFunction(() => document.querySelector("#filter-status").matches(":open"));
  await status.locator("option:not(:checked)").nth(1).hover();
  await page.waitForTimeout(200);
  const list = await status.evaluate((select) => {
    const chosen = select.querySelector("option:checked");
    const pointed = select.querySelector("option:hover");
    const first = select.querySelector("option");
    const option = getComputedStyle(first);
    const panel = getComputedStyle(select, "::picker(select)");
    return {
      chosen: chosen.textContent,
      ink: getComputedStyle(chosen).color,
      weight: getComputedStyle(chosen).fontWeight,
      tick: getComputedStyle(chosen, "::checkmark").order,
      arrow: getComputedStyle(select, "::picker-icon").display,
      pointed: pointed ? getComputedStyle(pointed).backgroundColor : "nothing under the pointer",
      padding: option.padding,
      corners: option.borderRadius,
      gap: first.getBoundingClientRect().top - select.getBoundingClientRect().bottom,
      panel: `${panel.borderTopLeftRadius} corners, ${panel.borderTopWidth} border, shadow ${panel.boxShadow}`,
    };
  });
  check(
    list.ink === (await token(page, "--accent")) && list.weight === "600",
    `${theme}: the chosen option "${list.chosen}" is ${list.ink}, the accent colour, weight ${list.weight}`,
  );
  check(list.tick === "1", `${theme}: the tick is moved to the option's right-hand end (order ${list.tick})`);
  check(list.arrow === "none", `${theme}: the browser's own arrow is hidden (display ${list.arrow})`);
  check(
    list.pointed === (await token(page, "--panel-sunk")),
    `${theme}: the option under the pointer is ${list.pointed}, the sunk panel colour`,
  );
  check(
    list.padding === "5px 9px" && list.corners === "6px",
    `${theme}: an option has ${list.padding} padding and ${list.corners} corners`,
  );
  // 4px of gap, the panel's 1px border and its 4px of padding.
  check(Math.abs(list.gap - 9) < 1, `${theme}: the first option starts ${list.gap}px under the box`);
  console.log(`   ${theme}: the opened list's panel: ${list.panel}`);
  await page.screenshot({ path: `${screens}ticket-37-runs-status-open-${theme}.png` });
  await page.keyboard.press("Escape");
  await page.close();
}

async function pipelinePage(inside, theme) {
  const page = await open(inside, "/pipeline?source=demo_csv&dataset=customers&name=customers_built");
  await page.locator(".block-fields .config-label select").first().waitFor();
  const dropdowns = await page.evaluate(() =>
    [...document.querySelectorAll(".block-fields .config-label select")].map((select) => {
      const style = getComputedStyle(select);
      const pen = document.createElement("canvas").getContext("2d");
      pen.font = `${style.fontWeight} ${style.fontSize} ${style.fontFamily}`;
      const widest = Math.max(
        ...[...select.options].map((option) => pen.measureText(option.textContent).width),
      );
      return {
        value: select.selectedOptions[0]?.textContent ?? "",
        width: select.getBoundingClientRect().width,
        // The widest option, 12px and 27px of padding (the left one was 9px at #37), and the
        // two 1px borders.
        room: widest + 12 + 27 + 2,
        cell: select.closest(".config-row").getBoundingClientRect().width,
      };
    }),
  );
  check(dropdowns.length > 0, `${theme}: the pipeline's steps have ${dropdowns.length} dropdowns`);
  for (const dropdown of dropdowns) {
    check(
      dropdown.width <= dropdown.room + 1 && dropdown.width < dropdown.cell - 20,
      `${theme}: a step's dropdown showing "${dropdown.value}" is ${dropdown.width.toFixed(1)}px wide; its text and arrow need ${dropdown.room.toFixed(1)}px and its cell is ${dropdown.cell.toFixed(1)}px`,
    );
  }
  const fields = await page.evaluate(() =>
    [...document.querySelectorAll("main select, main input:not([type='checkbox'])")].map(
      (node) => getComputedStyle(node).height,
    ),
  );
  check(
    fields.every((height) => height === "38px"),
    `${theme}: the builder's ${fields.length} dropdowns and inputs are all 38px tall (${[...new Set(fields)].join(", ")})`,
  );
  await page.screenshot({ path: `${screens}ticket-37-pipeline-step-${theme}.png`, fullPage: true });
  await page.close();
}

async function narrowScreen(inside, theme) {
  const page = await open(inside, "/datasets/pagila/actor?tab=lineage");
  const run = page.locator(".lineage .controls select");
  await run.waitFor();
  const fit = await run.evaluate((select) => {
    const controls = select.closest(".controls").getBoundingClientRect();
    const style = getComputedStyle(select);
    const pen = document.createElement("canvas").getContext("2d");
    pen.font = `${style.fontWeight} ${style.fontSize} ${style.fontFamily}`;
    return {
      over: select.getBoundingClientRect().right - controls.right,
      // What the chosen value would need, against the room between the box's paddings.
      needs: pen.measureText(select.selectedOptions[0].textContent).width,
      room: select.clientWidth - parseFloat(style.paddingLeft) - parseFloat(style.paddingRight),
      spill: select.scrollWidth - select.clientWidth,
      height: style.height,
    };
  });
  check(
    fit.over <= 0.5 && fit.needs > fit.room && fit.spill === 0 && fit.height === "38px",
    `${theme}, 380px wide: the Lineage dropdown's value needs ${fit.needs.toFixed(0)}px and is cut to the ${fit.room.toFixed(0)}px inside its box (${fit.over}px past its row, ${fit.spill}px into its padding, ${fit.height} tall)`,
  );
  check((await sideways(page)) <= 0, `${theme}, 380px wide: the Lineage tab does not scroll sideways`);
  await page.screenshot({ path: `${screens}ticket-37-lineage-long-value-${theme}.png` });

  await page.goto(`${base}/datasets/pagila/actor?tab=runs`, { waitUntil: "networkidle" });
  const before = await sideways(page);
  await page.getByRole("button", { name: "Run now" }).click();
  const note = page.getByRole("status");
  await note.waitFor();
  await page.waitForTimeout(400);
  const noteBox = await note.boundingBox();
  const groupBox = await page.locator(".head .right").boundingBox();
  check(
    Math.abs(noteBox.x + noteBox.width - (groupBox.x + groupBox.width)) < 1 &&
      noteBox.x >= 0 &&
      Math.abs(noteBox.y - (groupBox.y + groupBox.height) - 8) < 1,
    `${theme}, 380px wide: the note is under the buttons, right-aligned, from ${noteBox.x}px to ${noteBox.x + noteBox.width}px`,
  );
  const after = await sideways(page);
  check(after <= before, `${theme}, 380px wide: the note adds no sideways scrolling (${after}px with it, ${before}px without)`);
  await page.screenshot({ path: `${screens}ticket-37-dataset-note-narrow-${theme}.png` });
  await page.close();
}

async function reducedMotion() {
  const inside = await context("light", { reducedMotion: "reduce" });
  const page = await open(inside, "/datasets/pagila/actor?tab=runs");
  await page.getByRole("button", { name: "Run now" }).click();
  const note = page.getByRole("status");
  await note.waitFor();
  const sliding = await note.evaluate((node) => getComputedStyle(node).animationName);
  check(sliding === "none", `with reduced motion asked for, the note does not slide (animation ${sliding})`);
  await inside.close();
}

try {
  for (const theme of ["light", "dark"]) {
    const wide = await context(theme);
    const probe = await open(wide, "/runs");
    const panel = await token(probe, "--panel");
    await probe.close();
    check(
      panel === (theme === "dark" ? "rgb(23, 30, 40)" : "rgb(255, 255, 255)"),
      `${theme}: the theme in force has the panel colour ${panel}`,
    );
    await datasetPage(wide, theme);
    await runsPage(wide, theme);
    await pipelinePage(wide, theme);
    await wide.close();

    const narrow = await context(theme, { viewport: { width: 380, height: 800 } });
    await narrowScreen(narrow, theme);
    await narrow.close();
  }
  await reducedMotion();
} catch (error) {
  check(false, `the check stopped: ${error.message.split("\n")[0]}`);
} finally {
  await browser.close();
  await server.close();
}

console.log(`ticket-37: ${checks} checks, ${failures.length} failed, screenshots in .work/screens`);
process.exit(failures.length ? 1 : 0);
