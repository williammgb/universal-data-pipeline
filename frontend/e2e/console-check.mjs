// Opens every dashboard page in headless chromium and fails when any page logs a console error,
// throws, has a request fail, or never finishes loading. Usage: node console-check.mjs <base url>
import { chromium } from "playwright";

const base = (process.argv[2] ?? "").replace(/\/$/, "");
if (!base) {
  console.error("usage: node console-check.mjs <base url>");
  process.exit(2);
}

// The API may need a key; the browser reads it from storage the way a person's browser would.
const key = process.env.UDP_API_KEY ?? "";
const latest = await fetch(`${base}/api/runs?limit=1`, {
  headers: key ? { "X-API-Key": key } : {},
}).then((response) => response.json());
const runId = latest.runs?.[0]?.run_id;
if (!runId) {
  console.error("no run to open: the smoke loads the demo sources before this check");
  process.exit(1);
}

const dataset = "/datasets/demo_csv/customers";
const addresses = [
  "/",
  `${dataset}?tab=schema`,
  `${dataset}?tab=profile`,
  `${dataset}?tab=preview`,
  `${dataset}?tab=quality`,
  `${dataset}?tab=runs`,
  `${dataset}?tab=config`,
  "/runs",
  "/runs?status=succeeded",
  `/runs/${runId}`,
  "/settings",
];

const browser = await chromium.launch();
let failed = false;

/** Saves one edit from the configuration tab and checks it comes back as a change. */
async function editTheConfiguration(page) {
  const problems = [];
  await page.goto(`${base}${dataset}?tab=config`, { waitUntil: "networkidle", timeout: 30_000 });
  const threshold = page.getByLabel("Threshold, percent of rows");
  await threshold.waitFor({ timeout: 10_000 });
  await threshold.fill("7");
  await page.getByRole("button", { name: "Save" }).click();
  try {
    // The saved change, not the line above the Save button: that one names the field before
    // anything is saved, so waiting on it would go on while the save was still in flight.
    await page.locator(".change", { hasText: "1 to 7" }).first().waitFor({ timeout: 10_000 });
  } catch (error) {
    problems.push(`the saved edit never showed up: ${error.message.split("\n")[0]}`);
  }
  if (await page.getByText("Not saved.").count()) problems.push("the platform refused the edit");
  if ((await threshold.inputValue()) !== "7") problems.push("the saved value did not stay");

  // Taken back again, both to exercise the way back and to leave the demo source exactly as its
  // file has it: an edit in force changes what a run depends on, and the backup check that comes
  // after this one expects the next run to skip an unchanged file.
  await page.getByRole("button", { name: /use the file/ }).first().click();
  await page.getByRole("button", { name: "Save" }).click();
  try {
    await page.getByText("Nothing is edited").waitFor({ timeout: 10_000 });
  } catch (error) {
    problems.push(`the edit could not be taken back: ${error.message.split("\n")[0]}`);
  }
  return problems;
}

try {
  for (const address of addresses) {
    const page = await browser.newPage();
    if (key) {
      await page.addInitScript((stored) => {
        window.localStorage.setItem("udp.apiKey", stored);
      }, key);
    }
    const problems = [];
    page.on("console", (message) => {
      if (message.type() === "error") problems.push(`console: ${message.text()}`);
    });
    page.on("pageerror", (error) => problems.push(`page error: ${error.message}`));
    page.on("requestfailed", (request) =>
      problems.push(`request failed: ${request.url()} ${request.failure()?.errorText ?? ""}`),
    );
    page.on("response", (response) => {
      if (response.status() >= 400) problems.push(`${response.status()}: ${response.url()}`);
    });

    try {
      await page.goto(base + address, { waitUntil: "networkidle", timeout: 30_000 });
      await page.locator("main h1").first().waitFor({ timeout: 10_000 });
      if (await page.getByText("Loading…").count()) problems.push("still loading");
    } catch (error) {
      problems.push(`did not load: ${error.message.split("\n")[0]}`);
    }
    await page.close();

    console.log(`${address}: ${problems.length} errors (console, page, requests)`);
    for (const problem of problems) console.log(`  ${problem}`);
    failed ||= problems.length > 0;
  }

  const page = await browser.newPage();
  if (key) {
    await page.addInitScript((stored) => {
      window.localStorage.setItem("udp.apiKey", stored);
    }, key);
  }
  const problems = [];
  page.on("console", (message) => {
    if (message.type() === "error") problems.push(`console: ${message.text()}`);
  });
  page.on("pageerror", (error) => problems.push(`page error: ${error.message}`));
  problems.push(...(await editTheConfiguration(page)));
  await page.close();
  console.log(`${dataset}?tab=config (saving an edit): ${problems.length} errors`);
  for (const problem of problems) console.log(`  ${problem}`);
  failed ||= problems.length > 0;
} finally {
  await browser.close();
}
process.exit(failed ? 1 : 0);
