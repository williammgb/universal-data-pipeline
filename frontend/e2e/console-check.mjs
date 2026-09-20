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
  `${dataset}?tab=preview`,
  `${dataset}?tab=quality`,
  `${dataset}?tab=runs`,
  "/runs",
  "/runs?status=succeeded",
  `/runs/${runId}`,
];

const browser = await chromium.launch();
let failed = false;
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
} finally {
  await browser.close();
}
process.exit(failed ? 1 : 0);
