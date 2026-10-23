// Takes the README's screenshots from a running platform with the demo sources loaded, into
// assets/. It first runs the demo_csv_customers pipeline, so the pipeline page and the runs list
// show one. Usage: node frontend/e2e/screens.mjs http://127.0.0.1:8000 [api key]
import { fileURLToPath } from "node:url";

import { chromium } from "playwright";

const base = (process.argv[2] ?? "http://127.0.0.1:8000").replace(/\/$/, "");
const key = process.argv[3] ?? "";
const assets = fileURLToPath(new URL("../../assets/", import.meta.url));
const headers = key ? { "X-API-Key": key } : {};

async function api(path, init = {}) {
  const response = await fetch(`${base}/api${path}`, {
    ...init,
    headers: { ...headers, ...(init.headers ?? {}) },
  });
  if (!response.ok) throw new Error(`${path}: ${response.status} ${await response.text()}`);
  return response.json();
}

const PIPELINE = "demo_csv_customers";
const { execution_id: run } = await api(`/pipelines/${PIPELINE}/runs`, { method: "POST" });
for (let tries = 0; ; tries += 1) {
  const state = await api(`/pipeline-runs/${run}`);
  if (state.status !== "running") break;
  if (tries > 60) throw new Error("the pipeline run did not finish in a minute");
  await new Promise((done) => setTimeout(done, 1000));
}

const SHOTS = [
  ["v2_datasets", "/"],
  ["v2_schema", "/datasets/demo_csv/customers"],
  ["v2_dataprofiling", "/datasets/demo_csv/customers?tab=profile"],
  ["v2_dataset", "/datasets/demo_db/orders?tab=preview"],
  ["v2_configuration", "/datasets/demo_csv/customers?tab=config"],
  ["v2_runs", "/runs"],
  [
    "v2_pipeline",
    `/pipeline?source=demo_csv&dataset=customers&name=${PIPELINE}&run=${run}`,
  ],
];

const browser = await chromium.launch();
const page = await browser.newPage({ viewport: { width: 1600, height: 1000 } });
await page.emulateMedia({ colorScheme: "light" });
if (key) {
  await page.goto(base);
  await page.evaluate((value) => localStorage.setItem("udp.apiKey", value), key);
}
for (const [name, path] of SHOTS) {
  await page.goto(`${base}${path}`);
  await page.waitForLoadState("networkidle");
  await page.waitForTimeout(500);
  await page.screenshot({ path: `${assets}${name}.png` });
  console.log(`${name}.png`);
}
await browser.close();
