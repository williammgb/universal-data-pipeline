// The browser check for ticket #50: the Guide tab's "Adding a source" tells a person to run
// `udp new my_shop`, shows the source.yaml it writes, then `udp load my_shop`, in the light and
// the dark theme. The built dashboard is served by vite's own preview server; the guide reads
// nothing from the API, and any request the page makes anyway is answered with an empty list.
// Usage: ./run ui
import { mkdirSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { chromium } from "playwright";
import { preview } from "vite";

const root = fileURLToPath(new URL("..", import.meta.url));
const screens = fileURLToPath(new URL("../../.work/screens/", import.meta.url));
mkdirSync(screens, { recursive: true });

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
  preview: { host: "127.0.0.1", port: 45494 },
});
const base = server.resolvedUrls.local[0].replace(/\/$/, "");
const browser = await chromium.launch();

async function guide(theme) {
  const inside = await browser.newContext({
    colorScheme: theme,
    viewport: { width: 1280, height: 900 },
  });
  await inside.route(
    (url) => url.pathname.startsWith("/api/"),
    (route) => route.fulfill({ status: 200, json: [] }),
  );
  const page = await inside.newPage();
  page.on("console", (message) => {
    if (message.type() === "error") check(false, `${theme}: console error: ${message.text()}`);
  });
  page.on("pageerror", (error) => check(false, `${theme}: page error: ${error.message}`));
  await page.goto(`${base}/guide#adding-a-source`, { waitUntil: "networkidle", timeout: 30_000 });

  const section = page.locator("section.guide-section", { has: page.locator("h2#adding-a-source") });
  await section.waitFor();
  const text = await section.innerText();
  const codes = await section.locator("p code").allTextContents();
  check(codes.includes("udp new my_shop"), `${theme}: the section names \`udp new my_shop\``);
  check(codes.includes("udp load my_shop"), `${theme}: the section names \`udp load my_shop\``);
  check(
    text.indexOf("udp new my_shop") < text.indexOf("udp load my_shop"),
    `${theme}: \`udp new\` comes before \`udp load\``,
  );
  const yaml = await section.locator("pre code").textContent();
  check(
    yaml.startsWith("# Made by udp new. Every setting: docs/adding-a-source.md\nconnection:"),
    `${theme}: the YAML shown starts with the header udp new writes`,
  );
  check(!text.includes("./udp"), `${theme}: the section has no ./udp command left`);

  await section.scrollIntoViewIfNeeded();
  await section.screenshot({ path: `${screens}ticket-50-guide-adding-a-source-${theme}.png` });
  await inside.close();
}

try {
  for (const theme of ["light", "dark"]) await guide(theme);
} catch (error) {
  check(false, `the check stopped: ${error.message.split("\n")[0]}`);
} finally {
  await browser.close();
  await server.close();
}

console.log(`ticket-50: ${checks} checks, ${failures.length} failed, screenshots in .work/screens`);
process.exit(failures.length ? 1 : 0);
