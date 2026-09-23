/** How long values show in tables. Kept in this browser only, like the API key: storage can be
 * switched off or full, so every use of it is allowed to fail and the default then holds. */
const STORED = "udp.longValues";

export type LongValues = "shorten" | "full";

/** The class on the page's root element that lets every table show values in full. */
export const FULL_CLASS = "values-full";

function storage(): Storage | null {
  try {
    return window.localStorage;
  } catch {
    return null;
  }
}

export function longValues(): LongValues {
  try {
    return storage()?.getItem(STORED) === "full" ? "full" : "shorten";
  } catch {
    return "shorten";
  }
}

/** Show the choice on the page now; called at start-up and whenever it changes. */
export function applyLongValues(choice: LongValues = longValues()): void {
  document.documentElement.classList.toggle(FULL_CLASS, choice === "full");
}

export function chooseLongValues(choice: LongValues): void {
  try {
    storage()?.setItem(STORED, choice);
  } catch {
    // A browser that refuses storage keeps the choice for this page only.
  }
  applyLongValues(choice);
}
