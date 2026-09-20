/** The API key the dashboard sends. A page in a browser has nowhere else to keep one, so it
 * lives in this browser's own storage: never sent anywhere but this API, never logged. */
const STORED = "udp.apiKey";

type Listener = (refused: boolean) => void;

const listeners = new Set<Listener>();
let refused = false;

/** Storage can be switched off or full; a dashboard with no stored key still works when the
 * API needs none, so every use of it is allowed to fail. */
function storage(): Storage | null {
  try {
    return window.localStorage;
  } catch {
    return null;
  }
}

export function apiKey(): string {
  try {
    return storage()?.getItem(STORED) ?? "";
  } catch {
    return "";
  }
}

export function rememberKey(key: string): void {
  try {
    if (key) storage()?.setItem(STORED, key);
    else storage()?.removeItem(STORED);
  } catch {
    // A browser that refuses storage keeps the key for this page only.
  }
  tell(false);
}

/** Called when the API refuses a request: the stored key is wrong or missing, so it goes. */
export function keyWasRefused(): void {
  try {
    storage()?.removeItem(STORED);
  } catch {
    // Nothing to clear.
  }
  tell(true);
}

export function keyIsNeeded(): boolean {
  return refused;
}

export function whenKeyIsNeeded(listener: Listener): () => void {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

function tell(needed: boolean): void {
  refused = needed;
  for (const listener of listeners) listener(needed);
}
