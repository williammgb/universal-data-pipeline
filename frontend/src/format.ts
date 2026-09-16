/** How values from the API are shown. Times are UTC everywhere, matching the schedules. */

export type Cell = { text: string; isNull: boolean };

export function cell(value: string | number | boolean | null): Cell {
  if (value === null) return { text: "null", isNull: true };
  return { text: typeof value === "string" ? value : String(value), isNull: false };
}

/** An API timestamp as "YYYY-MM-DD HH:MM:SS" in UTC; unreadable input is passed through. */
export function moment(value: string | null): string {
  if (value === null) return "—";
  const when = new Date(value);
  if (Number.isNaN(when.getTime())) return value;
  return when.toISOString().replace("T", " ").slice(0, 19);
}

export function duration(started: string, ended: string | null): string {
  if (ended === null) return "—";
  const seconds = (new Date(ended).getTime() - new Date(started).getTime()) / 1000;
  if (!Number.isFinite(seconds)) return "—";
  if (seconds < 60) return `${seconds.toFixed(1)}s`;
  const minutes = Math.floor(seconds / 60);
  return `${minutes}m ${Math.round(seconds - minutes * 60)}s`;
}

export function count(value: number | null): string {
  return value === null ? "—" : value.toLocaleString("en-GB");
}
