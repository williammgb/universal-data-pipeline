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

const CONNECTOR_NAMES: Record<string, string> = {
  csv: "CSV",
  excel: "Excel",
  database: "Database",
  rest_api: "API",
};

/** What a source is, in the words a person uses for it. */
export function connectorName(type: string): string {
  return CONNECTOR_NAMES[type] ?? type;
}

const DAYS = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];

function whole(field: string, low: number, high: number): number | null {
  if (!/^[0-9]+$/.test(field)) return null;
  const value = Number(field);
  return value >= low && value <= high ? value : null;
}

function every(field: string): number | null {
  const match = /^\*\/([0-9]+)$/.exec(field);
  return match ? Number(match[1]) : null;
}

const two = (value: number) => String(value).padStart(2, "0");

/** A five-field cron schedule in plain English, for the common shapes only; null otherwise,
 * so an unusual schedule is shown as it is written rather than described wrongly. */
export function describeSchedule(schedule: string): string | null {
  const fields = schedule.trim().split(/\s+/);
  if (fields.length !== 5) return null;
  const [minute, hour, day, month, weekday] = fields as [string, string, string, string, string];
  if (month !== "*") return null;
  const everyDay = day === "*" && weekday === "*";
  if (everyDay && hour === "*") {
    if (minute === "*") return "every minute";
    const step = every(minute);
    if (step !== null && step > 0) return step === 1 ? "every minute" : `every ${step} minutes`;
    const at = whole(minute, 0, 59);
    return at === null ? null : `every hour at :${two(at)}`;
  }
  const at = whole(minute, 0, 59);
  if (at === null) return null;
  const hours = every(hour);
  if (everyDay && hours !== null && hours > 0) {
    return hours === 1 ? `every hour at :${two(at)}` : `every ${hours} hours at :${two(at)}`;
  }
  const hh = whole(hour, 0, 23);
  if (hh === null) return null;
  const time = `${two(hh)}:${two(at)} UTC`;
  if (everyDay) return `every day at ${time}`;
  if (day === "*") {
    if (weekday === "1-5") return `every weekday at ${time}`;
    const wd = whole(weekday, 0, 7);
    return wd === null ? null : `every ${DAYS[wd % 7]} at ${time}`;
  }
  const dd = whole(day, 1, 31);
  if (dd === null || weekday !== "*") return null;
  return `on day ${dd} of every month at ${time}`;
}
