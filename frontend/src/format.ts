/** How values from the API are shown. Times are shown in Amsterdam time; the API, the addresses
 * and the schedules themselves stay in UTC. */

/** The one zone every moment on the dashboard is shown in, whatever the browser's own zone. */
export const TIME_ZONE = "Europe/Amsterdam";

export type CellValue = string | number | boolean | { [key: string]: unknown } | unknown[] | null;

/** A value as shown in a table: `text` on one line, `full` for when values are shown in full —
 * the same text, except a JSON object or array, which is indented over several lines there. */
export type Cell = { text: string; full: string; isNull: boolean };

export function cell(value: CellValue): Cell {
  if (value === null) return { text: "null", full: "null", isNull: true };
  if (typeof value === "object") {
    return { text: JSON.stringify(value), full: JSON.stringify(value, null, 2), isNull: false };
  }
  const text = typeof value === "string" ? value : String(value);
  return { text, full: text, isNull: false };
}

type Wall = { year: string; month: string; day: string; hour: string; minute: string; second: string };

const WALL = new Intl.DateTimeFormat("en-GB", {
  timeZone: TIME_ZONE,
  hourCycle: "h23",
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
});

/** The date and clock time in TIME_ZONE at a moment, read by part name, never by punctuation. */
function wall(when: Date): Wall {
  const parts: Record<string, string> = {};
  for (const part of WALL.formatToParts(when)) parts[part.type] = part.value;
  const { year = "", month = "", day = "", hour = "", minute = "", second = "" } = parts;
  return { year: year.padStart(4, "0"), month, day, hour, minute, second };
}

/** How many minutes TIME_ZONE is ahead of UTC at a moment: 60 in winter, 120 in summer. */
function offsetMinutes(when: Date): number {
  const shown = wall(when);
  const asUtc = new Date(0);
  asUtc.setUTCFullYear(Number(shown.year), Number(shown.month) - 1, Number(shown.day));
  asUtc.setUTCHours(Number(shown.hour), Number(shown.minute), Number(shown.second), 0);
  const whole = when.getTime() - when.getUTCMilliseconds();
  return Math.round((asUtc.getTime() - whole) / 60000);
}

function readable(value: string): Date | null {
  const when = new Date(value);
  return Number.isNaN(when.getTime()) ? null : when;
}

/** An API timestamp as "YYYY-MM-DD HH:MM:SS" in Amsterdam time; unreadable input is passed
 * through. */
export function moment(value: string | null): string {
  if (value === null) return "—";
  const when = readable(value);
  if (when === null) return value;
  const shown = wall(when);
  return `${shown.year}-${shown.month}-${shown.day} ${shown.hour}:${shown.minute}:${shown.second}`;
}

/** An API moment as the Amsterdam "YYYY-MM-DDTHH:MM" a datetime-local box takes; "" for empty or
 * unreadable text. */
export function toInput(instant: string): string {
  const when = instant ? readable(instant) : null;
  if (when === null) return "";
  const shown = wall(when);
  return `${shown.year}-${shown.month}-${shown.day}T${shown.hour}:${shown.minute}`;
}

/** An Amsterdam time typed in a datetime-local box as the UTC moment "YYYY-MM-DDTHH:MM:00Z";
 * "" for empty or unreadable text. A time in the hour skipped in March is read as one hour later,
 * and one in the hour repeated in October as the second, winter-time one. */
export function fromInput(typed: string): string {
  const match = /^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2})(?::\d{2}(?:\.\d+)?)?$/.exec(typed);
  if (!match) return "";
  const guess = readable(`${match[1]}:00Z`);
  if (guess === null) return "";
  const first = offsetMinutes(guess);
  let result = guess.getTime() - first * 60000;
  const second = offsetMinutes(new Date(result));
  if (second !== first) result = guess.getTime() - second * 60000;
  return `${new Date(result).toISOString().slice(0, 16)}:00Z`;
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
  json: "JSON",
  rest_api: "API",
};

/** What a source is, in the words a person uses for it. */
export function connectorName(type: string): string {
  return CONNECTOR_NAMES[type] ?? type;
}

/** A value the API spells in lower case, as a label starts: "full" reads "Full",
 * "every_step" reads "Every step". */
export function sentence(value: string): string {
  const words = value.replaceAll("_", " ");
  return words.charAt(0).toUpperCase() + words.slice(1);
}

/** A dataset's name as a title: "order_lines" reads "Order lines". The name itself is still
 * shown beside it wherever it is needed exactly. */
export function datasetTitle(name: string): string {
  return sentence(name);
}

const TYPE_NAMES: [RegExp, string][] = [
  [/^(bigint|integer|smallint)$/, "Integer"],
  [/^(double precision|real|float)$/, "Float"],
  [/^(text|character varying.*)$/, "Text"],
  [/^boolean$/, "Boolean"],
  [/^date$/, "Date"],
  [/^timestamp.*$/, "Timestamp"],
  [/^jsonb?$/, "JSON"],
];

/** A column's type in words: a declared type or the table's own, "decimal(10,2)" and
 * "numeric(10,2)" both reading "Decimal (10, 2)". An unknown type is shown as it is. */
export function typeName(type: string | null): string {
  if (type === null) return "—";
  const exact = /^(?:decimal|numeric)\((\d+),\s*(\d+)\)$/.exec(type);
  if (exact) return `Decimal (${exact[1]}, ${exact[2]})`;
  if (type === "numeric" || type === "decimal") return "Decimal";
  const found = TYPE_NAMES.find(([pattern]) => pattern.test(type));
  return found ? found[1] : type;
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
 * so an unusual schedule is shown as it is written rather than described wrongly. The cron is
 * UTC; a clock time is given in Amsterdam time as of `now`. Where that moves the time to another
 * day, "every day" needs nothing and a single weekday names the next day, but "every weekday"
 * and "on day N of every month" cannot be said truly that way, so they keep the UTC time. */
export function describeSchedule(schedule: string, now: Date = new Date()): string | null {
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
  const minutes = hh * 60 + at + offsetMinutes(now);
  const shift = Math.floor(minutes / 1440);
  const local = minutes - shift * 1440;
  const time = `${two(Math.floor(local / 60))}:${two(local % 60)}`;
  const utc = shift === 0 ? time : `${two(hh)}:${two(at)} UTC`;
  if (everyDay) return `every day at ${time}`;
  if (day === "*") {
    if (weekday === "1-5") return `every weekday at ${utc}`;
    const wd = whole(weekday, 0, 7);
    return wd === null ? null : `every ${DAYS[(((wd + shift) % 7) + 7) % 7]} at ${time}`;
  }
  const dd = whole(day, 1, 31);
  if (dd === null || weekday !== "*") return null;
  return `on day ${dd} of every month at ${utc}`;
}
