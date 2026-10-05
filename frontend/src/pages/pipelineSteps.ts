// What each transformation type and each constraint takes, as the builder's forms offer it. The
// names and choices are the engine's own (src/udp/transformations, src/udp/config/constraints.py);
// the API checks every value again when the pipeline is saved.

export type Settings = Record<string, unknown>;

export type FieldKind =
  | "column" // one of the dataset's columns
  | "columns" // some of them
  | "choice"
  | "number"
  | "text"
  | "value" // a number when it reads as one, otherwise text
  | "values" // a list of those, separated by commas
  | "flag"
  | "mapping"; // value=replacement, one per line

export interface Field {
  name: string;
  label: string;
  kind: FieldKind;
  choices?: readonly string[];
  /** Shown only when this holds; a field that is hidden is left out of what is saved. */
  when?: (settings: Settings) => boolean;
  /** A `columns` field left empty is left out, which the engine reads as every column. */
  optional?: boolean;
}

export interface Kind {
  label: string;
  fields: Field[];
  /** A new block's settings; `column` fields start on the dataset's first column. */
  start: Settings;
}

export const DECLARED_TYPES = [
  "text",
  "integer",
  "float",
  "boolean",
  "date",
  "timestamp",
  "json",
] as const;

export const STEP_KINDS: Record<string, Kind> = {
  normalize_column_names: { label: "Normalize column names", fields: [], start: {} },
  normalize_values: {
    label: "Normalize values",
    fields: [
      { name: "columns", label: "Columns", kind: "columns" },
      { name: "trim", label: "Trim spaces", kind: "flag" },
      { name: "case", label: "Case", kind: "choice", choices: ["", "lower", "upper", "title"] },
      { name: "mapping", label: "Replace (from=to)", kind: "mapping" },
    ],
    start: { columns: [], trim: true },
  },
  convert_type: {
    label: "Convert type",
    fields: [
      { name: "column", label: "Column", kind: "column" },
      { name: "to", label: "To", kind: "choice", choices: DECLARED_TYPES },
    ],
    start: { column: "", to: "text" },
  },
  fill_missing: {
    label: "Fill missing values",
    fields: [
      { name: "columns", label: "Columns", kind: "columns" },
      {
        name: "method",
        label: "Method",
        kind: "choice",
        choices: ["mean", "median", "mode", "value"],
      },
      {
        name: "value",
        label: "Value",
        kind: "value",
        when: (settings) => settings["method"] === "value",
      },
    ],
    start: { columns: [], method: "median" },
  },
  drop_missing: {
    label: "Drop rows with missing values",
    fields: [{ name: "columns", label: "Columns", kind: "columns", optional: true }],
    start: {},
  },
  outliers: {
    label: "Outliers",
    fields: [
      { name: "column", label: "Column", kind: "column" },
      { name: "action", label: "Action", kind: "choice", choices: ["flag", "cap", "remove"] },
      { name: "lower_percentile", label: "Lower percentile", kind: "number" },
      { name: "upper_percentile", label: "Upper percentile", kind: "number" },
    ],
    start: { column: "", action: "flag", lower_percentile: 1, upper_percentile: 99 },
  },
  validate: {
    label: "Validate",
    fields: [
      { name: "on_invalid", label: "Invalid rows", kind: "choice", choices: ["keep", "drop"] },
      { name: "stop_on_critical", label: "Stop on a critical failure", kind: "flag" },
    ],
    start: { on_invalid: "keep", stop_on_critical: true },
  },
  python: {
    label: "Python script",
    fields: [
      { name: "script", label: "Script", kind: "text" },
      { name: "timeout", label: "Timeout, seconds", kind: "number" },
    ],
    start: { script: "scripts/custom/" },
  },
};

export const CONSTRAINT_KINDS: Record<string, Kind> = {
  not_null: {
    label: "Not null",
    fields: [{ name: "column", label: "Column", kind: "column" }],
    start: { column: "" },
  },
  unique: {
    label: "Unique",
    fields: [{ name: "columns", label: "Columns", kind: "columns" }],
    start: { columns: [] },
  },
  min: {
    label: "Minimum",
    fields: [
      { name: "column", label: "Column", kind: "column" },
      { name: "value", label: "Minimum", kind: "value" },
    ],
    start: { column: "", value: 0 },
  },
  max: {
    label: "Maximum",
    fields: [
      { name: "column", label: "Column", kind: "column" },
      { name: "value", label: "Maximum", kind: "value" },
    ],
    start: { column: "", value: 0 },
  },
  allowed_values: {
    label: "Allowed values",
    fields: [
      { name: "column", label: "Column", kind: "column" },
      { name: "values", label: "Values", kind: "values" },
    ],
    start: { column: "", values: [] },
  },
  pattern: {
    label: "Pattern",
    fields: [
      { name: "column", label: "Column", kind: "column" },
      { name: "pattern", label: "Pattern", kind: "text" },
    ],
    start: { column: "", pattern: "" },
  },
  datatype: {
    label: "Type",
    fields: [
      { name: "column", label: "Column", kind: "column" },
      { name: "type", label: "Type", kind: "choice", choices: DECLARED_TYPES },
    ],
    start: { column: "", type: "text" },
  },
};

/** A new step or constraint of this kind, its column fields on the dataset's first column. */
export function started(
  kinds: Record<string, Kind>,
  tag: "type" | "constraint",
  kind: string,
  columns: string[],
): Settings {
  const spec = kinds[kind]!;
  const settings: Settings = { [tag]: kind, ...spec.start };
  for (const field of spec.fields) {
    if (field.kind === "column") settings[field.name] = columns[0] ?? "";
  }
  return settings;
}

/** Text typed into a value field: a number when it reads as one, otherwise the text itself. */
export function typedValue(text: string): string | number {
  const trimmed = text.trim();
  if (trimmed !== "" && Number.isFinite(Number(trimmed))) return Number(trimmed);
  return text;
}

/** `a=b` lines as a mapping; a line without `=` is not a pair yet and is left out. */
export function mappingFrom(text: string): Record<string, string> {
  const mapping: Record<string, string> = {};
  for (const line of text.split("\n")) {
    const at = line.indexOf("=");
    if (at > 0) mapping[line.slice(0, at)] = line.slice(at + 1);
  }
  return mapping;
}

export function mappingText(mapping: unknown): string {
  if (!mapping || typeof mapping !== "object") return "";
  return Object.entries(mapping as Record<string, unknown>)
    .map(([from, to]) => `${from}=${String(to)}`)
    .join("\n");
}

/** One problem the API found in a draft: where — `steps` or `constraints`, a position counted
 * from 1, and the field — and what is wrong. */
export interface DraftProblem {
  part: "steps" | "constraints" | "name";
  position: number;
  field: string;
  message: string;
}

/** The problems in a refused save, or none when the refusal was not a list of them. */
export function draftProblems(detail: unknown): DraftProblem[] {
  if (!Array.isArray(detail)) return [];
  return detail.flatMap((item): DraftProblem[] => {
    if (!item || typeof item !== "object") return [];
    const { loc, msg } = item as { loc?: unknown; msg?: unknown };
    if (!Array.isArray(loc) || typeof msg !== "string") return [];
    const [part, position, ...field] = loc as unknown[];
    if (part === "name") return [{ part: "name", position: 0, field: "name", message: msg }];
    if ((part !== "steps" && part !== "constraints") || typeof position !== "number") return [];
    return [{ part, position, field: field.map(String).join("."), message: msg }];
  });
}

/** Two drafts are the same when they hold the same values, whatever order the keys came in. */
export function sameDraft(one: unknown, other: unknown): boolean {
  return canonical(one) === canonical(other);
}

function canonical(value: unknown): string {
  return JSON.stringify(value, (_, inner: unknown) =>
    inner && typeof inner === "object" && !Array.isArray(inner)
      ? Object.fromEntries(
          Object.entries(inner as Record<string, unknown>).sort(([a], [b]) => (a < b ? -1 : 1)),
        )
      : inner,
  );
}
