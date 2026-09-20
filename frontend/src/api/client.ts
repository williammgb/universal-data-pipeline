import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import { apiKey, keyWasRefused } from "./key";
import type { components } from "./schema";

export type SourceItem = components["schemas"]["SourceItem"];
export type DatasetItem = components["schemas"]["DatasetItem"];
export type DatasetDetail = components["schemas"]["DatasetDetail"];
export type RowsPage = components["schemas"]["RowsPage"];
export type QualityReport = components["schemas"]["QualityReport"];
export type RunsPage = components["schemas"]["RunsPage"];
export type RunItem = components["schemas"]["RunItem"];
export type RunDetail = components["schemas"]["RunDetail"];
export type RunAccepted = components["schemas"]["RunAccepted"];
export type RunStatus = RunItem["status"];
export type RunTrigger = RunItem["trigger"];

/** What the API said went wrong, so a page can show it instead of a blank screen. */
export class ApiError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
    this.name = "ApiError";
  }
}

/** One readable line for what the API refused. A refused request carries a list of problems
 * — where it was and what was wrong — rather than a sentence, so those are spelled out. */
function readable(detail: unknown): string {
  if (typeof detail === "string") return detail;
  if (Array.isArray(detail)) {
    const problems = detail.map((item) => {
      if (!item || typeof item !== "object") return String(item);
      const { loc, msg } = item as { loc?: unknown; msg?: unknown };
      const where = Array.isArray(loc) ? loc.filter((part) => part !== "path").join(" ") : "";
      const said = typeof msg === "string" ? msg : JSON.stringify(msg);
      return where ? `${where}: ${said}` : said;
    });
    return problems.join("; ");
  }
  return JSON.stringify(detail);
}

/** Every request carries the stored key, if there is one; the API ignores it when it needs none. */
function headers(extra: Record<string, string> = {}): Record<string, string> {
  const key = apiKey();
  return {
    Accept: "application/json",
    ...extra,
    ...(key ? { "X-API-Key": key } : {}),
  };
}

async function failure(response: Response): Promise<ApiError> {
  if (response.status === 401) keyWasRefused();
  let detail = `${response.status} ${response.statusText}`;
  try {
    const body: unknown = await response.json();
    if (body && typeof body === "object" && "detail" in body) {
      detail = readable((body as { detail: unknown }).detail);
    }
  } catch {
    // A response that is not JSON leaves the status line as the message.
  }
  return new ApiError(response.status, detail);
}

export async function getJson<T>(path: string, params?: URLSearchParams): Promise<T> {
  const query = params && [...params].length > 0 ? `?${params}` : "";
  const response = await fetch(`/api${path}${query}`, { headers: headers() });
  if (!response.ok) throw await failure(response);
  return (await response.json()) as T;
}

export async function postJson<T>(path: string, body: unknown): Promise<T> {
  const response = await fetch(`/api${path}`, {
    method: "POST",
    headers: headers({ "Content-Type": "application/json" }),
    body: JSON.stringify(body),
  });
  if (!response.ok) throw await failure(response);
  return (await response.json()) as T;
}

const REFRESH_MS = 5000;

export function useDatasets(q: string, source: string) {
  const params = new URLSearchParams();
  if (q) params.set("q", q);
  if (source) params.set("source", source);
  return useQuery({
    queryKey: ["datasets", q, source],
    queryFn: () => getJson<DatasetItem[]>("/datasets", params),
  });
}

export function useSources() {
  return useQuery({
    queryKey: ["sources"],
    queryFn: () => getJson<SourceItem[]>("/sources"),
  });
}

export function useDataset(source: string, dataset: string) {
  return useQuery({
    queryKey: ["dataset", source, dataset],
    queryFn: () => getJson<DatasetDetail>(`/datasets/${source}/${dataset}`),
  });
}

export const PREVIEW_ROWS = 50;

export function useRows(source: string, dataset: string, offset: number) {
  const params = new URLSearchParams({
    limit: String(PREVIEW_ROWS),
    offset: String(offset),
  });
  return useQuery({
    queryKey: ["rows", source, dataset, offset],
    queryFn: () => getJson<RowsPage>(`/datasets/${source}/${dataset}/rows`, params),
  });
}

export function useQuality(source: string, dataset: string) {
  return useQuery({
    queryKey: ["quality", source, dataset],
    queryFn: () => getJson<QualityReport>(`/datasets/${source}/${dataset}/quality`),
  });
}

export function useRuns(params: URLSearchParams) {
  return useQuery({
    queryKey: ["runs", params.toString()],
    queryFn: () => getJson<RunsPage>("/runs", params),
    refetchInterval: REFRESH_MS,
  });
}

export function useRun(runId: string) {
  return useQuery({
    queryKey: ["run", runId],
    queryFn: () => getJson<RunDetail>(`/runs/${runId}`),
    refetchInterval: REFRESH_MS,
  });
}

export function useStartRun(source: string, dataset: string) {
  const queries = useQueryClient();
  return useMutation({
    mutationFn: () => postJson<RunAccepted>("/runs", { source, dataset }),
    onSuccess: () => {
      void queries.invalidateQueries({ queryKey: ["runs"] });
      void queries.invalidateQueries({
        queryKey: ["dataset", source, dataset],
      });
      // The datasets list shows each dataset's last run, so it is stale from now too.
      void queries.invalidateQueries({ queryKey: ["datasets"] });
    },
  });
}
