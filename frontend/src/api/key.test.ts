import { beforeEach, describe, expect, it, vi } from "vitest";

import { apiKey, keyIsNeeded, rememberKey, whenKeyIsNeeded } from "./key";
import { getJson } from "./client";

describe("the stored API key", () => {
  beforeEach(() => {
    window.localStorage.clear();
    rememberKey("");
  });

  it("is kept, read back and cleared", () => {
    rememberKey("secret");
    expect(apiKey()).toBe("secret");

    rememberKey("");
    expect(apiKey()).toBe("");
  });

  it("is sent with every request once it is stored", async () => {
    const fetching = vi.fn(async () => new Response("[]", { status: 200 }));
    vi.stubGlobal("fetch", fetching);
    rememberKey("secret");

    await getJson("/datasets");

    const [, options] = fetching.mock.calls[0] as unknown as [string, RequestInit];
    expect((options.headers as Record<string, string>)["X-API-Key"]).toBe("secret");
    vi.unstubAllGlobals();
  });

  it("is left out when there is none", async () => {
    const fetching = vi.fn(async () => new Response("[]", { status: 200 }));
    vi.stubGlobal("fetch", fetching);

    await getJson("/datasets");

    const [, options] = fetching.mock.calls[0] as unknown as [string, RequestInit];
    expect((options.headers as Record<string, string>)["X-API-Key"]).toBeUndefined();
    vi.unstubAllGlobals();
  });

  it("is cleared and asked for again when the API refuses a request", async () => {
    const seen: boolean[] = [];
    const stop = whenKeyIsNeeded((needed) => seen.push(needed));
    rememberKey("wrong");
    vi.stubGlobal(
      "fetch",
      vi.fn(
        async () =>
          new Response(JSON.stringify({ detail: "an API key is required" }), {
            status: 401,
          }),
      ),
    );

    await expect(getJson("/datasets")).rejects.toThrow("an API key is required");

    expect(apiKey()).toBe("");
    expect(keyIsNeeded()).toBe(true);
    expect(seen).toEqual([false, true]);
    rememberKey("right");
    expect(keyIsNeeded()).toBe(false);
    stop();
    vi.unstubAllGlobals();
  });
});
