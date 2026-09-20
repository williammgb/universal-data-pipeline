import { useState } from "react";

import { rememberKey } from "../api/key";

/** Shown when the API refuses a request: the dashboard needs a key before it can read anything. */
export default function ApiKey() {
  const [typed, setTyped] = useState("");

  return (
    <main>
      <div className="head">
        <h1>A key is needed</h1>
        <div className="sub">the API refused this browser&rsquo;s request</div>
      </div>
      <form
        className="controls"
        onSubmit={(event) => {
          event.preventDefault();
          if (typed.trim()) rememberKey(typed.trim());
        }}
      >
        <label className="field" htmlFor="api-key">
          API key
          <input
            id="api-key"
            type="password"
            autoComplete="off"
            value={typed}
            onChange={(event) => setTyped(event.target.value)}
          />
        </label>
        <button type="submit" disabled={!typed.trim()}>
          Use this key
        </button>
      </form>
      <p className="note">
        It is kept in this browser only, and sent to this platform&rsquo;s API alone. The person who
        runs the platform has it: it is the <code>UDP_API_KEYS</code> setting.
      </p>
    </main>
  );
}
