import { Component, type ReactNode, useEffect, useState } from "react";
import { Link, NavLink, Route, Routes } from "react-router";

import { keyIsNeeded, whenKeyIsNeeded } from "./api/key";
import ApiKey from "./pages/ApiKey";
import Dataset from "./pages/Dataset";
import Datasets from "./pages/Datasets";
import Guide from "./pages/Guide";
import Pipeline from "./pages/Pipeline";
import Run from "./pages/Run";
import Runs from "./pages/Runs";
import Settings from "./pages/Settings";
import { applyLongValues } from "./settings";

class ErrorBoundary extends Component<{ children: ReactNode }, { problem: Error | null }> {
  state = { problem: null as Error | null };

  static getDerivedStateFromError(problem: Error) {
    return { problem };
  }

  render() {
    if (this.state.problem) {
      return (
        <main>
          <div className="head">
            <h1>The page stopped</h1>
          </div>
          <p className="note">{this.state.problem.message}</p>
        </main>
      );
    }
    return this.props.children;
  }
}

function NotFound() {
  return (
    <main>
      <div className="head">
        <h1>No such page</h1>
      </div>
      <p className="note">
        <Link to="/">Back to the datasets</Link>
      </p>
    </main>
  );
}

/** The pages in the side bar, each with its line icon. */
const PAGES: { to: string; label: string; icon: ReactNode }[] = [
  {
    to: "/",
    label: "Datasets",
    icon: (
      <>
        <rect x="3" y="4" width="18" height="16" rx="2" />
        <path d="M3 10h18M9 10v10" />
      </>
    ),
  },
  { to: "/runs", label: "Runs", icon: <path d="M3 12h4l3-7 4 14 3-7h4" /> },
  {
    to: "/pipeline",
    label: "Pipeline",
    icon: (
      <>
        <circle cx="6" cy="6" r="2.5" />
        <circle cx="18" cy="18" r="2.5" />
        <path d="M6 8.5v4a3 3 0 0 0 3 3h6.5" />
      </>
    ),
  },
  {
    to: "/settings",
    label: "Settings",
    icon: (
      <>
        <circle cx="12" cy="12" r="3" />
        <path d="M12 3v3M12 18v3M3 12h3M18 12h3M5.6 5.6l2.1 2.1M16.3 16.3l2.1 2.1M5.6 18.4l2.1-2.1M16.3 7.7l2.1-2.1" />
      </>
    ),
  },
  {
    to: "/guide",
    label: "Guide",
    icon: <path d="M4 21V5a2 2 0 0 1 2-2h13v16H6a2 2 0 0 0-2 2z" />,
  },
];

export default function App() {
  const [needsKey, setNeedsKey] = useState(keyIsNeeded);
  useEffect(() => whenKeyIsNeeded(setNeedsKey), []);
  useEffect(() => applyLongValues(), []);

  return (
    <div className="shell">
      <header className="bar">
        <Link className="name" to="/">
          <svg width="28" height="28" viewBox="0 0 28 28" aria-hidden="true">
            <rect width="28" height="28" rx="7" fill="var(--accent)" />
            <path
              d="M8 10h12M8 14h8M8 18h12"
              stroke="var(--on-accent)"
              strokeWidth="2"
              strokeLinecap="round"
            />
          </svg>
          UDP Console
        </Link>
        <nav aria-label="Main">
          {PAGES.map((page) => (
            <NavLink
              key={page.to}
              to={page.to}
              end={page.to === "/"}
              className={({ isActive }) => (isActive ? "current" : "")}
            >
              <svg
                width="18"
                height="18"
                viewBox="0 0 24 24"
                fill="none"
                stroke="currentColor"
                strokeWidth="1.8"
                aria-hidden="true"
              >
                {page.icon}
              </svg>
              {page.label}
            </NavLink>
          ))}
        </nav>
        <div className="clock">Times in Amsterdam</div>
      </header>
      <div className="page">
        <ErrorBoundary>
        {/* The guide reads nothing from the API, so it stays reachable when the key prompt has
            taken over every other page: it is the one thing worth reading before you have a key. */}
        <Routes>
          <Route path="/guide" element={<Guide />} />
          {needsKey ? (
            <Route path="*" element={<ApiKey />} />
          ) : (
            <>
              <Route path="/" element={<Datasets />} />
              <Route path="/datasets/:source/:dataset" element={<Dataset />} />
              <Route path="/runs" element={<Runs />} />
              <Route path="/runs/:runId" element={<Run />} />
              <Route path="/pipeline" element={<Pipeline />} />
              <Route path="/settings" element={<Settings />} />
              <Route path="*" element={<NotFound />} />
            </>
          )}
        </Routes>
        </ErrorBoundary>
      </div>
    </div>
  );
}
