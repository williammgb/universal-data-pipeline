import { Component, type ReactNode, useEffect, useState } from "react";
import { Link, NavLink, Route, Routes } from "react-router";

import { keyIsNeeded, whenKeyIsNeeded } from "./api/key";
import ApiKey from "./pages/ApiKey";
import Dataset from "./pages/Dataset";
import Datasets from "./pages/Datasets";
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

export default function App() {
  const [needsKey, setNeedsKey] = useState(keyIsNeeded);
  useEffect(() => whenKeyIsNeeded(setNeedsKey), []);
  useEffect(() => applyLongValues(), []);

  return (
    <>
      <header className="bar">
        <Link className="name" to="/">
          udp <span>console</span>
        </Link>
        <nav>
          <NavLink to="/" end className={({ isActive }) => (isActive ? "current" : "")}>
            Datasets
          </NavLink>
          <NavLink to="/runs" className={({ isActive }) => (isActive ? "current" : "")}>
            Runs
          </NavLink>
          <NavLink to="/settings" className={({ isActive }) => (isActive ? "current" : "")}>
            Settings
          </NavLink>
        </nav>
        <div className="clock">all times UTC</div>
      </header>
      <ErrorBoundary>
        {needsKey ? (
          <ApiKey />
        ) : (
          <Routes>
            <Route path="/" element={<Datasets />} />
            <Route path="/datasets/:source/:dataset" element={<Dataset />} />
            <Route path="/runs" element={<Runs />} />
            <Route path="/runs/:runId" element={<Run />} />
            <Route path="/settings" element={<Settings />} />
            <Route path="*" element={<NotFound />} />
          </Routes>
        )}
      </ErrorBoundary>
    </>
  );
}
