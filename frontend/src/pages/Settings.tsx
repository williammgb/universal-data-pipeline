import { useState } from "react";

import { chooseLongValues, longValues, type LongValues } from "../settings";

const CHOICES: { value: LongValues; label: string; help: string }[] = [
  {
    value: "shorten",
    label: "Shorten them",
    help: "Cut at 22 characters with “…”; point at a value to read all of it.",
  },
  {
    value: "full",
    label: "Show them in full",
    help: "Long values wrap onto more lines, so rows get taller.",
  },
];

export default function Settings() {
  const [choice, setChoice] = useState<LongValues>(longValues);

  function choose(value: LongValues) {
    chooseLongValues(value);
    setChoice(value);
  }

  return (
    <main>
      <div className="head">
        <h1>Settings</h1>
      </div>
      <section className="setting" aria-labelledby="long-values">
        <h2 id="long-values">Long values in tables</h2>
        {CHOICES.map((item) => (
          <label key={item.value} htmlFor={`long-values-${item.value}`}>
            <input
              type="radio"
              name="long-values"
              id={`long-values-${item.value}`}
              value={item.value}
              checked={choice === item.value}
              onChange={() => choose(item.value)}
            />
            <span>
              {item.label}
              <small>{item.help}</small>
            </span>
          </label>
        ))}
        <p className="note">Kept in this browser, for every table of values on this dashboard.</p>
      </section>
    </main>
  );
}
