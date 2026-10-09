import type { ReactNode } from "react";
import { Link } from "react-router";

import { TAB_NAMES, TABS } from "./Dataset";

/** One section of the guide. `id` is both the heading's id and what the contents links to. */
type Section = { id: string; title: string; body: ReactNode };

/** What each tab of a dataset is for, keyed by the same tab names the dataset page renders. */
const WHAT_A_TAB_SHOWS: Record<(typeof TABS)[number], ReactNode> = {
  schema: (
    <>
      Every column and its type as the table has it now, plus the facts about the dataset — its
      table name, how it is loaded, its schedule, its primary key — and the history of column
      changes. Come here to find out what you can select, and to see whether a column appeared or
      changed type recently.
    </>
  ),
  profile: (
    <>
      What the data in each column actually looks like: how many values are missing, how many are
      distinct, the smallest and largest, a twenty-bar chart of the spread, the most and least
      common values, and the shape most values share. Come here before you trust a column — an
      unexpected pile of missing values, or one value covering 90% of rows, is usually the story.
    </>
  ),
  preview: (
    <>
      The first rows of the table, a page at a time, with empty values marked so a real blank cannot
      be mistaken for a missing one. Come here to see the data with your own eyes rather than in a
      summary.
    </>
  ),
  quality: (
    <>
      The result of every check on the last run: what was checked, whether it passed, and how many
      rows failed it. Come here when a run is marked as having quarantined rows, to find out which
      rule threw them out.
    </>
  ),
  runs: (
    <>
      Every copy the platform made of this one dataset, newest first, with rows read, rows written
      and rows quarantined. Come here to see whether the table is being filled as often as you think
      it is.
    </>
  ),
  config: (
    <>
      The dataset&rsquo;s settings — schedule, loading mode, watermark, primary key, column types,
      checks, quarantine threshold — as its file has them, and yours on top where you changed one.
      Come here to change how a dataset behaves without editing the file; the tab marks every
      setting you have edited and offers a way back to the file&rsquo;s own value.
    </>
  ),
  lineage: (
    <>
      Where the data came from and what was done to it, in order: the source file or table, RAW,
      each step of the pipeline run, and the CLEAN table with the run that made it. Click a step to
      see its transformation, column, method, rows and values changed; pick an older run to see its
      chain. Come here to trace a value in CLEAN back to the source it was read from.
    </>
  ),
};

const SECTIONS: Section[] = [
  {
    id: "what-this-does",
    title: "What this platform does",
    body: (
      <>
        <p>
          It copies data you already have — files, databases, web APIs — into one PostgreSQL
          database, on a schedule, and keeps a record of every copy it made. You point it at
          something once by writing a small file; after that it reads that thing over and over,
          writes what it read into a table you can query, checks the rows against rules you chose,
          and tells you when something looks wrong.
        </p>
        <p>
          What you do here, in this dashboard, is watch it: see which tables exist, what is in
          them, when they were last filled, what failed and why, and start a fresh copy by hand
          when you do not want to wait for the schedule.
        </p>
      </>
    ),
  },
  {
    id: "sources-and-datasets",
    title: "Sources and datasets",
    body: (
      <>
        <p>
          A <b>source</b> is one place data comes from: a folder of CSV files, one database, one web
          API. A <b>dataset</b> is one thing inside that place: one file, one table, one API
          address. Each dataset becomes exactly one table in the warehouse, named{" "}
          <code>datasets.&lt;source&gt;__&lt;dataset&gt;</code> — so the <code>customers</code>{" "}
          dataset of the <code>demo_csv</code> source lands in{" "}
          <code>datasets.demo_csv__customers</code>.
        </p>
        <p>
          Use this when you are looking for your data in a SQL client: the name in the table below
          the dataset&rsquo;s title is the table to query.
        </p>
      </>
    ),
  },
  {
    id: "adding-a-source",
    title: "Adding a source",
    body: (
      <>
        <p>
          Sources are not added from this dashboard — they are files in the project, so they can be
          reviewed and kept in version control. The shortest one is a folder under{" "}
          <code>sources/</code> holding a <code>source.yaml</code>:
        </p>
        <pre>
          <code>{`connection:
  type: csv
datasets:
  - name: customers
    path: data/customers.csv
    load_mode: full`}</code>
        </pre>
        <p>
          Then run <code>./udp load my_shop</code> once. It reads the file, tidies the
          column names (<code>Customer ID</code> becomes <code>customer_id</code>), works out a type
          for every column, writes the table, and the dataset appears in the list on this
          dashboard.
        </p>
        <p className="note">
          The full detail — databases, web APIs, Excel, column types, your own Python step, and what
          to do when a run fails — is in <code>docs/adding-a-source.md</code> in the project.
        </p>
      </>
    ),
  },
  {
    id: "datasets-list",
    title: "The Datasets page",
    body: (
      <>
        <p>
          The front page lists every dataset the platform knows about, with the kind of source it
          comes from, how many rows its table holds right now, how it is loaded, when it next runs,
          and how the last run went. Read it as the one-screen answer to &ldquo;is everything
          filled and nothing broken?&rdquo; — anything red or amber is worth opening.
        </p>
        <p>Click a dataset&rsquo;s name to open it. That page has six tabs.</p>
      </>
    ),
  },
  {
    id: "dataset-tabs",
    title: "The six tabs on a dataset",
    body: (
      <dl className="guide-list">
        {TABS.map((tab) => (
          <div key={tab}>
            <dt>{TAB_NAMES[tab]}</dt>
            <dd>{WHAT_A_TAB_SHOWS[tab]}</dd>
          </div>
        ))}
      </dl>
    ),
  },
  {
    id: "runs",
    title: "The Runs page and its filters",
    body: (
      <>
        <p>
          A <b>run</b> is one attempt to copy one dataset. The Runs page lists them all, newest
          first, across every source. The filters above the list narrow it by dataset, by status
          (succeeded, failed, running) and by time, and they are kept in the page&rsquo;s address —
          so a filtered list is a link you can keep, paste into a message, or reload without
          setting the filters again.
        </p>
        <p>
          Use it after a change to a source: filter to that dataset, and the next run is at the top
          with whether it worked.
        </p>
      </>
    ),
  },
  {
    id: "run-page",
    title: "What a run page shows",
    body: (
      <p>
        Open a run to see what happened in it: which dataset it copied, what started it, when it
        began and ended, how many rows were read, written and quarantined — and, when it failed,
        the kind of failure, the message and the full trace. The trace is the thing to read first
        when a run went wrong, and the thing to paste when asking someone else about it.
      </p>
    ),
  },
  {
    id: "run-now",
    title: "Run now",
    body: (
      <p>
        The <b>Run now</b> button at the top of a dataset asks the platform to copy that dataset
        immediately, without waiting for its schedule. The run is queued and the page tells you the
        time it was asked for; watch it finish on the dataset&rsquo;s {TAB_NAMES.runs} tab. Use it
        after changing a source file or a setting, to see the effect straight away instead of
        waiting. A file that has not changed since the last run is skipped, so a run that loads no
        rows is often correct rather than broken.
      </p>
    ),
  },
  {
    id: "schedules",
    title: "What a schedule means",
    body: (
      <>
        <p>
          A dataset&rsquo;s <b>schedule</b> is when the platform copies it on its own, written as
          five cron fields in UTC — <code>*/15 * * * *</code> is every fifteen minutes,{" "}
          <code>0 6 * * *</code> is every day at 06:00. Wherever a schedule is shown, the plain
          English version is next to it, so you never have to read the cron yourself.
        </p>
        <p>
          A dataset with no schedule is only ever copied when someone presses <b>Run now</b> or
          runs the command. Every time on this dashboard is UTC, as the bar at the top says.
        </p>
      </>
    ),
  },
  {
    id: "loading-modes",
    title: "The three loading modes",
    body: (
      <dl className="guide-list">
        <div>
          <dt>full</dt>
          <dd>
            Every run throws the table away and writes everything it read. The simplest, and right
            for anything small enough to copy whole — a spreadsheet, a reference list.
          </dd>
        </div>
        <div>
          <dt>append</dt>
          <dd>
            Every run adds only the rows newer than the last one it saw, judged by a column you
            name (the <b>watermark</b>, usually a timestamp). Right for something that only ever
            gains rows, like a log.
          </dd>
        </div>
        <div>
          <dt>merge</dt>
          <dd>
            Every run reads the rows that changed and updates the matching rows in the table,
            adding the ones that are new. It needs a watermark and a primary key — the column or
            columns that say which row is which. Right for records that are edited after they are
            created, like orders or customers.
          </dd>
        </div>
      </dl>
    ),
  },
  {
    id: "quality",
    title: "Quality checks and quarantine",
    body: (
      <>
        <p>
          A <b>check</b> is one rule the data must obey, written in the source&rsquo;s file and run
          on every run: a column is never empty, a column&rsquo;s values are unique, a number
          stays within a range, a value is one of a short list, a table has at least so many rows,
          a timestamp is no older than so many hours.
        </p>
        <p>
          Each check either warns or errors. A warning is recorded and nothing else happens. An
          error is taken seriously: the failing row is put aside instead of written — that is{" "}
          <b>quarantine</b> — or, for a rule about the table as a whole, the whole run fails. A
          dataset&rsquo;s quarantine threshold is how much of that is tolerable: pass more than
          that share of rows and the run fails rather than quietly loading a fraction of the data.
        </p>
        <p>
          So a table with fewer rows than you expected, and a run marked with quarantined rows, is
          the {TAB_NAMES.quality} tab&rsquo;s business: it names the rule that threw them out.
        </p>
      </>
    ),
  },
  {
    id: "settings",
    title: "The Settings page",
    body: (
      <p>
        Settings holds the choices about this dashboard rather than about your data. Today there is
        one: whether a long value in a table is cut short with the rest available on hover, or shown
        in full and allowed to wrap. It is kept in this browser only and applies to every table of
        values. Settings that belong to a dataset — its schedule, its mode, its checks — are on that
        dataset&rsquo;s {TAB_NAMES.config} tab instead.
      </p>
    ),
  },
  {
    id: "api-key",
    title: "The API key",
    body: (
      <>
        <p>
          Every address under <code>/api</code> but the health check needs an <b>API key</b>, so
          nobody who merely reaches the platform over the network can read your data. When the API
          refuses this browser&rsquo;s request, the dashboard asks for the key and remembers it; the
          person who runs the platform has it, in the <code>UDP_API_KEYS</code> setting.
        </p>
        <p>
          The key is kept in this browser&rsquo;s own storage and sent to this platform&rsquo;s API
          alone — never to anywhere else, and never written into a log. To stop using it on a shared
          machine, clear the browser&rsquo;s site data for this address. This guide needs no key: it
          reads nothing from the API, so it is here to read before you have one.
        </p>
      </>
    ),
  },
];

export default function Guide() {
  return (
    <main>
      <div className="head">
        <h1>Guide</h1>
        <div className="sub">what this platform does, and what every page here is for</div>
      </div>
      <nav className="guide-toc" aria-label="Sections of this guide">
        <ol>
          {SECTIONS.map((section) => (
            <li key={section.id}>
              <a href={`#${section.id}`}>{section.title}</a>
            </li>
          ))}
        </ol>
      </nav>
      {SECTIONS.map((section) => (
        <section className="guide-section" key={section.id} aria-labelledby={section.id}>
          <h2 id={section.id}>{section.title}</h2>
          {section.body}
        </section>
      ))}
      <p className="note">
        Still stuck? <code>README.md</code> in the project covers starting and stopping the
        platform, and <code>docs/adding-a-source.md</code> covers every way to describe a source.{" "}
        <Link to="/">Back to the datasets</Link>.
      </p>
    </main>
  );
}
