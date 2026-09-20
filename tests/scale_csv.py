"""Write the source the memory measurement loads: one CSV of however many rows are asked for.

Usage: python scale_csv.py <folder> <rows>
Writes <folder>/scale_csv/source.yaml and <folder>/scale_csv/events.csv, then prints the size.
"""

import sys
from pathlib import Path

SOURCE = """connection:
  type: csv
datasets:
  - name: events
    path: events.csv
    load_mode: full
    columns:
      event_id: integer
      happened_at: timestamp
      city: text
      amount: decimal(12,2)
      is_open: boolean
"""

CITIES = ("Delft", "Rotterdam", "Utrecht", "Groningen", "Eindhoven")
ROWS_PER_WRITE = 100_000


def write_source(folder: Path, rows: int) -> Path:
    source = folder / "scale_csv"
    source.mkdir(parents=True, exist_ok=True)
    (source / "source.yaml").write_text(SOURCE, encoding="utf-8")
    events = source / "events.csv"
    with events.open("w", encoding="utf-8", newline="\n") as file:
        file.write("Event ID,Happened At,City,Amount,Is Open\n")
        lines: list[str] = []
        for row in range(rows):
            day = 1 + row % 28
            lines.append(
                f"{row},2026-01-{day:02d}T0{row % 10}:00:00,{CITIES[row % len(CITIES)]},"
                f"{row % 100_000}.{row % 100:02d},{'true' if row % 2 else 'false'}\n"
            )
            if len(lines) == ROWS_PER_WRITE:
                file.writelines(lines)
                lines = []
        file.writelines(lines)
    return events


if __name__ == "__main__":
    folder = Path(sys.argv[1])
    rows = int(sys.argv[2])
    events = write_source(folder, rows)
    print(f"{events}: {events.stat().st_size / 1024 / 1024:.0f} MiB, {rows} rows")
