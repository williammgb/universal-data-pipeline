# universal-data-pipeline

A data platform that loads CSV files, Excel workbooks, JSON files, database tables and REST APIs
into PostgreSQL, checks the data, and shows it in a dashboard. Each source is one small YAML file.

## Start it

You need Docker with Compose. On Windows or macOS, start Docker Desktop first and wait until it
says the engine is running. Then, in an empty folder:

```
mkdir sources
curl -fsSLO https://raw.githubusercontent.com/williammgb/universal-data-pipeline/main/deploy/compose.release.yaml
export POSTGRES_PASSWORD=pick-a-password
export UDP_API_KEYS=pick-a-key
docker compose -f compose.release.yaml up -d --wait
```

Open http://127.0.0.1:8000 and enter your API key. This runs the `latest` release; to pin one,
set `UDP_VERSION` before `up`, as in `export UDP_VERSION=2.0.0`.

To stop it, keeping the data: `docker compose -f compose.release.yaml down`.

## Add a source

A source is a folder in `sources/` with a `source.yaml` that says where the data is:

```
sources/
  my_shop/
    source.yaml
    data/customers.csv
```

```yaml
connection:
  type: csv
datasets:
  - name: customers
    path: data/customers.csv
    load_mode: full
```

Load it:

```
docker compose -f compose.release.yaml run --rm app load my_shop
```

The dataset now shows in the dashboard. Excel, JSON, database and REST API sources, quality
checks and schedules are in [docs/adding-a-source.md](docs/adding-a-source.md).

## More

[docs/guide.md](docs/guide.md) covers running from the source code, the demo, cleaning data with
pipelines, settings, and backups. The dashboard's **Guide** tab explains each page.

![The datasets list in the dashboard](assets/v1_datasets.png)
