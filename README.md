# Football Analytics Data Warehouse

Course project for **Data Warehouses and Analytical Processing** (ФИНКИ) — a Spark/MLlib data warehouse pipeline built on football data, using a medallion architecture with a Kimball dimensional model in the Gold layer, MLlib models, a ClickHouse serving layer, and dashboards.

## Dataset

**Transfermarkt** — "Football Data from Transfermarkt" (Kaggle, `davidcariboo/player-scores`).

- 12 source tables: `competitions`, `games`, `clubs`, `players`, `appearances`, `player_valuations`, `club_games`, `game_events`, `game_lineups`, `transfers`, `countries`, `national_teams`
- Scale: 88,000+ games, 50,000+ players, 1,890,000+ appearances, 2,800,000+ game lineup entries, 1,100,000+ game events, 520,000+ player valuation records
- Source format: CSV → converted to Parquet in Bronze

## Architecture

### Bronze
Raw ingestion — each source CSV read into a Spark DataFrame and written straight to Parquet, one-to-one with the source tables, no transformations.

### Silver
- Null handling and type casting (`market_value_in_eur`, dates in `DD/MM/YY` format need explicit schema rather than relying on `inferSchema`)
- Joins across `games` ↔ `clubs` ↔ `competitions`, and `appearances` ↔ `players` ↔ `games`
- Deduplication where needed

### Gold — Kimball star schema
- **Dimensions**: `Dim_Player`, `Dim_Club`, `Dim_Competition`, `Dim_Country`, `Dim_Date`
- **Facts**:
  - `Fact_Appearance` — grain: one row per player per game (goals, assists, minutes played, cards)
  - `Fact_Game` — grain: one row per game (score, competition, attendance, date)
  - `Fact_PlayerValuation` — grain: one row per player per valuation date (a genuine time series)

## ML models (MLlib)

1. **Random Forest** — match outcome classification (home win / draw / away win), using squad market value at match time (joined to the nearest valuation date before the match) as a team-strength feature
2. **Linear Regression** — predict a player's market value from performance stats (goals/assists/minutes per appearance), age, position, and international caps
3. **K-Means** — cluster players by performance profile (per-90 goals, assists, cards, minutes share), with features scaled before clustering
4. *Optional*: time-series forecasting of a player's market-value trajectory from `player_valuations`

### Design notes
- The original plan used the European Soccer Database (FIFA-style skill attributes). Switched to Transfermarkt for its larger scale and because it supports a real per-match player performance fact table (`Fact_Appearance`), which the old dataset couldn't provide.
- Predicting market value from performance stats is a better regression problem than predicting a FIFA overall rating from its own sub-attributes — market value is a subjective expert assessment, not a formula of the inputs, so it won't produce a trivially perfect fit.

## Serving layer — ClickHouse

Gold tables are loaded into ClickHouse for fast OLAP querying. Key demonstration: run the same aggregate query in Spark SQL and in ClickHouse and compare latency — this shows the practical difference between a general-purpose engine and a columnar OLAP store.

## Dashboards

Apache Superset, self-hosted, connected to ClickHouse via the `clickhouse-connect` driver.

## Environment

- **Databricks Free Edition** — serverless compute, Unity Catalog Volumes for raw/bronze/silver/gold storage
- **Version control** — Databricks Git folders, linked to this GitHub repo

## Repo structure

```
/notebooks
  01_bronze_ingest.py
  02_silver_clean_join.py
  03_gold_star_schema.py
  04_ml_classification.py
  05_ml_regression.py
  06_ml_clustering.py
  07_clickhouse_export.py
/sql
  gold_ddl.sql
README.md
```

## Status

- [x] Topic and dataset finalized
- [x] Architecture and ML tasks designed
- [ ] Bronze ingestion
- [ ] Silver cleaning and joins
- [ ] Gold star schema
- [ ] ML models
- [ ] ClickHouse integration
- [ ] Dashboards
