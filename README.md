# Football Analytics Data Warehouse

Course project for **Data Warehouses and Analytical Processing** (ФИНКИ) — a Spark/MLlib data warehouse pipeline built on football data, using a medallion architecture with a Kimball dimensional model in the Gold layer, three MLlib models tracked with MLflow, a ClickHouse Cloud serving layer benchmarked against Spark, and three parallel BI dashboards.

## Dataset

**Transfermarkt** — "Football Data from Transfermarkt" (Kaggle, `davidcariboo/player-scores`).

12 source tables, ingested to Bronze as-is:

| Table | Rows |
|---|---|
| `games` | 88,958 |
| `clubs` | 796 |
| `players` | 50,149 |
| `appearances` | 1,894,350 |
| `player_valuations` | 656,301 |
| `club_games` | 177,916 |
| `game_events` | 1,274,469 |
| `game_lineups` | 3,179,016 |
| `transfers` | 175,165 |
| `competitions` | 65 |
| `countries` | 124 |
| `national_teams` | 124 |

Source format: CSV → Delta tables.

## Architecture

Bronze, Silver, and Gold are each a dedicated Unity Catalog schema of managed **Delta tables** (`workspace.bronze`, `workspace.silver`, `workspace.gold`) — not Parquet files in a Volume. This was a deliberate upgrade over an earlier version of the pipeline: proper table registration gives real Unity Catalog lineage graphs between notebooks, `DESCRIBE HISTORY` time-travel on every table, and per-table/per-notebook documentation via `COMMENT ON TABLE`.

### Bronze — `workspace.bronze`
Raw ingestion, one table per source CSV, `inferSchema` with one manual override: `games.aggregate` (two-legged tie scorelines like `"3:1"`) gets forced to `StringType`, because Spark's schema inference otherwise misreads the colon as a time value. No other transformations. Each table is written with `df.write.format("delta").mode("overwrite").saveAsTable(...)` and documented with `COMMENT ON TABLE`.

### Silver — `workspace.silver`
- Deduplication (full-row for passthrough tables, key-based for `players`/`games`)
- Type casts and null handling — dates arrive in ISO format from Bronze already correctly typed; the Silver cast is a formality, not a fix for a broken format
- Referential integrity, duplicate, and missingness checks (notebook `03_eda.py`), plus a `transfer_type` classification heuristic (paid / internal squad move / free-or-loan) later reused in the Gold `fact_transfer`

### Gold — `workspace.gold` — Kimball star schema
**Dimensions** (surrogate keys via `row_number()`, each with an explicit `-1` "Unknown member" row for unmatched lookups):
- `dim_date` (7,564 rows)
- `dim_player` (50,150 rows)
- `dim_club` (18,537 rows) — includes ~17,740 clubs inferred via the late-arriving-dimension technique (referenced by `transfers`/`club_games` but never present in the `clubs` source table)
- `dim_competition` (66 rows)

**Facts:**
- `fact_game` (88,958 rows) — grain: one row per game; role-playing dimension for home/away club
- `fact_appearance` (1,894,350 rows) — grain: one row per player per game
- `fact_player_valuation` (656,301 rows) — grain: one row per player per valuation date; periodic snapshot fact, semi-additive
- `fact_transfer` (175,165 rows) — grain: one row per transfer record; role-playing dimension for from/to club, includes derived `transfer_type`

All facts resolve unmatched dimension lookups to `-1`, and fact row counts are checked to match their Silver source counts exactly.

## ML models (MLlib), tracked with MLflow

All three problems are genuinely leakage-safe: temporal (never random) train/val/test splits, rolling-window features with explicit boundaries, and as-of joins for point-in-time-correct feature values.

**1. Classification — match outcome (home win / draw / away win), Random Forest**
Features include leakage-safe rolling 5-game team form (`rowsBetween(-5, -1)`) and an as-of join to squad market value at match time. Minority-class (draw) oversampling is applied to training data only; **macro F1** was chosen as the selection metric after an earlier weighted-F1 model was found to completely ignore the draw class. Grid search and the final model are logged to MLflow.
- Test accuracy: **0.509** vs. majority-class baseline **0.465**
- Test macro F1: **0.479** (home 0.607 / away 0.533 / draw 0.297), weighted F1: 0.513
- Walk-forward validation across 2021/2022/2023 folds confirmed stable (macro F1 ≈ 0.476–0.486)

**2. Regression — transfer fee prediction, Linear Regression with regularization**
Predicts a player's **transfer fee** (not market value) from performance stats, age, position, and an as-of club-value feature. Log-transformed target, `age_squared` term (to capture the real peak-and-decline shape), `avg_minutes_per_appearance` (replacing a near-duplicate, highly collinear feature), median imputation for nulls, and rare-league bucketing (train-only counts) before one-hot encoding. Regularization grid tracked in MLflow.
- Test R²: **0.760**, RMSE (log space): **0.812**
- Test MAE: **€1,752,388** vs. baseline **€4,257,375**; median absolute percentage error: 42.8%
- Residual diagnostics: mean −0.091, std 0.807, no leftover structure against age or fitted value

**3. Clustering — player playing style, K-Means**
Players aggregated to one row (900-minute-per-season floor, chosen from threshold testing), 4 scaled per-90 features (goals, assists, cards, minutes share); `red_per90` dropped as degenerate (73.9% exactly zero). k selected via elbow + silhouette across k=2–10 (each k tracked in MLflow); **k=4** chosen as primary and validated against real position data — goalkeepers cluster together with 99.6% purity, while midfield/attack roles split meaningfully across the remaining clusters.

**MLflow tracking:** all three experiments (grid search + final model, or per-k runs for clustering) log params, metrics, and the fitted Spark model via `mlflow.spark.log_model`. Models are tracked but **not registered** in the Unity Catalog Model Registry for the classification and inference-pipeline models — Free Edition serverless compute's registry requirements (mandatory model signature, JSON-serializable input example) are incompatible with `DenseVector`-based SparkML pipelines in a way that could not be resolved after several genuinely distinct fix attempts, so registration was deliberately dropped in favor of full experiment tracking.

## Serving layer — ClickHouse Cloud

Gold tables are loaded directly from Spark into ClickHouse Cloud — `df.toPandas()` + `client.insert_df(...)` via the `clickhouse_connect` Python client, no intermediate file export. Credentials are read from a Databricks secret scope, never hardcoded, since the notebooks are tracked in a public GitHub repo through Databricks Git folders. 9 tables are loaded (the 4 dimensions, 4 facts, plus `player_style_clusters`), each with a `MergeTree` engine and an `ORDER BY` chosen to match its expected query pattern (e.g. `fact_appearance` ordered by `(competition_key, date_key, player_key)`).

**Benchmarks** (Spark SQL vs. ClickHouse, same query, 5 runs each, steady-state = median of runs 2–5, correctness-checked row-for-row before trusting any timing):

| Benchmark | Spark (steady) | ClickHouse (steady) | Speedup |
|---|---|---|---|
| Broad aggregation (goals/assists by competition × year) | 0.799s | 0.265s | ≈3.0x |
| Selective point lookup (single player's valuation history) | 0.589s | 0.102s | ≈5.8x |

ClickHouse's timings include a real network round-trip to ClickHouse Cloud that Spark's local-table timings don't pay, so if anything its actual advantage is understated here. The larger margin on the point lookup reflects the `ORDER BY` design: `fact_player_valuation` is physically sorted by `(player_key, date_key)`, letting ClickHouse use its sparse index instead of a scan.

## Dashboards

Three parallel dashboards, sharing six core visualizations (for direct cross-tool comparability) plus one platform-unique tile each:

- **Databricks AI/BI** — native, backed directly by the Gold Delta tables; unique tile: an ML metrics summary
- **ClickHouse Cloud native dashboards** (SQL Console) — unique tile: a live parameterized query
- **Apache Superset** — self-hosted via Docker Compose, connected to the same ClickHouse Cloud instance through a container-installed `clickhouse-connect` driver

## Environment

- **Databricks Free Edition** — serverless compute; Bronze/Silver/Gold as managed Delta tables under dedicated Unity Catalog schemas (not Volumes)
- **MLflow** — experiment tracking (Databricks-hosted)
- **ClickHouse Cloud** — serving layer
- **Apache Superset** — self-hosted via Docker Compose
- **Version control** — Databricks Git folders, linked to this GitHub repo

## Repo structure

```
/notebooks
  01_bronze_ingest.py
  02_silver_clean.py
  03_eda.py
  04_gold_star_schema.py
  05_ml_classification_features.py
  06_ml_classification_model.py
  07_ml_regression_features.py
  08_ml_regression_model.py
  09_ml_clustering_features.py
  10_ml_clustering_model.py
  11_clickhouse_export.py
  12_spark_clickhouse_benchmark.py
README.md
```
