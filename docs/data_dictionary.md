# Data dictionary

## Silver

### `silver.weekly_sales` (grain: store × department × week)
| Column | Type | Description |
|---|---|---|
| store_id | int | Store number (1–45) |
| dept_id | int | Department number (1–99) |
| week_date | date | Week ending Friday |
| weekly_sales | decimal(14,2) | Net sales for the week; negative when returns exceed sales |
| is_holiday | boolean | Week contains a Walmart event (Super Bowl, Labor Day, Thanksgiving, Christmas) |
| _ingest_date / _ingested_at / _source_file / _row_hash | | Lineage from bronze |
| _silver_updated_at | timestamp | Last time the row was inserted/updated in silver (drives dbt incremental) |

### `silver.store_features` (grain: store × week)
| Column | Type | Description |
|---|---|---|
| store_id, week_date | | Keys |
| temperature_f | double | Average regional temperature (°F) |
| fuel_price | double | Regional fuel price (USD/gal) |
| markdown_1 … markdown_5 | decimal(14,2) | Promotional markdowns; only tracked from 2011-11-11 |
| cpi | double | Consumer price index (may be NULL; forward-filled in gold) |
| unemployment_rate | double | Regional unemployment rate (%) |
| is_holiday | boolean | Holiday week flag |

### `silver.stores` (grain: store)
| Column | Type | Description |
|---|---|---|
| store_id | int | Store number |
| store_type | string | A / B / C (mapped to Supercenter / Discount Store / Neighborhood Market in gold) |
| store_size_sqft | int | Store size in square feet |

## Data-quality rules (silver)

| Dataset | Rule | Severity |
|---|---|---|
| all | `store_id` present and positive | error |
| stores | `store_type in ('A','B','C')`, `store_size_sqft > 0` | error |
| features | valid `week_date`, `fuel_price > 0`, `is_holiday` parseable | error |
| features | week ends on Friday; temperature −60…130 °F; unemployment 0…50% | warn |
| sales | `dept_id` 1–99, valid `week_date`, numeric `weekly_sales`, `is_holiday` parseable | error |
| sales | week ends on Friday; abs(weekly_sales) < 1M | warn |

## Gold

| Model | Grain | Notes |
|---|---|---|
| `fct_weekly_sales` | store × dept × week | Incremental; carries fiscal period, holiday name, forward-filled CPI/unemployment, store markdown total |
| `dim_store` | store | Current SCD2 version; format, size band, remodel flag |
| `dim_store_history` | store version | Full SCD2 history (`valid_from`, `valid_to`, `is_current`) |
| `dim_department` | department | Illustrative names and groups (the public dataset anonymises departments) |
| `dim_date` | day | Calendar + Walmart fiscal year (Feb–Jan), week-ending-Friday key, holiday week |
| `mart_*` | see README | Analytics marts |
