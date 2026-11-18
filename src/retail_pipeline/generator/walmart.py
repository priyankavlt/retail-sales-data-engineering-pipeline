"""Synthetic Walmart sales data generator.

Produces files with exactly the same schema and quirks as the Kaggle
"Walmart Recruiting - Store Sales Forecasting" dataset, so the pipeline runs end to end
without a Kaggle account and the real files can be dropped in unchanged:

* ``stores.csv``   - Store, Type, Size
* ``features.csv`` - Store, Date, Temperature, Fuel_Price, MarkDown1..5, CPI, Unemployment, IsHoliday
* ``train.csv``    - Store, Dept, Date, Weekly_Sales, IsHoliday

Realistic signal is baked in (store format and size, department mix, yearly seasonality,
Thanksgiving / pre-Christmas peaks, markdown lift, macro effects, return weeks), along with
the same gaps as the real data (``NA`` markdowns before Nov 2011, occasional missing CPI).

Output is deterministic: a given (seed, store, dept, week) always yields the same value,
so generating one week at a time produces exactly the rows a full-history run would.
A small, configurable share of defective rows (duplicates, bad dates, non-numeric sales,
missing keys) is injected to exercise the data-quality layer.
"""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

N_STORES = 45
FIRST_WEEK = date(2010, 2, 5)
LAST_WEEK = date(2012, 10, 26)
MARKDOWNS_AVAILABLE_FROM = pd.Timestamp("2011-11-11")

# Holiday weeks exactly as labelled in the Kaggle dataset (week ending Friday).
HOLIDAY_WEEKS = {
    "Super Bowl": ["2010-02-12", "2011-02-11", "2012-02-10", "2013-02-08"],
    "Labor Day": ["2010-09-10", "2011-09-09", "2012-09-07", "2013-09-06"],
    "Thanksgiving": ["2010-11-26", "2011-11-25", "2012-11-23", "2013-11-29"],
    "Christmas": ["2010-12-31", "2011-12-30", "2012-12-28", "2013-12-27"],
}

# Stores that are remodelled (expanded) part-way through, to exercise SCD Type 2 in dbt.
STORE_REMODELS = {12: ("2011-06-03", 1.18), 33: ("2012-03-02", 1.25), 7: ("2012-10-26", 1.12)}

SALES_COLUMNS = ["Store", "Dept", "Date", "Weekly_Sales", "IsHoliday"]
FEATURE_COLUMNS = [
    "Store", "Date", "Temperature", "Fuel_Price",
    "MarkDown1", "MarkDown2", "MarkDown3", "MarkDown4", "MarkDown5",
    "CPI", "Unemployment", "IsHoliday",
]
STORE_COLUMNS = ["Store", "Type", "Size"]


def _mix(*keys: np.ndarray | int) -> np.ndarray:
    """Vectorised splitmix64 over a tuple of integer keys -> uint64 hash."""
    h = np.zeros(np.broadcast(*[np.asarray(k) for k in keys]).shape, dtype=np.uint64)
    with np.errstate(over="ignore"):
        for k in keys:
            h ^= np.asarray(k, dtype=np.int64).astype(np.uint64)
            h += np.uint64(0x9E3779B97F4A7C15)
            z = h
            z = (z ^ (z >> np.uint64(30))) * np.uint64(0xBF58476D1CE4E5B9)
            z = (z ^ (z >> np.uint64(27))) * np.uint64(0x94D049BB133111EB)
            h = z ^ (z >> np.uint64(31))
    return h


def _uniform(*keys) -> np.ndarray:
    return (_mix(*keys) >> np.uint64(11)).astype(np.float64) / float(1 << 53)


def _normal(*keys) -> np.ndarray:
    # Box-Muller on two decorrelated uniforms.
    u1 = np.clip(_uniform(*keys, 1), 1e-12, 1.0)
    u2 = _uniform(*keys, 2)
    return np.sqrt(-2.0 * np.log(u1)) * np.cos(2.0 * np.pi * u2)


@dataclass(frozen=True)
class GeneratorSpec:
    start: date = FIRST_WEEK
    end: date = LAST_WEEK
    n_stores: int = N_STORES
    seed: int = 42
    defect_rate: float = 0.001


def friday_weeks(start: date, end: date) -> pd.DatetimeIndex:
    weeks = pd.date_range(pd.Timestamp(start), pd.Timestamp(end), freq="W-FRI")
    if weeks.empty:
        raise ValueError(f"No Friday week-ending dates between {start} and {end}")
    return weeks


def _holiday_lookup() -> dict[pd.Timestamp, str]:
    return {pd.Timestamp(d): name for name, ds in HOLIDAY_WEEKS.items() for d in ds}


def build_stores(spec: GeneratorSpec, as_of: pd.Timestamp | None = None) -> pd.DataFrame:
    """Store master as of ``as_of`` (remodels after that date are not yet visible)."""
    as_of = as_of or pd.Timestamp(spec.end)
    ids = np.arange(1, spec.n_stores + 1)
    u = _uniform(spec.seed, 1, ids)
    store_type = np.where(u < 0.49, "A", np.where(u < 0.87, "B", "C"))
    size_u = _uniform(spec.seed, 2, ids)
    size = np.select(
        [store_type == "A", store_type == "B"],
        [140_000 + size_u * 80_000, 34_000 + size_u * 106_000],
        default=34_000 + size_u * 10_000,
    )
    for store_id, (remodel_date, factor) in STORE_REMODELS.items():
        if store_id <= spec.n_stores and as_of >= pd.Timestamp(remodel_date):
            size[store_id - 1] *= factor
    return pd.DataFrame({"Store": ids, "Type": store_type, "Size": size.round().astype(int)})


def _store_size_on(stores_final: pd.DataFrame, store: np.ndarray, week: pd.Series) -> np.ndarray:
    size = stores_final.set_index("Store")["Size"].reindex(store).to_numpy(dtype=float)
    for store_id, (remodel_date, factor) in STORE_REMODELS.items():
        before = (store == store_id) & (week.to_numpy() < np.datetime64(remodel_date))
        size[before] /= factor
    return size


def build_features(spec: GeneratorSpec, weeks: pd.DatetimeIndex) -> pd.DataFrame:
    stores = np.arange(1, spec.n_stores + 1)
    grid = pd.MultiIndex.from_product([stores, weeks], names=["Store", "Date"]).to_frame(index=False)
    s = grid["Store"].to_numpy()
    d = grid["Date"]
    day_num = ((d - pd.Timestamp(FIRST_WEEK)).dt.days).to_numpy()
    doy = d.dt.dayofyear.to_numpy()
    years = day_num / 365.25
    seed = spec.seed

    climate = 35 + 35 * _uniform(seed, 10, s)  # colder northern vs warmer southern stores
    temperature = climate + 22 * np.sin(2 * np.pi * (doy - 105) / 365.25) + 4 * _normal(seed, 11, s, day_num)
    fuel = 2.55 + 0.45 * years + 0.12 * np.sin(2 * np.pi * years) + 0.25 * _uniform(seed, 12, s) + 0.03 * _normal(seed, 13, s, day_num)
    cpi_base = np.where(_uniform(seed, 14, s) < 0.55, 210.0, 127.0) + 6 * _uniform(seed, 15, s)
    cpi = cpi_base * (1 + 0.021 * years) + 0.15 * _normal(seed, 16, s, day_num)
    unemployment = 5.0 + 7.5 * _uniform(seed, 17, s) - 0.55 * years + 0.05 * _normal(seed, 18, s, day_num)

    holiday_season = np.isin(d.dt.month.to_numpy(), [11, 12])
    markdowns = {}
    for i in range(1, 6):
        present = _uniform(seed, 20 + i, s, day_num) < (0.85 if i in (1, 5) else 0.55)
        amount = np.exp(7.5 + 0.9 * _normal(seed, 30 + i, s, day_num)) * np.where(holiday_season, 3.0, 1.0)
        values = np.where(present, amount.round(2), np.nan)
        values[d.to_numpy() < MARKDOWNS_AVAILABLE_FROM.to_datetime64()] = np.nan
        markdowns[f"MarkDown{i}"] = values

    # The real data has gaps in CPI / unemployment; recreate a few for forward-fill logic.
    cpi[_uniform(seed, 40, s, day_num) < 0.01] = np.nan
    unemployment[_uniform(seed, 41, s, day_num) < 0.01] = np.nan

    holidays = _holiday_lookup()
    return pd.DataFrame(
        {
            "Store": s,
            "Date": d.dt.strftime("%Y-%m-%d"),
            "Temperature": temperature.round(2),
            "Fuel_Price": fuel.round(3),
            **markdowns,
            "CPI": cpi.round(7),
            "Unemployment": unemployment.round(3),
            "IsHoliday": d.isin(list(holidays)).map({True: "TRUE", False: "FALSE"}),
        }
    )[FEATURE_COLUMNS]


def build_sales(spec: GeneratorSpec, weeks: pd.DatetimeIndex, features: pd.DataFrame) -> pd.DataFrame:
    seed = spec.seed
    stores_final = build_stores(spec, as_of=pd.Timestamp("2099-01-01"))
    store_type = stores_final.set_index("Store")["Type"]

    all_depts = np.arange(1, 100)
    depts = all_depts[_uniform(seed, 50, all_depts) < 0.82]
    pairs = pd.MultiIndex.from_product([stores_final["Store"], depts], names=["Store", "Dept"]).to_frame(index=False)
    keep_p = pairs["Store"].map(store_type).map({"A": 0.97, "B": 0.88, "C": 0.72}).to_numpy()
    pairs = pairs[_uniform(seed, 51, pairs["Store"], pairs["Dept"]) < keep_p]

    grid = pairs.merge(pd.DataFrame({"Date": weeks}), how="cross")
    s, dept = grid["Store"].to_numpy(), grid["Dept"].to_numpy()
    d = grid["Date"]
    day_num = ((d - pd.Timestamp(FIRST_WEEK)).dt.days).to_numpy()
    doy = d.dt.dayofyear.to_numpy()
    years = day_num / 365.25

    size = _store_size_on(stores_final, s, d)
    dept_scale = np.exp(8.6 + 1.35 * _normal(seed, 60, dept))  # a few departments dominate
    store_factor = (size / 150_000) ** 0.85 * (0.75 + 0.5 * _uniform(seed, 61, s))
    base = dept_scale * store_factor

    giftiness = _uniform(seed, 62, dept) ** 2  # toys/electronics-like departments spike hardest
    seasonal = 1 + 0.12 * _normal(seed, 63, dept).clip(-2, 2) * np.sin(2 * np.pi * (doy - 80) / 365.25)
    month, dom = d.dt.month.to_numpy(), d.dt.day.to_numpy()
    pre_christmas = (month == 12) & (dom >= 10) & (dom <= 27)
    thanksgiving = d.isin([pd.Timestamp(x) for x in HOLIDAY_WEEKS["Thanksgiving"]]).to_numpy()
    super_bowl = d.isin([pd.Timestamp(x) for x in HOLIDAY_WEEKS["Super Bowl"]]).to_numpy()
    peak = (
        1
        + pre_christmas * (0.25 + 2.2 * giftiness)
        + thanksgiving * (0.20 + 1.4 * giftiness)
        + super_bowl * 0.06
    )

    feat = features.assign(Date=pd.to_datetime(features["Date"]))
    feat = feat.set_index(["Store", "Date"])
    idx = pd.MultiIndex.from_arrays([s, d])
    md_total = feat[[f"MarkDown{i}" for i in range(1, 6)]].sum(axis=1, min_count=1).reindex(idx).fillna(0).to_numpy()
    unemp = feat["Unemployment"].reindex(idx).ffill().to_numpy()
    markdown_lift = 1 + 0.06 * np.log1p(md_total / 5_000)
    macro = 1 - 0.012 * (np.nan_to_num(unemp, nan=8.0) - 8.0)
    trend = 1 + 0.025 * years * (_uniform(seed, 64, s) - 0.3)
    noise = np.exp(0.09 * _normal(seed, 65, s, dept, day_num))

    weekly_sales = base * seasonal * peak * markdown_lift * macro * trend * noise
    # Small departments occasionally post net-negative weeks (returns exceed sales).
    is_return_week = (dept_scale * store_factor < 900) & (_uniform(seed, 66, s, dept, day_num) < 0.04)
    weekly_sales = np.where(is_return_week, -weekly_sales * 0.3, weekly_sales)

    holidays = _holiday_lookup()
    return pd.DataFrame(
        {
            "Store": s,
            "Dept": dept,
            "Date": d.dt.strftime("%Y-%m-%d"),
            "Weekly_Sales": weekly_sales.round(2),
            "IsHoliday": d.isin(list(holidays)).map({True: "TRUE", False: "FALSE"}),
        }
    )[SALES_COLUMNS]


def inject_defects(df: pd.DataFrame, rate: float, seed: int, kind: str) -> pd.DataFrame:
    """Add duplicates and malformed rows, as seen in real operational extracts."""
    if rate <= 0 or df.empty:
        return df
    row_ids = np.arange(len(df))
    out = df.astype({c: "object" for c in df.columns}).copy()
    u = _uniform(seed, 90, row_ids, ord(kind[0]))
    out.loc[u < rate / 4, "Date"] = "2011-13-45"
    if kind == "sales":
        out.loc[(u >= rate / 4) & (u < rate / 2), "Weekly_Sales"] = "N/A"
        out.loc[(u >= rate / 2) & (u < 3 * rate / 4), "Dept"] = None
    else:
        out.loc[(u >= rate / 4) & (u < rate / 2), "Fuel_Price"] = "-1"
    dupes = out[_uniform(seed, 91, row_ids, ord(kind[0])) < rate]
    return pd.concat([out, dupes], ignore_index=True)


def generate(spec: GeneratorSpec, out_dir: Path) -> dict[str, Path]:
    weeks = friday_weeks(spec.start, spec.end)
    out_dir.mkdir(parents=True, exist_ok=True)
    features = build_features(spec, weeks)
    sales = build_sales(spec, weeks, features)
    stores = build_stores(spec)
    outputs = {
        "stores": (stores, out_dir / "stores.csv"),
        "features": (inject_defects(features, spec.defect_rate, spec.seed, "features"), out_dir / "features.csv"),
        "sales": (inject_defects(sales, spec.defect_rate, spec.seed, "sales"), out_dir / "train.csv"),
    }
    for name, (frame, path) in outputs.items():
        frame.to_csv(path, index=False, na_rep="NA")
        log.info("wrote %-8s %9d rows -> %s", name, len(frame), path)
    return {name: path for name, (_, path) in outputs.items()}


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, default=Path("data/generated"))
    parser.add_argument("--start", type=date.fromisoformat, default=FIRST_WEEK)
    parser.add_argument("--end", type=date.fromisoformat, default=LAST_WEEK)
    parser.add_argument("--stores", type=int, default=N_STORES)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--defect-rate", type=float, default=0.001, help="0 disables defect injection")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    generate(GeneratorSpec(args.start, args.end, args.stores, args.seed, args.defect_rate), args.out)


if __name__ == "__main__":
    main()
