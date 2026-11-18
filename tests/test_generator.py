from datetime import date

import pandas as pd

from retail_pipeline.generator.walmart import (
    FEATURE_COLUMNS,
    SALES_COLUMNS,
    STORE_COLUMNS,
    GeneratorSpec,
    build_stores,
    generate,
)


def _read(path):
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def test_outputs_match_kaggle_schema(tmp_path):
    paths = generate(GeneratorSpec(date(2011, 11, 4), date(2011, 11, 25), n_stores=3, defect_rate=0), tmp_path)
    assert list(_read(paths["stores"]).columns) == STORE_COLUMNS
    assert list(_read(paths["features"]).columns) == FEATURE_COLUMNS
    assert list(_read(paths["sales"]).columns) == SALES_COLUMNS
    assert paths["sales"].name == "train.csv"


def test_generation_is_deterministic_per_week(tmp_path):
    """Generating one week alone yields exactly the rows of a multi-week run."""
    full = _read(generate(GeneratorSpec(date(2012, 1, 6), date(2012, 2, 3), n_stores=4, defect_rate=0), tmp_path / "a")["sales"])
    week = _read(generate(GeneratorSpec(date(2012, 1, 20), date(2012, 1, 20), n_stores=4, defect_rate=0), tmp_path / "b")["sales"])
    expected = full[full["Date"] == "2012-01-20"].reset_index(drop=True)
    pd.testing.assert_frame_equal(week, expected)


def test_markdowns_missing_before_tracking_and_holidays_flagged(tmp_path):
    paths = generate(GeneratorSpec(date(2011, 11, 4), date(2011, 11, 25), n_stores=5, defect_rate=0), tmp_path)
    feats = _read(paths["features"])
    assert (feats.loc[feats["Date"] == "2011-11-04", "MarkDown1"] == "NA").all()
    assert (feats.loc[feats["Date"] >= "2011-11-11", "MarkDown1"] != "NA").any()
    sales = _read(paths["sales"])
    assert set(sales.loc[sales["Date"] == "2011-11-25", "IsHoliday"]) == {"TRUE"}
    assert set(sales.loc[sales["Date"] == "2011-11-18", "IsHoliday"]) == {"FALSE"}


def test_thanksgiving_outsells_normal_week(tmp_path):
    sales = pd.read_csv(generate(GeneratorSpec(date(2011, 11, 18), date(2011, 11, 25), defect_rate=0), tmp_path)["sales"])
    totals = sales.groupby("Date")["Weekly_Sales"].sum()
    assert totals["2011-11-25"] > 1.2 * totals["2011-11-18"]


def test_defects_are_injected(tmp_path):
    paths = generate(GeneratorSpec(date(2010, 2, 5), date(2010, 6, 25), n_stores=10, defect_rate=0.01), tmp_path)
    sales = _read(paths["sales"])
    assert sales.duplicated().any()
    assert (sales["Date"] == "2011-13-45").any()
    assert (sales["Weekly_Sales"] == "N/A").any()


def test_store_remodel_visible_only_after_date():
    spec = GeneratorSpec(n_stores=45)
    before = build_stores(spec, pd.Timestamp("2011-06-02")).set_index("Store")
    after = build_stores(spec, pd.Timestamp("2011-06-03")).set_index("Store")
    assert after.loc[12, "Size"] > before.loc[12, "Size"]
    assert after.loc[1, "Size"] == before.loc[1, "Size"]
