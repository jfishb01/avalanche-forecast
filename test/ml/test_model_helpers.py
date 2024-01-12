import pytest
from unittest import mock
import pandas as pd
from datetime import date

from src.ml.model_helpers import get_targets, get_features


def test_get_targets():
    db_uri = "uri"
    distributors = ["CAIC", "NWAC"]
    start_analysis_date = date(2000, 1, 1)
    end_analysis_date = date(2000, 1, 3)
    with mock.patch("pandas.read_sql") as m:
        get_targets(
            db_uri, distributors, start_analysis_date, end_analysis_date, "target"
        )
        m.assert_called_once_with(
            mock.ANY,  # Skip validating the exact SQL string
            db_uri,
            params=dict(
                distributors=distributors,
                start_analysis_date=start_analysis_date,
                end_analysis_date=end_analysis_date,
            ),
        )


@pytest.mark.parametrize(
    "desc,start_analysis_date,end_analysis_date,fill_values,query_results,expected",
    [
        (
            "All entries present results in no imputation",
            date(2000, 1, 1),
            date(2000, 1, 2),
            None,
            pd.DataFrame(
                {
                    "analysis_date": [date(2000, 1, 1), date(2000, 1, 2)],
                    "forecast_date": [date(2000, 1, 1), date(2000, 1, 2)],
                    "distributor": ["CAIC"] * 2,
                    "area_name": ["0"] * 2,
                    "area_id": ["0"] * 2,
                    "feature_1": [5.0] * 2,
                }
            ),
            pd.DataFrame(
                {
                    "analysis_date": [date(2000, 1, 1), date(2000, 1, 2)],
                    "forecast_date": [date(2000, 1, 1), date(2000, 1, 2)],
                    "distributor": ["CAIC"] * 2,
                    "area_name": ["0"] * 2,
                    "area_id": ["0"] * 2,
                    "feature_1": [5.0] * 2,
                }
            ),
        ),
        (
            "No prior entries fills the first entries of the season with defaults",
            date(2000, 1, 1),
            date(2000, 1, 2),
            None,
            pd.DataFrame(
                {
                    "analysis_date": [date(2000, 1, 2)],
                    "forecast_date": [date(2000, 1, 2)],
                    "distributor": ["CAIC"],
                    "area_name": ["0"],
                    "area_id": ["0"],
                    "feature_1": [5.0],
                }
            ),
            pd.DataFrame(
                {
                    "analysis_date": [date(2000, 1, 1), date(2000, 1, 2)],
                    "forecast_date": [date(2000, 1, 1), date(2000, 1, 2)],
                    "distributor": ["CAIC"] * 2,
                    "area_name": ["0"] * 2,
                    "area_id": ["0"] * 2,
                    "feature_1": [0.0, 5.0],
                }
            ),
        ),
        (
            "Missing entries in analysis date range are forward filled",
            date(2000, 1, 1),
            date(2000, 1, 2),
            None,
            pd.DataFrame(
                {
                    "analysis_date": [date(2000, 1, 1)],
                    "forecast_date": [date(2000, 1, 1)],
                    "distributor": ["CAIC"],
                    "area_name": ["0"],
                    "area_id": ["0"],
                    "feature_1": [5.0],
                }
            ),
            pd.DataFrame(
                {
                    "analysis_date": [date(2000, 1, 1), date(2000, 1, 2)],
                    "forecast_date": [date(2000, 1, 1), date(2000, 1, 2)],
                    "distributor": ["CAIC"] * 2,
                    "area_name": ["0"] * 2,
                    "area_id": ["0"] * 2,
                    "feature_1": [5.0] * 2,
                }
            ),
        ),
        (
            "Missing day number columns are correctly imputed",
            date(2000, 1, 1),
            date(2000, 1, 2),
            None,
            pd.DataFrame(
                {
                    "analysis_date": [date(2000, 1, 1), date(2000, 1, 2)],
                    "forecast_date": [date(2000, 1, 1), date(2000, 1, 2)],
                    "distributor": ["CAIC"] * 2,
                    "area_name": ["0"] * 2,
                    "area_id": ["0"] * 2,
                    "analysis_day_number": [1] * 2,
                }
            ),
            pd.DataFrame(
                {
                    "analysis_date": [date(2000, 1, 1), date(2000, 1, 2)],
                    "forecast_date": [date(2000, 1, 1), date(2000, 1, 2)],
                    "distributor": ["CAIC"] * 2,
                    "area_name": ["0"] * 2,
                    "area_id": ["0"] * 2,
                    "analysis_day_number": [122, 123],
                }
            ),
        ),
        (
            "Other fill values are respected",
            date(2000, 1, 1),
            date(2000, 1, 2),
            {"feature_2": 3},
            pd.DataFrame(
                {
                    "analysis_date": [date(2000, 1, 2)],
                    "forecast_date": [date(2000, 1, 2)],
                    "distributor": ["CAIC"],
                    "area_name": ["0"],
                    "area_id": ["0"],
                    "feature_1": [5.0],
                    "feature_2": [5.0],
                }
            ),
            pd.DataFrame(
                {
                    "analysis_date": [date(2000, 1, 1), date(2000, 1, 2)],
                    "forecast_date": [date(2000, 1, 1), date(2000, 1, 2)],
                    "distributor": ["CAIC"] * 2,
                    "area_name": ["0"] * 2,
                    "area_id": ["0"] * 2,
                    "feature_1": [0.0, 5.0],
                    "feature_2": [3.0, 5.0],
                }
            ),
        ),
    ],
)
def test_get_features__missing_entries_imputed(
    desc, start_analysis_date, end_analysis_date, fill_values, query_results, expected
):
    with mock.patch(
        "pandas.read_sql",
        return_value=query_results,
    ):
        args = ["uri", ["CAIC"], start_analysis_date, end_analysis_date, ["feature_1"]]
        if fill_values is not None:
            args.append(fill_values)
        actual = get_features(*args)
        pd.testing.assert_frame_equal(actual, expected, check_like=True)
