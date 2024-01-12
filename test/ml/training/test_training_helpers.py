import pytest
import numpy as np
import pandas as pd
from unittest.mock import patch
from datetime import date, timedelta

from src.ml.training.training_helpers import get_train_test_set, get_train_test_split


def test_get_train_test_set__assert_calls():
    db_uri = "uri"
    distributors = ["CAIC"]
    train_start_analysis_date = date(2000, 1, 1)
    test_end_analysis_date = date(2000, 1, 1)
    features = ["feature1", "feature2"]
    target = "target"
    fill_values = 0
    with patch("src.ml.training.training_helpers.get_features") as m1:
        with patch("src.ml.training.training_helpers.get_targets") as m2:
            get_train_test_set(
                db_uri,
                distributors,
                train_start_analysis_date,
                test_end_analysis_date,
                features,
                target,
            )
            m1.assert_called_once_with(
                db_uri,
                distributors,
                train_start_analysis_date,
                test_end_analysis_date,
                features,
                fill_values,
            )
            m2.assert_called_once_with(
                db_uri,
                distributors,
                train_start_analysis_date,
                test_end_analysis_date,
                target,
            )


@pytest.mark.parametrize(
    "desc,feature_df,target_df,expected",
    [
        (
            "Train and test set merged on forecast area",
            pd.DataFrame(
                {
                    "distributor": ["CAIC", "CAIC", "NWAC", "NWAC"],
                    "area_name": ["area_0", "area_1", "area_0", "area_1"],
                    "area_id": ["0", "1", "0", "1"],
                    "analysis_date": [date(2000, 1, 1)] * 4,
                    "forecast_date": [date(2000, 1, 1)] * 4,
                }
            ),
            pd.DataFrame(
                {
                    "distributor": ["CAIC", "CAIC", "NWAC"],
                    "area_name": ["area_0", "area_1", "area_0"],
                    "area_id": ["0", "1", "0"],
                    "observation_date": [date(2000, 1, 1)] * 3,
                    "target": [1] * 3,
                }
            ),
            pd.DataFrame(
                {
                    "distributor": ["CAIC", "CAIC", "NWAC"],
                    "area_name": ["area_0", "area_1", "area_0"],
                    "area_id": ["0", "1", "0"],
                    "analysis_date": [date(2000, 1, 1)] * 3,
                    "forecast_date": [date(2000, 1, 1)] * 3,
                    "observation_date": [date(2000, 1, 1)] * 3,
                    "target": [1] * 3,
                }
            ),
        ),
        (
            "Train and test set merged on forecast date",
            pd.DataFrame(
                {
                    "distributor": ["NWAC"] * 2,
                    "area_name": ["area_0"] * 2,
                    "area_id": ["0"] * 2,
                    "analysis_date": [date(2000, 1, 1)] * 2,
                    "forecast_date": [date(2000, 1, 2), date(2000, 1, 3)],
                }
            ),
            pd.DataFrame(
                {
                    "distributor": ["NWAC"] * 2,
                    "area_name": ["area_0"] * 2,
                    "area_id": ["0"] * 2,
                    "observation_date": [date(2000, 1, 1), date(2000, 1, 2)],
                    "target": [1] * 2,
                }
            ),
            pd.DataFrame(
                {
                    "distributor": ["NWAC"],
                    "area_name": ["area_0"],
                    "area_id": ["0"],
                    "analysis_date": [date(2000, 1, 1)],
                    "forecast_date": [date(2000, 1, 2)],
                    "observation_date": [date(2000, 1, 2)],
                    "target": [1],
                }
            ),
        ),
        (
            "Duplicate merge columns are filtered",
            pd.DataFrame(
                {
                    "distributor": ["NWAC"],
                    "area_name": ["area_0"],
                    "area_id": ["0"],
                    "analysis_date": [date(2000, 1, 1)],
                    "forecast_date": [date(2000, 1, 1)],
                    "observation_date": [date(2000, 1, 1)],
                }
            ),
            pd.DataFrame(
                {
                    "distributor": ["NWAC"],
                    "area_name": ["area_0"],
                    "area_id": ["0"],
                    "observation_date": [date(2000, 1, 1)],
                    "target": [1],
                }
            ),
            pd.DataFrame(
                {
                    "distributor": ["NWAC"],
                    "area_name": ["area_0"],
                    "area_id": ["0"],
                    "analysis_date": [date(2000, 1, 1)],
                    "forecast_date": [date(2000, 1, 1)],
                    "observation_date": [date(2000, 1, 1)],
                    "target": [1],
                }
            ),
        ),
        (
            "Reeturned dataset is sorted by distributor, area_id, analysis_date",
            pd.DataFrame(
                {
                    "distributor": ["NWAC", "NWAC", "NWAC", "CAIC"],
                    "area_name": ["area_0"] * 4,
                    "area_id": ["1", "0", "0", "0"],
                    "analysis_date": [
                        date(2000, 1, 1),
                        date(2000, 1, 2),
                        date(2000, 1, 1),
                        date(2000, 1, 1),
                    ],
                    "forecast_date": [
                        date(2000, 1, 1),
                        date(2000, 1, 2),
                        date(2000, 1, 3),
                        date(2000, 1, 4),
                    ],
                }
            ),
            pd.DataFrame(
                {
                    "distributor": ["NWAC", "NWAC", "NWAC", "CAIC"],
                    "area_name": ["area_0"] * 4,
                    "area_id": ["1", "0", "0", "0"],
                    "observation_date": [
                        date(2000, 1, 1),
                        date(2000, 1, 2),
                        date(2000, 1, 3),
                        date(2000, 1, 4),
                    ],
                    "target": [1] * 4,
                }
            ),
            pd.DataFrame(
                {
                    "distributor": ["CAIC", "NWAC", "NWAC", "NWAC"],
                    "area_name": ["area_0"] * 4,
                    "area_id": ["0", "0", "0", "1"],
                    "analysis_date": [
                        date(2000, 1, 1),
                        date(2000, 1, 1),
                        date(2000, 1, 2),
                        date(2000, 1, 1),
                    ],
                    "forecast_date": [
                        date(2000, 1, 4),
                        date(2000, 1, 3),
                        date(2000, 1, 2),
                        date(2000, 1, 1),
                    ],
                    "observation_date": [
                        date(2000, 1, 4),
                        date(2000, 1, 3),
                        date(2000, 1, 2),
                        date(2000, 1, 1),
                    ],
                    "target": [1] * 4,
                }
            ),
        ),
    ],
)
def test_get_train_test_set(desc, feature_df, target_df, expected):
    with patch(
        "src.ml.training.training_helpers.get_features", return_value=feature_df
    ):
        with patch(
            "src.ml.training.training_helpers.get_targets", return_value=target_df
        ):
            actual = get_train_test_set(
                db_uri="uri",
                distributors=["CAIC"],
                train_start_analysis_date=date(2000, 1, 1),
                test_end_analysis_date=date(2000, 1, 1),
                features=["feature1", "feature2"],
                target="target",
            )
            # import pdb; pdb.set_trace()
            pd.testing.assert_frame_equal(actual, expected, check_like=True)


@pytest.mark.parametrize(
    "desc,train_start_analysis_date,test_end_analysis_date,train_end_analysis_date,expected_date_splits",
    [
        (
            "Training set includes train_end_analysis_date",
            date(2000, 1, 1),
            date(2000, 1, 4),
            date(2000, 1, 2),
            (
                (
                    date(2000, 1, 1),
                    date(2000, 1, 2),
                ),
                (
                    date(2000, 1, 3),
                    date(2000, 1, 4),
                ),
            ),
        ),
        (
            "Training set is empty when split before training set begins",
            date(2000, 1, 1),
            date(2000, 1, 4),
            date(1999, 1, 1),
            (
                (),
                (
                    date(2000, 1, 1),
                    date(2000, 1, 2),
                    date(2000, 1, 3),
                    date(2000, 1, 4),
                ),
            ),
        ),
        (
            "Test set is empty when split after test set end",
            date(2000, 1, 1),
            date(2000, 1, 4),
            date(2001, 1, 1),
            (
                (
                    date(2000, 1, 1),
                    date(2000, 1, 2),
                    date(2000, 1, 3),
                    date(2000, 1, 4),
                ),
                (),
            ),
        ),
    ],
)
def test_get_train_test_split(
    desc,
    train_start_analysis_date,
    test_end_analysis_date,
    train_end_analysis_date,
    expected_date_splits,
):
    analysis_dates = []
    forecast_dates = []
    analysis_date = train_start_analysis_date
    forecast_date_offset = timedelta(days=3)
    while analysis_date <= test_end_analysis_date:
        analysis_dates.append(analysis_date)
        forecast_dates.append(analysis_date + forecast_date_offset)
        analysis_date += timedelta(days=1)

    train_dates, test_dates = expected_date_splits
    features_with_target = pd.DataFrame(
        {
            "distributor": ["CAIC", "NWAC"] * len(analysis_dates),
            "area_id": ["1", "2"] * len(analysis_dates),
            "area_name": ["area_0", "area_1"] * len(analysis_dates),
            "analysis_date": np.repeat(analysis_dates, 2),
            "forecast_date": np.repeat(forecast_dates, 2),
            "observation_date": np.repeat(forecast_dates, 2),
            "feature": np.repeat(list(range(len(analysis_dates))), 2),
            "target": np.repeat(list(range(len(analysis_dates))), 2),
        }
    )

    X_train, X_test, y_train, y_test = get_train_test_split(
        features_with_target, train_end_analysis_date
    )
    pd.testing.assert_frame_equal(
        X_train,
        pd.DataFrame(
            {
                "distributor": ["CAIC", "NWAC"] * len(train_dates),
                "area_id": ["1", "2"] * len(train_dates),
                "area_name": ["area_0", "area_1"] * len(train_dates),
                "analysis_date": np.repeat(train_dates, 2),
                "forecast_date": np.repeat(
                    [d + forecast_date_offset for d in train_dates], 2
                ),
                "feature": np.repeat(list(range(len(train_dates))), 2),
            }
        ),
        check_dtype=False,
    )
    pd.testing.assert_frame_equal(
        X_test,
        pd.DataFrame(
            {
                "distributor": ["CAIC", "NWAC"] * len(test_dates),
                "area_id": ["1", "2"] * len(test_dates),
                "area_name": ["area_0", "area_1"] * len(test_dates),
                "analysis_date": np.repeat(test_dates, 2),
                "forecast_date": np.repeat(
                    [d + forecast_date_offset for d in test_dates], 2
                ),
                "feature": np.repeat(
                    list(range(len(train_dates), len(train_dates) + len(test_dates))), 2
                ),
            }
        ),
        check_dtype=False,
    )
    pd.testing.assert_frame_equal(
        y_train,
        pd.DataFrame(
            {
                "distributor": ["CAIC", "NWAC"] * len(train_dates),
                "area_id": ["1", "2"] * len(train_dates),
                "area_name": ["area_0", "area_1"] * len(train_dates),
                "observation_date": np.repeat(
                    [d + forecast_date_offset for d in train_dates], 2
                ),
                "target": np.repeat(list(range(len(train_dates))), 2),
            }
        ),
        check_dtype=False,
    )
    pd.testing.assert_frame_equal(
        y_test,
        pd.DataFrame(
            {
                "distributor": ["CAIC", "NWAC"] * len(test_dates),
                "area_id": ["1", "2"] * len(test_dates),
                "area_name": ["area_0", "area_1"] * len(test_dates),
                "observation_date": np.repeat(
                    [d + forecast_date_offset for d in test_dates], 2
                ),
                "target": np.repeat(
                    list(range(len(train_dates), len(train_dates) + len(test_dates))), 2
                ),
            }
        ),
        check_dtype=False,
    )
