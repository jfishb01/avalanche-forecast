import pytest
from unittest.mock import patch, call
from datetime import date
from fastapi import HTTPException

from src.utils.datetime_helpers import date_range
from src.ingestion.avalanche_forecast import load
from src.ingestion.avalanche_forecast.common import (
    ForecastDistributorEnum,
    forecast_filename,
)


@pytest.fixture
def mock_client():
    class dummy_client:
        def command(self, *args, **kwargs):
            pass

    with patch(
        "src.ingestion.avalanche_forecast.load.get_client", return_value=dummy_client()
    ) as m:
        yield m


@pytest.fixture
def mock_insert_file():
    with patch("src.ingestion.avalanche_forecast.load.insert_file") as m:
        yield m


@pytest.mark.parametrize(
    "desc,start_date,end_date",
    [
        (
            "Single file loaded performs one insert",
            date(2000, 1, 1),
            date(2000, 1, 1),
        ),
        (
            "Multiple files loaded performs an insert for each file",
            date(2000, 1, 1),
            date(2000, 1, 3),
        ),
    ],
)
def test_load__all_files_loaded(
    mock_client, mock_insert_file, desc, start_date, end_date
):
    distributors = [ForecastDistributorEnum.CAIC]
    src = "/test/downloads"
    load.load(
        load.ApiQueryParams(
            distributors=distributors,
            start_date=start_date,
            end_date=end_date,
            src=src,
            dest="http://example.com",
        )
    )

    database, table = load.DB_TABLE.split(".")
    expected_calls = []
    for analysis_date in date_range(start_date, end_date):
        filename = forecast_filename(distributors[0], analysis_date, src)
        expected_calls.append(
            call(mock_client(), table, filename, database=database, fmt="JSONEachRow")
        )
    mock_insert_file.assert_has_calls(expected_calls)


@pytest.mark.parametrize(
    "desc,start_date,end_date",
    [
        (
            "Single file load calls 'create table' once",
            date(2000, 1, 1),
            date(2000, 1, 1),
        ),
        (
            "Multiple file load still calls 'create table' once",
            date(2000, 1, 1),
            date(2000, 1, 3),
        ),
    ],
)
def test_load__create_table_only_called_once(
    mock_client, mock_insert_file, desc, start_date, end_date
):
    distributors = [ForecastDistributorEnum.CAIC]
    src = "/test/downloads"
    with patch(
        "src.ingestion.avalanche_forecast.load._create_table", return_value=True
    ) as m:
        load.load(
            load.ApiQueryParams(
                distributors=distributors,
                start_date=start_date,
                end_date=end_date,
                src=src,
                dest="http://example.com",
            )
        )
        m.assert_called_once()


def test_load__create_table_called_multiple_times_during_failures(
    mock_client, mock_insert_file
):
    def side_effect(client, filename):
        if filename == forecast_filename(
            ForecastDistributorEnum.CAIC, date(2000, 1, 1), "/test/downloads"
        ):
            raise Exception()
        return True

    distributors = [ForecastDistributorEnum.CAIC]
    src = "/test/downloads"
    with pytest.raises(HTTPException):
        with patch(
            "src.ingestion.avalanche_forecast.load._create_table",
            side_effect=side_effect,
        ) as m:
            load.load(
                load.ApiQueryParams(
                    distributors=distributors,
                    start_date=date(2000, 1, 1),
                    end_date=date(2000, 1, 5),
                    src=src,
                    dest="http://example.com",
                )
            )
            assert m.call_count == 2


def test_load__only_new_files_inserted(mock_client, mock_insert_file):
    def side_effect(client, distributor, analysis_date):
        if analysis_date == date(2000, 1, 2):
            return False
        return True

    database, table = load.DB_TABLE.split(".")
    distributors = [ForecastDistributorEnum.CAIC]
    src = "/test/downloads"
    with patch(
        "src.ingestion.avalanche_forecast.load._data_already_loaded",
        side_effect=side_effect,
    ):
        load.load(
            load.ApiQueryParams(
                distributors=distributors,
                start_date=date(2000, 1, 1),
                end_date=date(2000, 1, 5),
                src=src,
                dest="http://example.com",
            )
        )

    expected_file = forecast_filename(
        ForecastDistributorEnum.CAIC, date(2000, 1, 2), src
    )
    mock_insert_file.assert_called_once_with(
        mock_client(), table, expected_file, database=database, fmt="JSONEachRow"
    )


def test_load__all_files_inserted_with_force_load(mock_client, mock_insert_file):
    def side_effect(client, distributor, analysis_date):
        if analysis_date == date(2000, 1, 2):
            return False
        return True

    database, table = load.DB_TABLE.split(".")
    distributors = [ForecastDistributorEnum.CAIC]
    src = "/test/downloads"
    start_date = date(2000, 1, 1)
    end_date = date(2000, 1, 5)
    with patch(
        "src.ingestion.avalanche_forecast.load._data_already_loaded",
        side_effect=side_effect,
    ):
        load.load(
            load.ApiQueryParams(
                distributors=distributors,
                start_date=start_date,
                end_date=end_date,
                src=src,
                dest="http://example.com",
                force_load=True,
            )
        )

    expected_calls = []
    for analysis_date in date_range(start_date, end_date):
        filename = forecast_filename(ForecastDistributorEnum.CAIC, analysis_date, src)
        expected_calls.append(
            call(mock_client(), table, filename, database=database, fmt="JSONEachRow")
        )
    mock_insert_file.assert_has_calls(expected_calls)


def test_load__relative_source_path_raises_exception(mock_client, mock_insert_file):
    with pytest.raises(HTTPException):
        load.load(
            load.ApiQueryParams(
                distributors=[ForecastDistributorEnum.CAIC],
                start_date=date(2000, 1, 1),
                end_date=date(2000, 1, 5),
                src="test/downloads",
                dest="http://example.com",
                force_load=True,
            )
        )


def test_load__failed_load_raises_exception(mock_client, mock_insert_file):
    def side_effect(*args, **kwargs):
        raise RuntimeError

    with pytest.raises(HTTPException):
        mock_insert_file.side_effect = side_effect
        load.load(
            load.ApiQueryParams(
                distributors=[ForecastDistributorEnum.CAIC],
                start_date=date(2000, 1, 1),
                end_date=date(2000, 1, 5),
                src="test/downloads",
                dest="http://example.com",
                force_load=True,
            )
        )
