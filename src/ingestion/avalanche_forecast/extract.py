"""Extract avalanche forecasts from regional forecast distributors and save them to the provided destination."""

import os
import logging
import argparse
import traceback
from datetime import date, datetime
from pydantic import BaseModel
from typing import Iterable, Callable
from fastapi import FastAPI, HTTPException

from src.utils.loggers import set_console_logger
from src.ingestion.avalanche_forecast.distributors import caic, nwac
from src.ingestion.avalanche_forecast.common import (
    ForecastDistributorEnum,
    RawAvalancheForecast,
    forecast_filename,
)


app = FastAPI()


class ApiQueryParams(BaseModel):
    distributors: Iterable[ForecastDistributorEnum]
    start_date: date
    end_date: date
    dest: str


@app.post("/extract")
def extract(ApiQueryParams) -> None:
    """Extract avalanche forecasts from the provided distributors over a date range and saves them."""
    exceptions = []
    for distributor in ApiQueryParams.distributors:
        try:
            forecasts = _get_extractor(distributor)(
                ApiQueryParams.start_date, ApiQueryParams.end_date
            )
            _save(distributor, forecasts, ApiQueryParams.dest)
        except:
            exceptions.append(f"{distributor.name}: {traceback.format_exc()}")
    if exceptions:
        exceptions_str = "\n".join(exceptions)
        logging.error(exceptions_str)
        raise HTTPException(
            status_code=500,
            detail=f"Extraction failed with the following exceptions:\n\n{exceptions_str}",
        )


def _get_extractor(
    distributor: ForecastDistributorEnum,
) -> Callable[[date, date], Iterable[RawAvalancheForecast]]:
    """Factory to get an extraction method corresponding to the provided distributor."""
    if distributor == ForecastDistributorEnum.CAIC:
        return caic.extract
    if distributor == ForecastDistributorEnum.NWAC:
        return nwac.extract
    raise KeyError(f"Unknown distributor: {distributor}")


def _save(
    distributor: ForecastDistributorEnum,
    forecasts: Iterable[RawAvalancheForecast],
    dest: str,
) -> None:
    """Save the forecasts to destination directory. Forecasts are saved to <dest>/<distributor>/<date>.json."""
    base_dir_created = False
    for forecast_data in forecasts:
        output_filename = forecast_filename(
            distributor, forecast_data.analysis_date, dest
        )
        if not base_dir_created:
            os.makedirs(os.path.dirname(output_filename), exist_ok=True)
            base_dir_created = True
        logging.info(
            f"Saving {distributor} forecasts for {forecast_data.analysis_date.isoformat()}"
        )
        with open(
            output_filename,
            "w",
        ) as f:
            f.write(forecast_data.forecast)


def main():
    set_console_logger()
    distributors_str = "\n\t".join(list(ForecastDistributorEnum))
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--distributors",
        dest="distributors",
        action="store",
        required=True,
        help=f"Comma separated list of forecast distributors to download from (ie CAIC,NWAC).\n\tOptions: "
        f"{distributors_str}",
    )
    parser.add_argument(
        "--start-date",
        dest="start_date",
        action="store",
        required=False,
        default=date.today().strftime("%Y-%m-%d"),
        help="Start analysis date, format: YYYY-MM-DD",
    )
    parser.add_argument(
        "--end-date",
        dest="end_date",
        action="store",
        required=False,
        default=date.today().strftime("%Y-%m-%d"),
        help="End analysis date inclusive, format: YYYY-MM-DD",
    )
    parser.add_argument(
        "--dest",
        dest="dest",
        action="store",
        required=True,
        help="Output directory to place the downloaded files",
    )
    args = parser.parse_args()
    extract(
        ApiQueryParams(
            distributors=args.distributors.split(","),
            start_date=datetime.strptime(args.start_date, "%Y-%m-%d").date(),
            end_date=datetime.strptime(args.end_date, "%Y-%m-%d").date(),
            dest=args.dest,
        )
    )


if __name__ == "__main__":
    main()
