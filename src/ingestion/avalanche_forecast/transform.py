"""Transform raw avalanche forecasts from regional forecast distributors and save them."""

import os
import argparse
import logging
import traceback
from pydantic import BaseModel
from datetime import date, datetime
from fastapi import FastAPI, HTTPException
from typing import Iterable, List, Callable

from src.utils.loggers import set_console_logger
from src.ingestion.avalanche_forecast.distributors import caic, nwac
from src.ingestion.avalanche_forecast.ingestion_helpers import forecast_filename
from src.schemas.feature_sets.avalanche_forecast import (
    ForecastDistributorEnum,
    AvalancheForecastFeatureSet,
)


app = FastAPI()


class ApiQueryParams(BaseModel):
    distributors: Iterable[ForecastDistributorEnum]
    start_date: date
    end_date: date
    src: str
    dest: str


@app.post("/transform")
def transform(ApiQueryParams) -> None:
    """Transform avalanche forecasts from a source directory over a date range and saves them."""
    exceptions = []
    for distributor in ApiQueryParams.distributors:
        logging.info(f"Processing distributor: {distributor}")
        try:
            to_transform = _get_files_to_transform(
                distributor,
                ApiQueryParams.start_date,
                ApiQueryParams.end_date,
                ApiQueryParams.src,
            )
            transformed_forecasts = _get_transformer(distributor)(to_transform)
            _save(distributor, transformed_forecasts, ApiQueryParams.dest)
        except:
            exceptions.append(f"{distributor.name}: {traceback.format_exc()}")
    if exceptions:
        exceptions_str = "\n".join(exceptions)
        logging.error(exceptions_str)
        raise HTTPException(
            status_code=500,
            detail=f"Transformation failed with the following exceptions:\n\n{exceptions_str}",
        )


def _get_files_to_transform(
    distributor: ForecastDistributorEnum, start_date: date, end_date: date, src: str
) -> Iterable[str]:
    input_dir, start_file = os.path.split(
        forecast_filename(distributor, start_date, src)
    )
    end_file = os.path.basename(forecast_filename(distributor, end_date, src))
    all_raw_files = os.listdir(input_dir)
    return [
        os.path.join(input_dir, f) for f in all_raw_files if start_file <= f <= end_file
    ]


def _get_transformer(
    distributor: ForecastDistributorEnum,
) -> Callable[
    [Iterable[str]], Iterable[List[AvalancheForecastFeatureSet]]
]:  # pragma: no cover
    """Factory to get a transformation method corresponding to the provided distributor."""
    if distributor == ForecastDistributorEnum.CAIC:
        return caic.transform
    if distributor == ForecastDistributorEnum.NWAC:
        return nwac.transform
    raise KeyError(f"Unknown distributor: {distributor}")


def _save(
    distributor: ForecastDistributorEnum,
    transformed: Iterable[List[AvalancheForecastFeatureSet]],
    dest: str,
) -> None:
    """Save transformed data to the destination directory. Data are saved to <dest>/<distributor>/<date>.json."""
    base_dir_created = False
    for transformed_data in transformed:
        if not transformed_data:
            continue

        publish_date = transformed_data[0].publish_date
        output_filename = forecast_filename(distributor, publish_date, dest)
        if not base_dir_created:
            os.makedirs(os.path.dirname(output_filename), exist_ok=True)
            base_dir_created = True
        logging.info(f"Saving forecasts for {publish_date}")
        with open(output_filename, "w") as f:
            # Some extra formatting is necessary to serialize the list of json dumped models as json
            f.write("\n".join([row.model_dump_json() for row in transformed_data]))


def main():  # pragma: no cover
    set_console_logger()
    distributors_str = "\n\t".join(list(ForecastDistributorEnum))
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--distributors",
        dest="distributors",
        action="store",
        required=True,
        help=f"Comma separated list of forecast distributors to transform data for (ie CAIC,NWAC).\n\tOptions: "
        f"{distributors_str}",
    )
    parser.add_argument(
        "--start-date",
        dest="start_date",
        action="store",
        required=False,
        default=date.today().strftime("%Y-%m-%d"),
        help="Start publish date, format: YYYY-MM-DD",
    )
    parser.add_argument(
        "--end-date",
        dest="end_date",
        action="store",
        required=False,
        default=date.today().strftime("%Y-%m-%d"),
        help="End publish date inclusive, format: YYYY-MM-DD",
    )
    parser.add_argument(
        "--src",
        dest="src",
        action="store",
        required=True,
        help="Input directory to read the raw files to be transformed",
    )
    parser.add_argument(
        "--dest",
        dest="dest",
        action="store",
        required=True,
        help="Output directory to place the transformed files",
    )
    args = parser.parse_args()
    transform(
        ApiQueryParams(
            distributors=args.distributors.split(","),
            start_date=datetime.strptime(args.start_date, "%Y-%m-%d").date(),
            end_date=datetime.strptime(args.end_date, "%Y-%m-%d").date(),
            src=args.src,
            dest=args.dest,
        )
    )


if __name__ == "__main__":  # pragma: no cover
    main()
