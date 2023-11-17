import os
import json
import argparse
import logging
import traceback
from pydantic import BaseModel
from datetime import date, datetime
from fastapi import FastAPI, HTTPException
from typing import Iterable, List, Callable

from src.utils.loggers import set_console_logger
from src.ingestion.avalanche_forecast.distributors import caic
from src.ingestion.avalanche_forecast.common import (
    ForecastDistributorEnum,
    TransformedAvalancheForecast,
    forecast_filename,
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
    exceptions = []
    for distributor in ApiQueryParams.distributors:
        logging.info(f"Processing distributor: {distributor}")
        try:
            transformed_forecasts = _get_transformer(distributor)(
                ApiQueryParams.start_date, ApiQueryParams.end_date, ApiQueryParams.src
            )
            _save(distributor, transformed_forecasts, ApiQueryParams.dest)
        except:
            exceptions.append(str({distributor: traceback.format_exc()}))
    if exceptions:
        exceptions_str = "\n".join(exceptions)
        logging.error(exceptions)
        raise HTTPException(
            status_code=500,
            detail=f"Transformation failed with the following exceptions:\n\n{exceptions_str}",
        )


def _get_transformer(
    distributor: ForecastDistributorEnum,
) -> Callable[[date, date, str], Iterable[List[TransformedAvalancheForecast]]]:
    if distributor == ForecastDistributorEnum.CAIC:
        return caic.transform
    raise KeyError(f"Unknown distributor: {distributor}")


def _save(
    distributor: ForecastDistributorEnum,
    transformed: Iterable[List[TransformedAvalancheForecast]],
    dest: str,
) -> None:
    base_dir_created = False
    for transformed_data in transformed:
        if not transformed_data:
            continue

        analysis_date = transformed_data[0].analysis_date
        output_filename = forecast_filename(distributor, analysis_date, dest)
        if not base_dir_created:
            os.makedirs(os.path.dirname(output_filename), exist_ok=True)
            base_dir_created = True
        logging.info(f"Saving forecasts for {analysis_date}")
        with open(output_filename, "w") as f:
            # Some extra formatting is necessary to serialize the list of json dumped models as json
            f.write(
                f"[{','.join([row.model_dump_json() for row in transformed_data])}]"
            )


def main():
    set_console_logger()
    distributors_str = "\n\t".join(list(ForecastDistributorEnum))
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--distributors",
        dest="distributors",
        action="store",
        required=True,
        help=f"Comma separated list of forecast distributors to transform data for (ie CAIC,FAC).\n\tOptions: "
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


if __name__ == "__main__":
    main()
