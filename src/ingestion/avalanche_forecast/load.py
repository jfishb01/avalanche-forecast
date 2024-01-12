"""Load transformed avalanche forecast data from regional forecast distributors to a DB."""


import os
import logging
import argparse
import traceback
from datetime import date, datetime
from clickhouse_connect import get_client
from clickhouse_connect.driver.tools import insert_file
from clickhouse_connect.driver.httpclient import HttpClient
from fastapi import FastAPI, HTTPException
from typing import Iterable
from pydantic import BaseModel

from src.utils.datetime_helpers import date_range
from src.utils.loggers import set_console_logger
from src.ingestion.avalanche_forecast.ingestion_helpers import forecast_filename
from src.schemas.feature_sets.avalanche_forecast import ForecastDistributorEnum


app = FastAPI()

DB_TABLE = "external.avalanche_forecast"


class ApiQueryParams(BaseModel):
    distributors: Iterable[ForecastDistributorEnum]
    start_date: date
    end_date: date
    src: str
    dest: str
    force_load: bool = False


@app.post("/load")
def load(ApiQueryParams) -> None:
    """Load avalanche forecast data from a source directory over a date range into a destination DB.

    Note: for local development, the "src" directory must live within avalanche-forecast/downloads which is
    mapped to /sandbox/downloads in the local clickhouse docker image. When this system is deployed on cloud
    infrastructure, this issue will be resolved by using a remote file URL such as from S3 instead of a
    local file.
    """
    client = get_client(dsn=ApiQueryParams.dest)

    database, table = DB_TABLE.split(".")
    table_exists = False
    exceptions = []
    for distributor in ApiQueryParams.distributors:
        for publish_date in date_range(
            ApiQueryParams.start_date, ApiQueryParams.end_date
        ):
            filename = forecast_filename(distributor, publish_date, ApiQueryParams.src)
            try:
                assert os.path.isabs(filename), f"{filename} must be an absolute path."
                table_exists = table_exists or _create_table(client, filename)
                if ApiQueryParams.force_load or not _data_already_loaded(
                    client, distributor, publish_date
                ):
                    logging.info(f"Loading {filename}")
                    insert_file(
                        client, table, filename, database=database, fmt="JSONEachRow"
                    )
            except:
                exceptions.append(f"{filename}: {traceback.format_exc()}")

    if exceptions:
        exceptions_str = "\n".join(exceptions)
        logging.error(exceptions_str)
        raise HTTPException(
            status_code=500,
            detail=f"Load failed with the following exceptions:\n\n{exceptions_str}",
        )


def _create_table(client: HttpClient, filename: str) -> bool:
    parameters = {"filename": filename}
    client.command(
        f"CREATE TABLE if NOT EXISTS {DB_TABLE} ENGINE=MergeTree ORDER BY tuple() "
        f"EMPTY AS SELECT * FROM file(%(filename)s, JSONEachRow)",
        parameters=parameters,
    )
    return True


def _data_already_loaded(
    client: HttpClient, distributor: ForecastDistributorEnum, publish_date: date
) -> bool:
    parameters = {"distributor": distributor, "publish_date": publish_date}
    return bool(
        client.command(
            f"SELECT COUNT(1) FROM {DB_TABLE} "
            f"WHERE distributor = %(distributor)s AND publish_date = %(publish_date)s",
            parameters=parameters,
        )
    )


def main():  # pragma: no cover
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
        help="Input directory to read the transformed files to be loaded",
    )
    parser.add_argument(
        "--dest",
        dest="dest",
        action="store",
        required=True,
        help="Connection string of the database to load the transformed files",
    )
    parser.add_argument(
        "-f",
        "--force-load",
        dest="force_load",
        action="store_true",
        help="Forcefully load all files, regardless of potential duplication",
    )
    args = parser.parse_args()
    load(
        ApiQueryParams(
            distributors=args.distributors.split(","),
            start_date=datetime.strptime(args.start_date, "%Y-%m-%d").date(),
            end_date=datetime.strptime(args.end_date, "%Y-%m-%d").date(),
            src=args.src,
            dest=args.dest,
            force_load=args.force_load,
        )
    )


if __name__ == "__main__":  # pragma: no cover
    main()
