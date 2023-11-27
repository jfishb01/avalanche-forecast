import os
from datetime import date, datetime
from enum import StrEnum, IntEnum
from pydantic import BaseModel


class ForecastDistributorEnum(StrEnum):
    CAIC = "CAIC"
    NWAC = "NWAC"


class AvalancheRiskEnum(IntEnum):
    NORATING = -2
    NOFORECAST = -1
    EARLYSEASON = 0
    LOW = 1
    MODERATE = 2
    CONSIDERABLE = 3
    HIGH = 4
    EXTREME = 5


class AvalancheProblemEnum(IntEnum):
    NOFORECAST = -1
    LOOSEDRY = 1
    STORMSLAB = 2
    WINDSLAB = 3
    PERSISTENTSLAB = 4
    DEEPPERSISTENTSLAB = 5
    WETLOOSE = 6
    WETSLAB = 7
    CORNICE = 8
    GLIDE = 9


class AvalancheLikelihoodEnum(IntEnum):
    NOFORECAST = -1
    UNLIKELY = 1
    POSSIBLE = 2
    LIKELY = 3
    VERYLIKELY = 4
    CERTAIN = 5


class RawAvalancheForecast(BaseModel):
    analysis_date: date
    forecast: str


class TransformedAvalancheForecast(BaseModel):
    distributor: ForecastDistributorEnum
    analysis_date: date
    forecast_date: date
    avalanche_season: str
    area_name: str
    area_id: str
    polygons: str
    avalanche_summary: str
    danger_alp: AvalancheRiskEnum = AvalancheRiskEnum.NOFORECAST
    danger_tln: AvalancheRiskEnum = AvalancheRiskEnum.NOFORECAST
    danger_btl: AvalancheRiskEnum = AvalancheRiskEnum.NOFORECAST
    problem_0: AvalancheProblemEnum = AvalancheProblemEnum.NOFORECAST
    likelihood_0: AvalancheLikelihoodEnum = AvalancheLikelihoodEnum.NOFORECAST
    min_size_0: float = -1.0
    max_size_0: float = -1.0
    n_alp_0: bool = False
    n_tln_0: bool = False
    n_btl_0: bool = False
    ne_alp_0: bool = False
    ne_tln_0: bool = False
    ne_btl_0: bool = False
    e_alp_0: bool = False
    e_tln_0: bool = False
    e_btl_0: bool = False
    se_alp_0: bool = False
    se_tln_0: bool = False
    se_btl_0: bool = False
    s_alp_0: bool = False
    s_tln_0: bool = False
    s_btl_0: bool = False
    sw_alp_0: bool = False
    sw_tln_0: bool = False
    sw_btl_0: bool = False
    w_alp_0: bool = False
    w_tln_0: bool = False
    w_btl_0: bool = False
    nw_alp_0: bool = False
    nw_tln_0: bool = False
    nw_btl_0: bool = False
    problem_1: AvalancheProblemEnum = AvalancheProblemEnum.NOFORECAST
    likelihood_1: AvalancheLikelihoodEnum = AvalancheLikelihoodEnum.NOFORECAST
    min_size_1: float = -1.0
    max_size_1: float = -1.0
    n_alp_1: bool = False
    n_tln_1: bool = False
    n_btl_1: bool = False
    ne_alp_1: bool = False
    ne_tln_1: bool = False
    ne_btl_1: bool = False
    e_alp_1: bool = False
    e_tln_1: bool = False
    e_btl_1: bool = False
    se_alp_1: bool = False
    se_tln_1: bool = False
    se_btl_1: bool = False
    s_alp_1: bool = False
    s_tln_1: bool = False
    s_btl_1: bool = False
    sw_alp_1: bool = False
    sw_tln_1: bool = False
    sw_btl_1: bool = False
    w_alp_1: bool = False
    w_tln_1: bool = False
    w_btl_1: bool = False
    nw_alp_1: bool = False
    nw_tln_1: bool = False
    nw_btl_1: bool = False
    problem_2: AvalancheProblemEnum = AvalancheProblemEnum.NOFORECAST
    likelihood_2: AvalancheLikelihoodEnum = AvalancheLikelihoodEnum.NOFORECAST
    min_size_2: float = -1.0
    max_size_2: float = -1.0
    n_alp_2: bool = False
    n_tln_2: bool = False
    n_btl_2: bool = False
    ne_alp_2: bool = False
    ne_tln_2: bool = False
    ne_btl_2: bool = False
    e_alp_2: bool = False
    e_tln_2: bool = False
    e_btl_2: bool = False
    se_alp_2: bool = False
    se_tln_2: bool = False
    se_btl_2: bool = False
    s_alp_2: bool = False
    s_tln_2: bool = False
    s_btl_2: bool = False
    sw_alp_2: bool = False
    sw_tln_2: bool = False
    sw_btl_2: bool = False
    w_alp_2: bool = False
    w_tln_2: bool = False
    w_btl_2: bool = False
    nw_alp_2: bool = False
    nw_tln_2: bool = False
    nw_btl_2: bool = False


def forecast_filename(
    distributor: ForecastDistributorEnum, analysis_date: date, base_dir: str
) -> str:
    """Ensure consistent filenames for avalanche forecast data for both generation and retrieval.

    Without building out a metadata database to track ingested files, we need a way to easily identify
    the analysis date that a file refers to. This method creates a clear naming scheme to track records across
    distributors and analysis dates. In the long-term, this should be replaced with a database storing metadate
    about the ingested files.
    """
    return os.path.join(
        os.path.join(base_dir, distributor), f"{analysis_date.isoformat()}.json"
    )


def analysis_date_from_forecast_filename(filename: str) -> date:
    """Get the analysis data from a forecast filename.

    In the long-term, this should be replaced with a database that stores this metadate as it is more
    resilient to changes and will allow for improved flexibility.
    """
    return datetime.strptime(os.path.basename(filename), "%Y-%m-%d.json").date()
