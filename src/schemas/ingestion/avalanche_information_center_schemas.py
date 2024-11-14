import pandera as pa
import pandas as pd
from datetime import date
from enum import IntEnum


class IntEnumDefaultNoForecast(IntEnum):
    @classmethod
    def _missing_(cls, value: str) -> int:  # pragma: no cover
        # If the provided enum key is undefined, return a NO_FORECAST value
        try:
            return cls[value]
        except KeyError:
            return cls.NOFORECAST


class AvalancheRiskEnum(IntEnumDefaultNoForecast):
    NOFORECAST = 0
    LOW = 1
    MODERATE = 2
    CONSIDERABLE = 3
    HIGH = 4
    EXTREME = 5


class AvalancheProblemEnum(IntEnumDefaultNoForecast):
    NOFORECAST = 0
    LOOSEDRY = 1
    STORMSLAB = 2
    WINDSLAB = 3
    PERSISTENTSLAB = 4
    DEEPPERSISTENTSLAB = 5
    WETLOOSE = 6
    WETSLAB = 7
    CORNICE = 8
    GLIDE = 9


class AvalancheLikelihoodEnum(IntEnumDefaultNoForecast):
    NOFORECAST = 0
    UNLIKELY = 1
    POSSIBLE = 2
    LIKELY = 3
    VERYLIKELY = 4
    ALMOSTCERTAIN = 5


class AvalancheForecastSchema(pa.DataFrameModel):
    """Pandera schema definition of regional avalanche forecast datasets."""

    distributor: str
    publish_datetime: pd.DatetimeTZDtype = pa.Field(
        dtype_kwargs={"unit": "ms", "tz": "UTC"}
    )
    analysis_datetime: pd.DatetimeTZDtype = pa.Field(
        dtype_kwargs={"unit": "ms", "tz": "UTC"}
    )
    forecast_date: date
    forecast_days_out: int
    forecast_date_season_day_number: int
    avalanche_season: str
    area_name: str = pa.Field(default="undefined")
    area_id: str
    polygons: str
    avalanche_summary: str = pa.Field(default="")
    danger_alp: int = pa.Field(default=0)
    danger_tln: int = pa.Field(default=0)
    danger_btl: int = pa.Field(default=0)
    problem_0: int = pa.Field(default=0)
    likelihood_0: int = pa.Field(default=0)
    min_size_0: float = pa.Field(default=0.0)
    max_size_0: float = pa.Field(default=0.0)
    n_alp_0: int = pa.Field(default=0)
    n_tln_0: int = pa.Field(default=0)
    n_btl_0: int = pa.Field(default=0)
    ne_alp_0: int = pa.Field(default=0)
    ne_tln_0: int = pa.Field(default=0)
    ne_btl_0: int = pa.Field(default=0)
    e_alp_0: int = pa.Field(default=0)
    e_tln_0: int = pa.Field(default=0)
    e_btl_0: int = pa.Field(default=0)
    se_alp_0: int = pa.Field(default=0)
    se_tln_0: int = pa.Field(default=0)
    se_btl_0: int = pa.Field(default=0)
    s_alp_0: int = pa.Field(default=0)
    s_tln_0: int = pa.Field(default=0)
    s_btl_0: int = pa.Field(default=0)
    sw_alp_0: int = pa.Field(default=0)
    sw_tln_0: int = pa.Field(default=0)
    sw_btl_0: int = pa.Field(default=0)
    w_alp_0: int = pa.Field(default=0)
    w_tln_0: int = pa.Field(default=0)
    w_btl_0: int = pa.Field(default=0)
    nw_alp_0: int = pa.Field(default=0)
    nw_tln_0: int = pa.Field(default=0)
    nw_btl_0: int = pa.Field(default=0)
    prevalence_0: int = pa.Field(default=0)
    alp_prevalence_0: int = pa.Field(default=0)
    tln_prevalence_0: int = pa.Field(default=0)
    btl_prevalence_0: int = pa.Field(default=0)
    problem_1: int = pa.Field(default=0)
    likelihood_1: int = pa.Field(default=0)
    min_size_1: float = pa.Field(default=0.0)
    max_size_1: float = pa.Field(default=0.0)
    n_alp_1: int = pa.Field(default=0)
    n_tln_1: int = pa.Field(default=0)
    n_btl_1: int = pa.Field(default=0)
    ne_alp_1: int = pa.Field(default=0)
    ne_tln_1: int = pa.Field(default=0)
    ne_btl_1: int = pa.Field(default=0)
    e_alp_1: int = pa.Field(default=0)
    e_tln_1: int = pa.Field(default=0)
    e_btl_1: int = pa.Field(default=0)
    se_alp_1: int = pa.Field(default=0)
    se_tln_1: int = pa.Field(default=0)
    se_btl_1: int = pa.Field(default=0)
    s_alp_1: int = pa.Field(default=0)
    s_tln_1: int = pa.Field(default=0)
    s_btl_1: int = pa.Field(default=0)
    sw_alp_1: int = pa.Field(default=0)
    sw_tln_1: int = pa.Field(default=0)
    sw_btl_1: int = pa.Field(default=0)
    w_alp_1: int = pa.Field(default=0)
    w_tln_1: int = pa.Field(default=0)
    w_btl_1: int = pa.Field(default=0)
    nw_alp_1: int = pa.Field(default=0)
    nw_tln_1: int = pa.Field(default=0)
    nw_btl_1: int = pa.Field(default=0)
    prevalence_1: int = pa.Field(default=0)
    alp_prevalence_1: int = pa.Field(default=0)
    tln_prevalence_1: int = pa.Field(default=0)
    btl_prevalence_1: int = pa.Field(default=0)
    problem_2: int = pa.Field(default=0)
    likelihood_2: int = pa.Field(default=0)
    min_size_2: float = pa.Field(default=0.0)
    max_size_2: float = pa.Field(default=0.0)
    n_alp_2: int = pa.Field(default=0)
    n_tln_2: int = pa.Field(default=0)
    n_btl_2: int = pa.Field(default=0)
    ne_alp_2: int = pa.Field(default=0)
    ne_tln_2: int = pa.Field(default=0)
    ne_btl_2: int = pa.Field(default=0)
    e_alp_2: int = pa.Field(default=0)
    e_tln_2: int = pa.Field(default=0)
    e_btl_2: int = pa.Field(default=0)
    se_alp_2: int = pa.Field(default=0)
    se_tln_2: int = pa.Field(default=0)
    se_btl_2: int = pa.Field(default=0)
    s_alp_2: int = pa.Field(default=0)
    s_tln_2: int = pa.Field(default=0)
    s_btl_2: int = pa.Field(default=0)
    sw_alp_2: int = pa.Field(default=0)
    sw_tln_2: int = pa.Field(default=0)
    sw_btl_2: int = pa.Field(default=0)
    w_alp_2: int = pa.Field(default=0)
    w_tln_2: int = pa.Field(default=0)
    w_btl_2: int = pa.Field(default=0)
    nw_alp_2: int = pa.Field(default=0)
    nw_tln_2: int = pa.Field(default=0)
    nw_btl_2: int = pa.Field(default=0)
    prevalence_2: int = pa.Field(default=0)
    alp_prevalence_2: int = pa.Field(default=0)
    tln_prevalence_2: int = pa.Field(default=0)
    btl_prevalence_2: int = pa.Field(default=0)
