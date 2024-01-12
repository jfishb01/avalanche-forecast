import os
from datetime import date, datetime
from enum import StrEnum, IntEnum
from pydantic import BaseModel, computed_field


NUM_POSSIBLE_PROBLEMS = 3
ASPECTS = ("n", "nw", "w", "sw", "s", "se", "e", "ne")
ELEVATIONS = ("alp", "tln", "btl")


class ForecastDistributorEnum(StrEnum):
    CAIC = "CAIC"
    NWAC = "NWAC"


class AvalancheRiskEnum(IntEnum):
    NOFORECAST = 0
    LOW = 1
    MODERATE = 2
    CONSIDERABLE = 3
    HIGH = 4
    EXTREME = 5

    @classmethod
    def _missing_(cls, value: str):  # pragma: no cover
        try:
            return cls[value]
        except KeyError:
            return cls.NOFORECAST


class AvalancheProblemEnum(IntEnum):
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

    @classmethod
    def _missing_(cls, value: str):  # pragma: no cover
        try:
            return cls[value]
        except KeyError:
            return cls.NOFORECAST


class AvalancheLikelihoodEnum(IntEnum):
    NOFORECAST = 0
    UNLIKELY = 1
    POSSIBLE = 2
    LIKELY = 3
    VERYLIKELY = 4
    ALMOSTCERTAIN = 5

    @classmethod
    def _missing_(cls, value: str):  # pragma: no cover
        try:
            return cls[value]
        except KeyError:
            return cls.NOFORECAST


class RawAvalancheForecast(BaseModel):
    publish_date: date
    forecast: str


class AvalancheForecastFeatureSet(BaseModel):
    distributor: ForecastDistributorEnum
    publish_date: date
    observation_date: date
    publish_day_number: int
    observation_day_number: int
    analysis_date: date
    forecast_date: date
    analysis_day_number: int
    forecast_day_number: int
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
    min_size_0: float = 0.0
    max_size_0: float = 0.0
    n_alp_0: int = 0
    n_tln_0: int = 0
    n_btl_0: int = 0
    ne_alp_0: int = 0
    ne_tln_0: int = 0
    ne_btl_0: int = 0
    e_alp_0: int = 0
    e_tln_0: int = 0
    e_btl_0: int = 0
    se_alp_0: int = 0
    se_tln_0: int = 0
    se_btl_0: int = 0
    s_alp_0: int = 0
    s_tln_0: int = 0
    s_btl_0: int = 0
    sw_alp_0: int = 0
    sw_tln_0: int = 0
    sw_btl_0: int = 0
    w_alp_0: int = 0
    w_tln_0: int = 0
    w_btl_0: int = 0
    nw_alp_0: int = 0
    nw_tln_0: int = 0
    nw_btl_0: int = 0
    problem_1: AvalancheProblemEnum = AvalancheProblemEnum.NOFORECAST
    likelihood_1: AvalancheLikelihoodEnum = AvalancheLikelihoodEnum.NOFORECAST
    min_size_1: float = 0.0
    max_size_1: float = 0.0
    n_alp_1: int = 0
    n_tln_1: int = 0
    n_btl_1: int = 0
    ne_alp_1: int = 0
    ne_tln_1: int = 0
    ne_btl_1: int = 0
    e_alp_1: int = 0
    e_tln_1: int = 0
    e_btl_1: int = 0
    se_alp_1: int = 0
    se_tln_1: int = 0
    se_btl_1: int = 0
    s_alp_1: int = 0
    s_tln_1: int = 0
    s_btl_1: int = 0
    sw_alp_1: int = 0
    sw_tln_1: int = 0
    sw_btl_1: int = 0
    w_alp_1: int = 0
    w_tln_1: int = 0
    w_btl_1: int = 0
    nw_alp_1: int = 0
    nw_tln_1: int = 0
    nw_btl_1: int = 0
    problem_2: AvalancheProblemEnum = AvalancheProblemEnum.NOFORECAST
    likelihood_2: AvalancheLikelihoodEnum = AvalancheLikelihoodEnum.NOFORECAST
    min_size_2: float = 0.0
    max_size_2: float = 0.0
    n_alp_2: int = 0
    n_tln_2: int = 0
    n_btl_2: int = 0
    ne_alp_2: int = 0
    ne_tln_2: int = 0
    ne_btl_2: int = 0
    e_alp_2: int = 0
    e_tln_2: int = 0
    e_btl_2: int = 0
    se_alp_2: int = 0
    se_tln_2: int = 0
    se_btl_2: int = 0
    s_alp_2: int = 0
    s_tln_2: int = 0
    s_btl_2: int = 0
    sw_alp_2: int = 0
    sw_tln_2: int = 0
    sw_btl_2: int = 0
    w_alp_2: int = 0
    w_tln_2: int = 0
    w_btl_2: int = 0
    nw_alp_2: int = 0
    nw_tln_2: int = 0
    nw_btl_2: int = 0

    @computed_field
    @property
    def alp_prevalence_0(self) -> int:
        return self._prevalence(0, "alp")

    @computed_field
    @property
    def tln_prevalence_0(self) -> int:
        return self._prevalence(0, "tln")

    @computed_field
    @property
    def btl_prevalence_0(self) -> int:
        return self._prevalence(0, "btl")

    @computed_field
    @property
    def prevalence_0(self) -> int:
        return self.alp_prevalence_0 + self.tln_prevalence_0 + self.btl_prevalence_0

    @computed_field
    @property
    def alp_prevalence_1(self) -> int:
        return self._prevalence(1, "alp")

    @computed_field
    @property
    def tln_prevalence_1(self) -> int:
        return self._prevalence(1, "tln")

    @computed_field
    @property
    def btl_prevalence_1(self) -> int:
        return self._prevalence(1, "btl")

    @computed_field
    @property
    def prevalence_1(self) -> int:
        return self.alp_prevalence_1 + self.tln_prevalence_1 + self.btl_prevalence_1

    @computed_field
    @property
    def alp_prevalence_2(self) -> int:
        return self._prevalence(2, "alp")

    @computed_field
    @property
    def tln_prevalence_2(self) -> int:
        return self._prevalence(2, "tln")

    @computed_field
    @property
    def btl_prevalence_2(self) -> int:
        return self._prevalence(2, "btl")

    @computed_field
    @property
    def prevalence_2(self) -> int:
        return self.alp_prevalence_2 + self.tln_prevalence_2 + self.btl_prevalence_2

    def _prevalence(self, problem_number: int, elevation: str) -> int:
        return sum(
            [
                getattr(self, f"{aspect}_{elevation}_{problem_number}")
                for aspect in ASPECTS
            ]
        )
