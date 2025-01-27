import pytest
import math
from typing import Tuple

from src.core.transformations.avalanche_forecast_center_transformations import (
    convert_aspects_to_sin_cos_and_range,
)


@pytest.mark.parametrize(
    "description, method_kwargs, expected",
    [
        pytest.param(
            "All aspects returns max range",
            dict(
                n=1,
                ne=1,
                e=1,
                se=1,
                s=1,
                sw=1,
                w=1,
                nw=1,
            ),
            (0.0, 0.0, 1.0),
            id="all_aspects_returns_max_range",
        ),
        pytest.param(
            "No aspects returns 0 range",
            dict(
                n=0,
                ne=0,
                e=0,
                se=0,
                s=0,
                sw=0,
                w=0,
                nw=0,
            ),
            (0.0, 0.0, 0.0),
            id="no_aspects_returns_0_range",
        ),
        pytest.param(
            "Single aspects returns correct values",
            dict(
                n=0,
                ne=0,
                e=0,
                se=1,
                s=0,
                sw=0,
                w=0,
                nw=0,
            ),
            (math.sin((3 * 2 * math.pi) / 8), math.cos((3 * 2 * math.pi) / 8), 1 / 8),
            id="single_aspect_returns_correct_values",
        ),
        pytest.param(
            "Multiple contiguous aspects returns central aspect with range",
            dict(
                n=0,
                ne=0,
                e=1,
                se=1,
                s=1,
                sw=0,
                w=0,
                nw=0,
            ),
            (math.sin((3 * 2 * math.pi) / 8), math.cos((3 * 2 * math.pi) / 8), 3 / 8),
            id="multiple_contiguous_aspects_returns_central_aspect_with_range",
        ),
        pytest.param(
            "Multiple even number of contiguous aspects returns central aspect with range",
            dict(
                n=0,
                ne=0,
                e=1,
                se=1,
                s=1,
                sw=1,
                w=0,
                nw=0,
            ),
            (
                math.sin((3.5 * 2 * math.pi) / 8),
                math.cos((3.5 * 2 * math.pi) / 8),
                4 / 8,
            ),
            id="multiple_even_number_ofcontiguous_aspects_returns_central_aspect_with_range",
        ),
        pytest.param(
            "Wrap around case correctly handled",
            dict(
                n=1,
                ne=1,
                e=0,
                se=0,
                s=0,
                sw=0,
                w=1,
                nw=1,
            ),
            (
                math.sin((7.5 * 2 * math.pi) / 8),
                math.cos((7.5 * 2 * math.pi) / 8),
                4 / 8,
            ),
            id="wrap_around_case_correctly_handled",
        ),
        pytest.param(
            "Multiple contiguous blocks chooses largest",
            dict(
                n=1,
                ne=0,
                e=0,
                se=1,
                s=1,
                sw=1,
                w=0,
                nw=0,
            ),
            (math.sin(math.pi), math.cos(math.pi), 3 / 8),
            id="multiple_contiguous_blocks_chooses_largest",
        ),
        pytest.param(
            "Multiple contiguous blocks of same size chooses first",
            dict(
                n=1,
                ne=1,
                e=0,
                se=1,
                s=1,
                sw=1,
                w=0,
                nw=1,
            ),
            (math.sin(0), math.cos(0), 3 / 8),
            id="multiple_contiguous_blocks_of_same_size_chooses_first",
        ),
    ],
)
def test_convert_aspects_to_sin_cos_and_range(
    description: str,
    method_kwargs: dict,
    expected: Tuple[float, float, float],
):
    actual = convert_aspects_to_sin_cos_and_range(**method_kwargs)
    assert actual == expected
