import math
import statistics
import numpy as np
from typing import Tuple


def convert_aspects_to_sin_cos_and_range(
    n: int,
    ne: int,
    e: int,
    se: int,
    s: int,
    sw: int,
    w: int,
    nw: int,
) -> Tuple[float, float, float]:
    """Given impacted aspects, get the sin and cos of the mean aspect angle and range of aspects affected.

    The aspect arguments should all be expressed as bit flags (0 or 1) with a value of 1 indicating that the
    aspect is impacted by the problem type.

    For the return tuple, the sin and cos values are computed for the mean aspect angle for the largest
    contiguous block of impacted aspects. The range is expressed as a value from 0.0 to 1.0 where 0.0
    indicates no aspects are impacted, 1.0 means all aspects are impacted, and 0.5 means half the aspects are
    impacted (0.25 to the left of the mean aspect angle and 0.25 to the right of the mean aspect angle). If
    multiple blocks of contiguous aspects are found, the mean aspect angle for the largest contiguous block will
    be returned. If multiple contiguous blocks have the same size, then the mean aspect angle for the first
    contiguous block will be returned starting from north going clockwise. If no aspects are impacted or all
    aspects are impacted, the sin and cos values will both be returned as 0.0.
    """
    # Convert impacted aspect arguments to array.
    impacted_aspects = np.array([n, ne, e, se, s, sw, w, nw]).astype(bool)
    aspect_radians = [
        (i * 2 * math.pi) / len(impacted_aspects)
        for i in range(len(impacted_aspects) + 1)
    ]

    # All aspects case.
    if all(impacted_aspects):
        return 0.0, 0.0, 1.0

    # No aspects case.
    if not any(impacted_aspects):
        return 0.0, 0.0, 0.0

    # Find blocks of contiguous affected aspects.
    current_sub_array = 0
    contiguous_sub_arrays = {}
    for index, aspect in enumerate(impacted_aspects):
        if aspect:
            contiguous_sub_arrays[current_sub_array] = contiguous_sub_arrays.get(
                current_sub_array, []
            ) + [index]
        else:
            current_sub_array += 1

    # Check for wrap-around case and join contiguous subsets if found.
    min_sub_array_index = min(contiguous_sub_arrays.keys())
    max_sub_array_index = max(contiguous_sub_arrays.keys())
    if (
        min_sub_array_index == 0
        and (len(impacted_aspects) - 1) in contiguous_sub_arrays[max_sub_array_index]
    ):
        # Join wrap around subsets in clockwise orientation (same orientation as impacted_aspects array).
        wrap_around = contiguous_sub_arrays.pop(max_sub_array_index)
        contiguous_sub_arrays[0] = wrap_around + contiguous_sub_arrays[0]

    # Use the largest contiguous aspect block if multiple contiguous aspect blocks found.
    # I don't think this case ever occurs, but best to safeguard against the case of multiple
    # contiguous blocks as we can only predict a single value.
    largest_contiguous_block = contiguous_sub_arrays[min_sub_array_index]
    for sub_array in contiguous_sub_arrays.values():
        if len(sub_array) > len(largest_contiguous_block):
            largest_contiguous_block = sub_array

    # Get the aspect(s) that are at the center of our largest contiguous block.
    middle_indices = {
        math.floor((len(largest_contiguous_block) - 1) / 2),
        math.ceil((len(largest_contiguous_block) - 1) / 2),
    }
    center_aspects = [largest_contiguous_block[i] for i in middle_indices]

    # Handle the wrap around case when converting the center aspect to radians.
    center_aspect_radians = [aspect_radians[i] for i in center_aspects]
    if len(center_aspect_radians) == 2 and center_aspect_radians[1] == 0:
        center_aspect_radians[1] = 2 * math.pi

    center_aspect_sin = math.sin(statistics.mean(center_aspect_radians))
    center_aspect_cos = math.cos(statistics.mean(center_aspect_radians))
    center_aspect_range = len(largest_contiguous_block) / len(impacted_aspects)
    return center_aspect_sin, center_aspect_cos, center_aspect_range
