from typing import Optional, Any, Union, Sequence


def try_get_field(
    obj: Union[dict, list],
    keys: Sequence[Union[int, str]],
    default: Optional[Any] = None,
) -> Optional[Any]:
    """Try to extract the nested keys from the object and if it doesn't exist, return the default value."""
    try:
        for k in keys:
            obj = obj[k]
        return obj
    except (KeyError, IndexError):
        return default
