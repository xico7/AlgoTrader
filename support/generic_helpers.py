from datetime import datetime
import time
from support.data_handling.data_helpers.vars_constants import TEN_SECONDS_IN_MS, PROGRAM_NAME
import inspect


class InvalidDictProvided(Exception): pass


def get_current_second_in_ms():
    return int(time.time()) * 1000


def seconds_to_ms(seconds) -> int:
    return int(seconds * 1000)


def mins_to_ms(minutes) -> int:
    return int(minutes * 1000 * 60)


def mins_to_seconds(minutes) -> int:
    return int(minutes * 60)


def ms_to_mins(milliseconds) -> float:  # Not used but useful sometimes.
    return float(milliseconds / 1000 / 60)


def get_next_millisecond_modulo(start_timestamp, milliseconds: int):
    return start_timestamp + milliseconds - (start_timestamp % milliseconds)


def current_time_in_ms(cur_time_in_secs: float):
    return cur_time_in_secs * 1000


def round_last_ten_secs(timestamp):
    return timestamp - TEN_SECONDS_IN_MS + (TEN_SECONDS_IN_MS - (timestamp % TEN_SECONDS_IN_MS))


def date_from_timestamp_in_ms(timestamp_in_ms):
    return datetime.fromtimestamp(timestamp_in_ms / 1000)


# TODO: Improve this function to make it iterate for unlimited 'childs' in dict with dicts.
def get_dict_key_path_one_child_only(parse_dict: dict, key: str):
    for k, v in parse_dict.items():
        if k == key:
            return k
    else:
        for k, v in parse_dict.items():
            if isinstance(v, dict):
                for k_2, v_2 in v.items():
                    if k_2 == key:
                        return [k, k_2]

    raise InvalidDictProvided("Object doesn't contain key, or it is 'deeper' than this function allows it to be.")


def get_value_from_dict_with_path(dict_to_get_value: dict, *args):
    for i in args:
        dict_to_get_value = dict_to_get_value[i]
    return dict_to_get_value


def import_classes_dynamically(name):
    components = name.split('.')
    mod = __import__(components[0])
    for comp in components[1:]:
        mod = getattr(mod, comp)
    return mod


def get_object_project_relative_path(module_object):
    return inspect.getfile(module_object).split(PROGRAM_NAME)[1].lstrip('\\').replace('\\', '.').rstrip('.py')
