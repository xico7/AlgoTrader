import time
from data_handling.data_helpers.vars_constants import TEN_SECONDS_IN_MS


def get_current_second_in_ms():
    return int(time.time()) * 1000


def seconds_to_ms(seconds):
    seconds_in_ms = seconds * 1000
    return int(seconds_in_ms) if isinstance(seconds_in_ms, int) else seconds_in_ms


def mins_to_ms(minutes):
    return seconds_to_ms(minutes) * 60


def get_next_millisecond_modulo(start_timestamp, milliseconds: int):
    return start_timestamp + milliseconds - (start_timestamp % milliseconds)


def current_time_in_ms(cur_time_in_secs: float):
    return cur_time_in_secs * 1000


def round_last_ten_secs(timestamp):
    return timestamp - TEN_SECONDS_IN_MS + (TEN_SECONDS_IN_MS - (timestamp % TEN_SECONDS_IN_MS))


def get_key_values_from_dict_with_dicts(parse_obj: dict, key: str):
    for k, v in parse_obj.items():
        if k == key:
            if isinstance(v, float):
                return v
        elif isinstance(v, dict):
            transverse_dict = get_key_values_from_dict_with_dicts(v, key)
            if isinstance(transverse_dict, float):
                return transverse_dict
