import functools
import typing

INITIATING = False
GET_MAPPING: dict[str, typing.Callable] = {}
GET_MAPPING_NO_ARG: dict[str, typing.Callable] = {}
UPDATE_MAPPING: dict[str, typing.Callable] = {}
SYNC_MAPPING: dict[str, typing.Callable] = {}


class RegisterEnv:
    def __enter__(self):
        global INITIATING
        INITIATING = True
        # print("run")

    def __exit__(self, exc_type, exc_val, exc_tb):
        global INITIATING
        INITIATING = False
        # print("exit")


def is_same_func(callable1: typing.Any, callable2):
    try:
        if (callable1.__code__.co_name == callable2.__code__.co_name) and \
                (callable1.__code__.co_filename == callable2.__code__.co_filename):
            return True
        else:
            return False
    except Exception as e:
        print(e)


def is_lru_wrapper(callable_obj):
    return callable_obj.__class__.__name__ == functools.lru_cache(lambda x: x).__class__.__name__


def register_get(name: str):
    """注册常规get"""
    def decorator(callable_obj):
        if not INITIATING:
            return callable_obj
        if name not in GET_MAPPING:
            GET_MAPPING[name] = callable_obj
            return callable_obj

        if is_lru_wrapper(callable_obj) and GET_MAPPING[name] != callable_obj:
            raise ValueError(f'duplicated get function `{name}`')
        elif not is_same_func(GET_MAPPING[name], callable_obj):
            raise ValueError(f'duplicated get function `{name}`')
        return GET_MAPPING[name]

    return decorator


def register_get_no_arg(name: str):
    """注册get(no args)"""
    def decorator(callable_obj):
        # if not INITIATING:
        #     return callable_obj
        # if name in GET_MAPPING_NO_ARG:
        #     raise ValueError(f'duplicated get (no arg) function `{name}`')
        # GET_MAPPING_NO_ARG[name] = callable_obj
        # return callable_obj

        if not INITIATING:
            return callable_obj
        if name not in GET_MAPPING_NO_ARG:
            GET_MAPPING_NO_ARG[name] = callable_obj
            return callable_obj

        if is_lru_wrapper(callable_obj) and GET_MAPPING_NO_ARG[name] != callable_obj:
            raise ValueError(f'duplicated get (no arg) function `{name}`')
        elif not is_same_func(GET_MAPPING_NO_ARG[name], callable_obj):
            raise ValueError(f'duplicated get (no arg) function `{name}`')
        return GET_MAPPING_NO_ARG[name]

    return decorator


def register_update(name: str):
    def decorator(callable_obj):
        # if not INITIATING:
        #     return callable_obj
        # if name in UPDATE_MAPPING:
        #     raise ValueError(f'duplicated update function `{name}`')
        # UPDATE_MAPPING[name] = callable_obj
        # return callable_obj

        if not INITIATING:
            return callable_obj
        if name not in UPDATE_MAPPING:
            UPDATE_MAPPING[name] = callable_obj
            return callable_obj

        if is_lru_wrapper(callable_obj) and UPDATE_MAPPING[name] != callable_obj:
            raise ValueError(f'duplicated get (no arg) function `{name}`')
        elif not is_same_func(UPDATE_MAPPING[name], callable_obj):
            raise ValueError(f'duplicated get (no arg) function `{name}`')
        return UPDATE_MAPPING[name]

    return decorator


def register_sync(name: str):
    def decorator(callable_obj):
        # if not INITIATING:
        #     return callable_obj
        # if name in UPDATE_MAPPING:
        #     raise ValueError(f'duplicated update function `{name}`')
        # UPDATE_MAPPING[name] = callable_obj
        # return callable_obj

        if not INITIATING:
            return callable_obj
        if name not in SYNC_MAPPING:
            SYNC_MAPPING[name] = callable_obj
            return callable_obj

        if is_lru_wrapper(callable_obj) and SYNC_MAPPING[name] != callable_obj:
            raise ValueError(f'duplicated get (no arg) function `{name}`')
        elif not is_same_func(SYNC_MAPPING[name], callable_obj):
            raise ValueError(f'duplicated get (no arg) function `{name}`')
        return SYNC_MAPPING[name]

    return decorator
