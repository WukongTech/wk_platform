import numpy as np


def mean(values):
    ret = np.nan
    if len(values):
        ret = np.array(values).mean()
    return ret


def stddev(values, ddof=1):
    ret = np.nan
    if len(values) > 1:
        ret = np.array(values).std(ddof=ddof)
    return ret

