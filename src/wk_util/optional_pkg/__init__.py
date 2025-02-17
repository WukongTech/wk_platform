import sys
try:
    get_ipython = sys.modules['IPython'].get_ipython
    if 'IPKernelApp' not in get_ipython().config:  # pragma: no cover
        raise ImportError("console")
except Exception:
    JUPYTER_AVAILABLE = False
else:
    JUPYTER_AVAILABLE = True
