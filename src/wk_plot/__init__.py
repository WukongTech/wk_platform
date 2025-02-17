import sys
from wk_util.logger import console_log
from pyecharts.charts import Line


def render2file(chart):
    chart.render()


def render2notebook(chart):
    return chart.render_notebook()


__render_func = render2file


def init():
    get_ipython = sys.modules['IPython'].get_ipython
    if 'IPKernelApp' in get_ipython().config:  # pragma: no cover
        from pyecharts.globals import CurrentConfig, NotebookType
        CurrentConfig.NOTEBOOK_TYPE = NotebookType.JUPYTER_LAB
        c = Line()
        __render_func = render2notebook
        console_log('using jupyter')
        return c.load_javascript()
    return None


def render(chart):
    return __render_func(chart)


