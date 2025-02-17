class DummyWidgets:
    def __init__(self, *args, **kwargs):
        pass

    def close(self):
        pass


try:
    from ipywidgets.widgets import *

except ImportError:
    class HTML(DummyWidgets):
        pass

    class Valid(DummyWidgets):
        pass

