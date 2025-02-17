try:
    from IPython.display import display
except ImportError:
    def display(*args, **kwargs):
        pass
