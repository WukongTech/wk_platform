class ServingMixin:
    def __init__(self):
        self.__init_tqdm = lambda x: x
        # self.