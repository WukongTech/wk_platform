class DataOutOfRangeException(Exception):
    """
    当使用的数据超出已有数据范围时抛出此异常
    """
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        print(self.msg)