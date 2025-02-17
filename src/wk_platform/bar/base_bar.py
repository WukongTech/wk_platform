from pyalgotrade.bar import Bars


class FastBars(object):

    """A group of :class:`Bar` objects.

    :param barDict: A map of instrument to :class:`Bar` objects.
    :type barDict: map.

    .. note::
        All bars must have the same datetime.
    """

    def __init__(self, barDict):
        self.__barDict = barDict
        # Check that bar datetimes are in sync
        first_bar = None

        for instrument, current_bar_func in barDict.items():
            first_bar = current_bar_func()[1]
            break

        self.__barDict = barDict
        self.__dateTime = first_bar.getDateTime()

    def __getitem__(self, instrument):
        """Returns the :class:`pyalgotrade.bar.Bar` for the given instrument.
        If the instrument is not found an exception is raised."""
        bar = self.__barDict[instrument]
        if isinstance(bar, tuple):
            bar = bar[1]()
            self.__barDict[instrument] = bar
        return self.__barDict[instrument]

    def __contains__(self, instrument):
        """Returns True if a :class:`pyalgotrade.bar.Bar` for the given instrument is available."""
        return instrument in self.__barDict

    def items(self):
        return list(self.__barDict.items())

    def keys(self):
        return list(self.__barDict.keys())

    def getInstruments(self):
        """Returns the instrument symbols."""
        return list(self.__barDict.keys())

    def getDateTime(self):
        """Returns the :class:`datetime.datetime` for this set of bars."""
        return self.__dateTime

    def getBar(self, instrument):
        """Returns the :class:`pyalgotrade.bar.Bar` for the given instrument or None if the instrument is not found."""
        return self.__barDict.get(instrument, None)
