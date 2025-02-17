# PyAlgoTrade
#
# Copyright 2011-2018 Gabriel Martin Becedillas Ruiz
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
.. moduleauthor:: Gabriel Martin Becedillas Ruiz <gabriel.becedillas@gmail.com>
"""
import typing
import pandas as pd
import six
from collections import OrderedDict
from pyalgotrade import utils
from pyalgotrade import bar
from wk_util.tqdm import tqdm
from wk_platform import barfeed
from wk_platform.bar.base_bar import FastBars
from wk_util.algo import bin_search


# A non real-time BarFeed responsible for:
# - Holding bars in memory.
# - Aligning them with respect to time.
#
# Subclasses should:
# - Forward the call to start() if they override it.

class BarFeed(barfeed.BaseBarFeed):
    def __init__(self, frequency, maxLen=None):
        super(BarFeed, self).__init__(frequency, maxLen)

        self.__bars = {}
        self.__nextPos = {}
        self.__started = False
        self.__currDateTime = None


    def reset(self):
        self.__nextPos = {}
        for instrument in self.__bars.keys():
            self.__nextPos.setdefault(instrument, 0)
        self.__currDateTime = None
        super(BarFeed, self).reset()

    def getCurrentDateTime(self):
        return self.__currDateTime

    def start(self):
        super(BarFeed, self).start()
        self.__started = True

    def stop(self):
        pass

    def join(self):
        pass

    def addBarsFromSequence(self, instrument, bars):
        if self.__started:
            raise Exception("Can't add more bars once you started consuming bars")

        self.__bars.setdefault(instrument, [])
        self.__nextPos.setdefault(instrument, 0)

        # Add and sort the bars
        self.__bars[instrument].extend(bars)
        self.__bars[instrument].sort(key=lambda b: b.getDateTime())

        self.registerInstrument(instrument)


    def eof(self):
        ret = True
        # Check if there is at least one more bar to return.
        for instrument, bars in six.iteritems(self.__bars):
            nextPos = self.__nextPos[instrument]
            if nextPos < len(bars):
                ret = False
                break
        return ret

    def peekDateTime(self):
        ret = None

        for instrument, bars in six.iteritems(self.__bars):
            nextPos = self.__nextPos[instrument]
            if nextPos < len(bars):
                ret = utils.safe_min(ret, bars[nextPos].getDateTime())
        return ret

    def getNextBars(self):
        # All bars must have the same datetime. We will return all the ones with the smallest datetime.
        smallestDateTime = self.peekDateTime()

        if smallestDateTime is None:
            return None

        # Make a second pass to get all the bars that had the smallest datetime.
        ret = {}
        # TODO: 将此步骤在预处理中完成
        for instrument, bars in self.__bars.items():
            nextPos = self.__nextPos[instrument]
            if nextPos < len(bars) and bars[nextPos].getDateTime() == smallestDateTime:
                ret[instrument] = bars[nextPos]
                self.__nextPos[instrument] += 1

        if self.__currDateTime == smallestDateTime:
            raise Exception("Duplicate bars found for %s on %s" % (list(ret.keys()), smallestDateTime))

        self.__currDateTime = smallestDateTime
        return bar.Bars(ret)

    def loadAll(self):
        for dateTime, bars in self:
            pass


class MacroBars:
    def __init__(self, bar_time):
        self.__bar_time = bar_time
        self.__data: dict[str, pd.DataFrame] = {}
        self.__parser: dict[str, typing.Callable] = {}
        self.__cache = None

    def add_data(self, name, df, parser):
        self.__data[name] = df
        self.__parser[name] = parser.parse_row
        # self.__parser[name] = parser.lazy_parser
        # self.parsed_bars()

    def parse_bars(self, force=False):
        if self.__cache is not None and not force:
            return
        result = {}
        for k, df in self.__data.items():
            parser = self.__parser[k]
            for row in df.itertuples():
                inst, bar_ = parser(row)
                result[inst] = bar_
            # values: pd.Series = df.apply(parser, axis=1)
            # for inst, bar_ in values.values:
            #     result[inst] = bar_
        self.__cache = result

    def items(self):
        return self.bars().items()

    def set_data(self, data):
        self.__cache = {k: v for k, v in data if not isinstance(v, float)}

    def bars(self):
        if self.__cache is None:
            self.parse_bars()
        return self.__cache


class FastBarFeed(barfeed.BaseBarFeed):
    """
    快速bar_feed，添加数据时为原始的DataFrame，回测调用时再实时解析具体的bar
    """
    def __init__(self, frequency, maxLen=None):
        super().__init__(frequency, maxLen)

        self.__bars = {}
        self.__nextPos = {}
        self.__started = False
        self.__currDateTime = None

        self.__data_seq: dict[str, MacroBars] = OrderedDict()
        self.__cursor = 0
        self.__max_cursor = 0
        self.__bar_time = []
        self.__specified_range = False
        self.__current_feed_df = None

    def reset(self):
        self.__cursor = 0
        self.__bar_time = sorted(self.__data_seq.keys())
        self.__currDateTime = None
        self.__specified_range = False
        super().reset()

    def getCurrentDateTime(self):
        return self.__currDateTime

    def start(self):
        super().start()
        if not self.__specified_range:
            self.__cursor = 0
            self.__bar_time = sorted(self.__data_seq.keys())
            self.__max_cursor = len(self.__bar_time)
        self.__started = True

    def align_time(self, max_time):
        bar_time = sorted(self.__data_seq.keys())
        idx = bin_search(bar_time, max_time)
        for k in bar_time[idx+1:]:
            del self.__data_seq[k]

    def stop(self):
        pass

    def join(self):
        pass

    def addBarsFromSequence(self, instrument, bars):
        if self.__started:
            raise Exception("Can't add more bars once you started consuming bars")

        self.__bars.setdefault(instrument, [])
        self.__nextPos.setdefault(instrument, 0)

        # Add and sort the bars
        self.__bars[instrument].extend(bars)
        self.__bars[instrument].sort(key=lambda b: b.getDateTime())

        self.registerInstrument(instrument)

    def add_data_from_dataframe(self, tag, time_field_name, data_frame: pd.DataFrame, row_parser, progress_bar=False):
        group = data_frame.groupby(time_field_name)  # 按照windcode分类
        groups = data_frame.groupby(time_field_name).groups  # groups类型为dict
        max_date = None
        for bar_time in tqdm(list(groups.keys()), disable=(not progress_bar)):
            data_part = group.get_group(str(bar_time))
            try:
                macro_bars = self.__data_seq[str(bar_time)]
                macro_bars.add_data(tag, data_part, row_parser)
            except KeyError:
                self.__data_seq[str(bar_time)] = MacroBars(bar_time)
                self.__data_seq[str(bar_time)].add_data(tag, data_part, row_parser)

            if max_date is None or bar_time > max_date:
                max_date = bar_time
        return max_date

    def get_data_seq(self):
        return self.__data_seq

    def data_seq_to_feed_df(self, refresh=False, progress_bar=False):
        df_list = []
        bar_time = sorted(self.__data_seq.keys())
        if self.__current_feed_df is None or refresh is True:
            pass
        else:
            newest_dt = self.__current_feed_df.index.max()
            start_idx = bin_search(bar_time, newest_dt)
            start_idx = start_idx + 1
            bar_time = bar_time[start_idx:]
            df_list.append(self.__current_feed_df)
        for k in tqdm(bar_time, disable=(not progress_bar)):
            row = self.__data_seq[k]
            row_data = {col: [v] for col, v in row.items()}
            row_df = pd.DataFrame.from_dict(row_data, orient='columns')
            row_df.index = [k]
            df_list.append(row_df)
        self.__current_feed_df = pd.concat(df_list)
        return self.__current_feed_df

    def add_data_from_feed_df(self, feed_df, progress_bar=False):
        result = {}
        feed_df.sort_index(inplace=True)
        columns = feed_df.columns
        for value in tqdm(feed_df.itertuples(), disable=(not progress_bar)):
            k = value[0]
            result[k] = MacroBars(k)
            result[k].set_data(zip(columns, value[1:]))
        self.__data_seq = result

    def prefetch(self, progress_bar=False, force=False):
        for k, v in tqdm(self.__data_seq.items(), disable=(not progress_bar)):
            v.parse_bars(force)

    def eof(self):
        return self.__cursor == self.__max_cursor

    def peekDateTime(self):
        ret = self.__bar_time[self.__cursor]
        return ret

    def set_start_date(self, dt_str):
        self.__bar_time = sorted(self.__data_seq.keys())
        self.__specified_range = True
        c = bin_search(self.__bar_time, dt_str, 'large')
        self.__cursor = c

    def set_end_date(self, dt_str):
        self.__bar_time = sorted(self.__data_seq.keys())
        self.__specified_range = True
        self.__max_cursor = bin_search(self.__bar_time, dt_str, 'small') + 1

    def get_last_date(self):
        self.reset()
        return self.__bar_time[-1]

    def getNextBars(self):
        # All bars must have the same datetime. We will return all the ones with the smallest datetime.
        smallestDateTime = self.peekDateTime()

        if smallestDateTime is None:
            return None

        bars = self.__data_seq[smallestDateTime].bars()
        self.__cursor += 1

        # if self.__currDateTime == smallestDateTime:
        #     raise Exception("Duplicate bars found for %s on %s" % (list(ret.keys()), smallestDateTime))

        self.__currDateTime = smallestDateTime
        return bar.Bars(bars)

    def loadAll(self):
        for dateTime, bars in self:
            pass

    def getNextValuesAndUpdateDS(self):
        dateTime, values = self.getNextValues()
        # if dateTime is not None:
        #     for key, value in values.items():
        #         # Get or create the datseries for each key.
        #         try:
        #             ds = self.__ds[key]
        #         except KeyError:
        #             ds = self.createDataSeries(key, self.__maxLen)
        #             self.__ds[key] = ds
        #         ds.appendWithDateTime(dateTime, value)
        return dateTime, values
