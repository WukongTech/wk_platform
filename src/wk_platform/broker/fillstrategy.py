# -*- coding: utf-8 -*-
# PyAlgoTrade
#
# Copyright 2011-2015 Gabriel Martin Becedillas Ruiz
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
.. moduleauthor:: Gabriel Martin Becedillas Ruiz <gabriel.becedillas@gmail.com>
"""
import enum

"""
Date: 20170719
Author: chenxiangdong
content:
该文件主要完成成交量的更新

"""

import abc
import copy

from pyalgotrade.bar import Frequency

from pyalgotrade.broker.fillstrategy import DefaultStrategy

from wk_util import logger
from wk_platform.util.round import round_100_shares
from wk_platform import broker
import wk_platform.feed.bar
from wk_platform.broker import slippage
from wk_platform.config import TradeRule



"""
获取最优
"""
# Returns the trigger price for a Limit or StopLimit order, or None if the limit price was not yet penetrated.
def get_limit_price_trigger(action, limitPrice, useAdjustedValues, bar):
    ret = None
    open_ = bar.getOpen(useAdjustedValues)
    high = bar.getHigh(useAdjustedValues)
    low = bar.getLow(useAdjustedValues)

    # If the bar is below the limit price, use the open price.
    # If the bar includes the limit price, use the open price or the limit price.
    if action in [broker.Order.Action.BUY, broker.Order.Action.BUY_TO_COVER]:
        if high < limitPrice:
            ret = open_
        elif limitPrice >= low:
            if open_ < limitPrice:  # The limit price was penetrated on open.
                ret = open_
            else:
                ret = limitPrice
    # If the bar is above the limit price, use the open price.
    # If the bar includes the limit price, use the open price or the limit price.
    elif action in [broker.Order.Action.SELL, broker.Order.Action.SELL_SHORT]:
        if low > limitPrice:
            ret = open_
        elif limitPrice <= high:
            if open_ > limitPrice:  # The limit price was penetrated on open.
                ret = open_
            else:
                ret = limitPrice
    else:  # Unknown action
        assert(False)
    return ret


# Returns the trigger price for a Stop or StopLimit order, or None if the stop price was not yet penetrated.
def get_stop_price_trigger(action, stopPrice, useAdjustedValues, bar):
    ret = None
    open_ = bar.getOpen(useAdjustedValues)
    high = bar.getHigh(useAdjustedValues)
    low = bar.getLow(useAdjustedValues)

    # If the bar is above the stop price, use the open price.
    # If the bar includes the stop price, use the open price or the stop price. Whichever is better.
    if action in [broker.Order.Action.BUY, broker.Order.Action.BUY_TO_COVER]:
        if low > stopPrice:
            ret = open_
        elif stopPrice <= high:
            if open_ > stopPrice:  # The stop price was penetrated on open.
                ret = open_
            else:
                ret = stopPrice
    # If the bar is below the stop price, use the open price.
    # If the bar includes the stop price, use the open price or the stop price. Whichever is better.
    elif action in [broker.Order.Action.SELL, broker.Order.Action.SELL_SHORT]:
        if high < stopPrice:
            ret = open_
        elif stopPrice >= low:
            if open_ < stopPrice:  # The stop price was penetrated on open.
                ret = open_
            else:
                ret = stopPrice
    else:  # Unknown action
        assert(False)

    return ret


def get_limit_price_in_bar_trigger(action, limitPrice, useAdjustedValues, bar):
    """
    校验bar内交易时limit_price是否合理
    :param action:
    :param limitPrice:
    :param useAdjustedValues:
    :param bar:
    :return:
    """
    ret = None
    open_ = bar.getOpen(useAdjustedValues)
    high = bar.getHigh(useAdjustedValues)
    low = bar.getLow(useAdjustedValues)

    return max(min(limitPrice, high), low)



class FillInfoTag(enum.Enum):
    NORMAL = '可正常交易'
    NOT_ALLOW_PARTIAL = "不允许部分成交"
    NOT_ALLOW_T0 = "T+1限制下当日持仓量不足"
    NOT_ALLOW_T0_PARTIAL = "T+1限制下当日持仓量不足且不允许部分成交"
    INVALID_LIMIT_PRICE = "限价单无效价格"


class FillInfo(object):
    def __init__(self, price, quantity, msg: FillInfoTag = FillInfoTag.NORMAL):
        self.__price = price
        assert isinstance(quantity, object)
        self.__quantity = quantity
        self.__msg = msg

    def getPrice(self):
        return self.__price

    def getQuantity(self):
        return self.__quantity

    def getMsg(self):
        return self.__msg


class CommonFillStrategy(DefaultStrategy):
    """
    Default fill strategy.

    :param volumeLimit: The proportion of the volume that orders can take up in a bar. Must be > 0 and <= 1.
        If None, then volume limit is not checked.
    :type volumeLimit: float

    This strategy works as follows:

    * A :class:`wk_pyalgotrade.broker.MarketOrder` is always filled using the open/close price.
    * A :class:`wk_pyalgotrade.broker.LimitOrder` will be filled like this:
        * If the limit price was penetrated with the open price, then the open price is used.
        * If the bar includes the limit price, then the limit price is used.
        * Note that when buying the price is penetrated if it gets <= the limit price, and when selling the price
          is penetrated if it gets >= the limit price
    * A :class:`wk_pyalgotrade.broker.StopOrder` will be filled like this:
        * If the stop price was penetrated with the open price, then the open price is used.
        * If the bar includes the stop price, then the stop price is used.
        * Note that when buying the price is penetrated if it gets >= the stop price, and when selling the price
          is penetrated if it gets <= the stop price
    * A :class:`wk_pyalgotrade.broker.StopLimitOrder` will be filled like this:
        * If the stop price was penetrated with the open price, or if the bar includes the stop price, then the limit
          order becomes active.
        * If the limit order is active:
            * If the limit order was activated in this same bar and the limit price is penetrated as well, then the
              best between the stop price and the limit fill price (as described earlier) is used.
            * If the limit order was activated at a previous bar then the limit fill price (as described earlier)
              is used.

    .. note::
        * This is the default strategy used by the Broker.
        * It uses :class:`wk_pyalgotrade.broker.slippage.NoSlippage` slippage model by default.
        * If volumeLimit is 0.25, and a certain bar's volume is 100, then no more than 25 shares can be used by all
          orders that get processed at that bar.
        * If using trade bars, then all the volume from that bar can be used.
    """
    LOGGER_NAME = "fillStrategy"
    def __init__(self, volumeLimit=0.25, trade_rule=TradeRule.T1):
        super(DefaultStrategy, self).__init__()
        self.__volumeLeft = {}
        self.__volumeUsed = {}
        self.__trade_rule = trade_rule
        
        """
        此处不设置成交量限制
        """
        self.setVolumeLimit(volumeLimit)
        #self.__volumeLimit = None
        
        """
        默认不开启滑点处理
        """
        self.setSlippageModel(slippage.NoSlippage())
        #self.setSlippageModel(slippage.VolumeShareSlippage())
        
        
        """
        记录当天开盘时持仓量，适用于T+1规则
        """
        self.__volume_at_begin = {}  # 开盘持仓量
        self.__buy_at_bar = {}  # 个股当日的卖出量
        self.__sell_at_bar = {}      # 个股当日的卖出量
        self.__logger = logger.getLogger(self.LOGGER_NAME, disable=True)
        
    """
    fillStrategy的onBars每天触发一次，需要更新volumeLeft,volumeBegin
    A股成交量的单位为手，处理时需要乘以100   chenxiangdong 20170719
    volume保留到小数点后两位，需要做整百处理
    """
    def onBars(self, broker_, bars):
        volumeLeft = {}
        volumeBegin = {}
        
        for instrument in bars.getInstruments():
            bar = bars[instrument]
            # Reset the volume available for each instrument.
            #if bar.getFrequency() == wk_pyalgotrade.bar.Frequency.TRADE:
            if bar.getFrequency() == Frequency.TRADE:
                volumeLeft[instrument] = bar.getVolume() * 100
            elif self.__volumeLimit is not None:
                # We can't round here because there is no order to request the instrument traits.
                """
                最大可成交数量计算
                """
                volumeLeft[instrument] = bar.getVolume() * self.__volumeLimit * 100
                          
                """
                进行整百处理, chenxiangdong, 20170723
                """
                volumeLeft[instrument] = round_100_shares(volumeLeft[instrument])
                #self.__logger.info("%s input volume is %s, real useful volume is %s" %(instrument,bar.getVolume()*100,volumeLeft[instrument]))
               
            # Reset the volume used for each instrument.
            self.__volumeUsed[instrument] = 0.0
                                         
            """
            每天开盘时持仓量的更新
            """
            volumeBegin[instrument] = broker_.getPositions().get(instrument)


            #volumeBegin[instrument] = copy.deepcopy(broker_.getPositions().get(instrument))
            if volumeBegin[instrument] == None:
                volumeBegin[instrument] = 0
            #self.__logger.info("%s, original volume can use to sell today is %s" %(instrument, volumeBegin[instrument]))
            
        self.__volumeLeft = volumeLeft
        self.__volume_at_begin = copy.deepcopy(volumeBegin)

        if self.__trade_rule != TradeRule.NEVER:
            self.__sell_at_bar = {}
            self.__buy_at_bar = {}

    @property
    def sell_volume(self):
        return self.__sell_at_bar

    @sell_volume.setter
    def sell_volume(self, value):
        self.__sell_at_bar = value

    @property
    def buy_volume(self):
        return self.__buy_at_bar

    @buy_volume.setter
    def buy_volume(self, value):
        self.__buy_at_bar = value

    def getVolumeLeft(self):
        return self.__volumeLeft

    def getVolumeUsed(self):
        return self.__volumeUsed
    
    #新增
    def getVolumeBegin(self):
        return self.__volume_at_begin

    """
    在每个订单完成时，需要更新剩余可成交量   
    
    chenxiangdong 20170719
    """
    def onOrderFilled(self, broker_, order):
        # Update the volume left.
        if self.__volumeLimit is not None:
            # We round the volume left here becuase it was not rounded when it was initialized.
            volumeLeft = order.getInstrumentTraits().roundQuantity(self.__volumeLeft[order.getInstrument()])
            fillQuantity = order.getExecutionInfo().getQuantity()
            assert volumeLeft >= fillQuantity, \
                "Invalid fill quantity %s. Not enough volume left %s" % (fillQuantity, volumeLeft)
            self.__volumeLeft[order.getInstrument()] = order.getInstrumentTraits().roundQuantity(
                volumeLeft - fillQuantity
            )
            
            self.__logger.info('%s volume left is %f' %(order.getInstrument(),self.__volumeLeft[order.getInstrument()]))

        # Update the volume used.
        self.__volumeUsed[order.getInstrument()] = order.getInstrumentTraits().roundQuantity(
            self.__volumeUsed[order.getInstrument()] + order.getExecutionInfo().getQuantity()
        )
        self.update_cum_trans_at_bar(order)

    def update_cum_trans_at_bar(self, order):
        instrument = order.getInstrument()
        fill_quantity = order.getExecutionInfo().getQuantity()
        if order.isBuy():
            current_volume = self.__buy_at_bar.get(instrument, 0)
            self.__buy_at_bar[instrument] = current_volume + fill_quantity
        else:
            current_volume = self.__sell_at_bar.get(instrument, 0)
            self.__sell_at_bar[instrument] = current_volume + fill_quantity

    def setVolumeLimit(self, volumeLimit):
        """
        Set the volume limit.

        :param volumeLimit: The proportion of the volume that orders can take up in a bar. Must be > 0 and <= 1.
            If None, then volume limit is not checked.
        :type volumeLimit: float
        """

        if volumeLimit is not None:
            assert volumeLimit > 0 and volumeLimit <= 1, "Invalid volume limit"
        self.__volumeLimit = volumeLimit

    def setSlippageModel(self, slippageModel):
        """
        Set the slippage model to use.

        :param slippageModel: The slippage model.
        :type slippageModel: :class:`wk_pyalgotrade.broker.slippage.SlippageModel`
        """

        self.__slippageModel = slippageModel

    """
    计算可成功成交的数量
    
    chenxiangdong 20170719
    """
    def __calculateFillSize(self, broker_, order, bar):
        ret = 0
        instrument = order.getInstrument()
        msg = FillInfoTag.NORMAL
        # If self.__volumeLimit is None then allow all the order to get filled.
        """
        没有设置volumeLimit时，让订单全部成交
        """
        if self.__volumeLimit is not None:
            maxVolume = self.__volumeLeft.get(order.getInstrument(), 0)
            maxVolume = order.getInstrumentTraits().roundQuantity(maxVolume)
        else:
            maxVolume = order.getRemaining()

        self.__logger.info("instrument %s max Volume is %f, order stocks remaining to deal is %f" \
                           % (order.getInstrument(), maxVolume, order.getRemaining()))

       
        """
        下单类型为允许部分成交，成交量为订单量与交易量的最小值
        下单类型为不允许部分成交，当量足够时成交订单量，量不足时成交失败
        """
        if order.getAllOrNone():  # 不允许部分成交
            if order.getRemaining() > maxVolume:  # 订单量大于可交易量
                ret = 0
            else:
                ret = order.getRemaining()
        else:  # 允许部分成交但
            ret = min(order.getRemaining(), maxVolume)
        if ret == 0:
            msg = FillInfoTag.NOT_ALLOW_PARTIAL

        # T+1规则检查
        # 如果是卖出的话，不能超出当天原有的持有量
        # 需要检查
        if order.isSell() and self.__trade_rule == TradeRule.T1:
            remaining_volume = self.__volume_at_begin.get(instrument, 0) - self.__sell_at_bar.get(instrument, 0)
            if order.getAllOrNone() and remaining_volume < ret:
                ret = 0
            else:
                ret = min(ret, remaining_volume)
            if ret < 0:
                print(ret, instrument)
            assert ret >= 0
            if ret == 0:
                msg = FillInfoTag.NOT_ALLOW_T0 if msg == FillInfoTag.NORMAL else FillInfoTag.NOT_ALLOW_T0_PARTIAL

            #
            # ret = min(ret, self.__volumeBegin.get())
            # self.__volumeBegin[order.getInstrument()] -= ret
        return ret, msg

    def fillMarketOrder(self, broker_, order, bar):
        # Calculate the fill size for the order.
        
       
        self.__logger.info("call fillMarketOrder")
        
        """
        计算可以成功成交的数量
        """
        fillSize, msg = self.__calculateFillSize(broker_, order, bar)
        self.__logger.info("%s trade fillSize is %d" %(order.getInstrument(),fillSize))
      
        """
        当天成交量不足以使订单成交
        """
        if fillSize == 0:
          
            broker_.getLogger().warning(
                "Not enough volume to fill %s market order [%s] for %s share/s" % (
                    order.getInstrument(),
                    order.getId(),
                    order.getRemaining()
                )
            )
            
            return FillInfo(0, 0, msg)
        
        
        """
        使用收盘价格
        """
        # Unless its a fill-on-close order, use the open price.
        if order.getFillOnClose():
            
            logger.getLogger().info("use close price")
            price = bar.getClose(broker_.getUseAdjustedValues())
            
            """
            logger.getLogger().info("price get is %f" %(price))
            """
            
        else:       #使用开盘价格
          
            logger.getLogger().info("use open price")
            # price = bar.getOpen(broker_.getUseAdjustedValues())

            price = bar.get_price(broker_.price_type, broker_.getUseAdjustedValues())
            
            """
            logger.getLogger().info("price get is %f" %(price))
            """
        assert price is not None

        """
        将价格经过滑点处理
        """
        # Don't slip prices when the bar represents the trading activity of a single trade.
        self.__logger.info("price before slippage is %f" %(price))
        if bar.getFrequency() != Frequency.TRADE:
            #print 'begin calc slippage'
            price = self.__slippageModel.calculatePrice(
                order, price, fillSize, bar, self.__volumeUsed[order.getInstrument()]
            )
        self.__logger.info("price after slippage is %f" %(price))
        return FillInfo(price, fillSize)

    def fillLimitOrder(self, broker_, order, bar):
        # Calculate the fill size for the order.
        fillSize, msg = self.__calculateFillSize(broker_, order, bar)
        if fillSize == 0:
           
            broker_.getLogger().warning("Not enough volume to fill %s limit order [%s] for %s share/s" % (
                order.getInstrument(), order.getId(), order.getRemaining())
            )
           
            # return None
            return FillInfo(0, 0, msg)

        ret = None
        
        self.__logger.info("price before limit is %f" %(order.getLimitPrice()))
        if order.in_bar:
            price = get_limit_price_in_bar_trigger(order.getAction(), order.getLimitPrice(), broker_.getUseAdjustedValues(), bar)
        else:
            price = get_limit_price_trigger(order.getAction(), order.getLimitPrice(), broker_.getUseAdjustedValues(), bar)
       
        if price is not None:
            
            self.__logger.info("price after limit is %f" %(price))
            ret = FillInfo(price, fillSize)
        else:
            self.__logger.info("after limit check, the price is illegal")
            ret = FillInfo(0, 0, FillInfoTag.INVALID_LIMIT_PRICE)
        return ret

    def fillStopOrder(self, broker_, order, bar):
        ret = None

        # First check if the stop price was hit so the market order becomes active.
        stopPriceTrigger = None
        if not order.getStopHit():
            stopPriceTrigger = get_stop_price_trigger(
                order.getAction(),
                order.getStopPrice(),
                broker_.getUseAdjustedValues(),
                bar
            )
            order.setStopHit(stopPriceTrigger is not None)

        # If the stop price was hit, check if we can fill the market order.
        if order.getStopHit():
            # Calculate the fill size for the order.
            fillSize = self.__calculateFillSize(broker_, order, bar)
            if fillSize == 0:
                """
                broker_.getLogger().debug("Not enough volume to fill %s stop order [%s] for %s share/s" % (
                    order.getInstrument(),
                    order.getId(),
                    order.getRemaining()
                ))
                """
                return None

            # If we just hit the stop price we'll use it as the fill price.
            # For the remaining bars we'll use the open price.
            if stopPriceTrigger is not None:
                price = stopPriceTrigger
            else:
                price = bar.getOpen(broker_.getUseAdjustedValues())
            assert price is not None

            # Don't slip prices when the bar represents the trading activity of a single trade.
            if bar.getFrequency() != Frequency.TRADE:
                price = self.__slippageModel.calculatePrice(
                    order, price, fillSize, bar, self.__volumeUsed[order.getInstrument()]
                )
            ret = FillInfo(price, fillSize)
        return ret

    def fillStopLimitOrder(self, broker_, order, bar):
        ret = None

        # First check if the stop price was hit so the limit order becomes active.
        stopPriceTrigger = None
        if not order.getStopHit():
            stopPriceTrigger = get_stop_price_trigger(
                order.getAction(),
                order.getStopPrice(),
                broker_.getUseAdjustedValues(),
                bar
            )
            order.setStopHit(stopPriceTrigger is not None)

        # If the stop price was hit, check if we can fill the limit order.
        if order.getStopHit():
            # Calculate the fill size for the order.
            fillSize = self.__calculateFillSize(broker_, order, bar)
            if fillSize == 0:
                """
                broker_.getLogger().debug("Not enough volume to fill %s stop limit order [%s] for %s share/s" % (
                    order.getInstrument(),
                    order.getId(),
                    order.getRemaining()
                ))
                """
                return None

            price = get_limit_price_trigger(
                order.getAction(),
                order.getLimitPrice(),
                broker_.getUseAdjustedValues(),
                bar
            )
            if price is not None:
                # If we just hit the stop price, we need to make additional checks.
                if stopPriceTrigger is not None:
                    if order.isBuy():
                        # If the stop price triggered is lower than the limit price, then use that one.
                        # Else use the limit price.
                        price = min(stopPriceTrigger, order.getLimitPrice())
                    else:
                        # If the stop price triggered is greater than the limit price, then use that one.
                        # Else use the limit price.
                        price = max(stopPriceTrigger, order.getLimitPrice())

                ret = FillInfo(price, fillSize)

        return ret



class CommonFillStrategyV2(CommonFillStrategy):
    LOGGER_NAME = "fillStrategy"

    def __init__(self, volumeLimit=0.25, trade_rule=TradeRule.T1):
        super(DefaultStrategy, self).__init__()
        self.__volumeLeft = {}
        self.__volumeUsed = {}
        self.__trade_rule = trade_rule

        """
        此处不设置成交量限制
        """
        self.setVolumeLimit(volumeLimit)
        # self.__volumeLimit = None

        """
        默认不开启滑点处理
        """
        self.setSlippageModel(slippage.NoSlippage())
        # self.setSlippageModel(slippage.VolumeShareSlippage())

        """
        记录当天开盘时持仓量，适用于T+1规则
        """
        self.__volume_at_begin = {}  # 开盘持仓量
        self.__buy_at_bar = {}  # 个股当日的卖出量
        self.__sell_at_bar = {}  # 个股当日的卖出量
        self.__logger = logger.getLogger(self.LOGGER_NAME, disable=True)

    """
    fillStrategy的onBars每天触发一次，需要更新volumeLeft,volumeBegin
    A股成交量的单位为手，处理时需要乘以100   chenxiangdong 20170719
    volume保留到小数点后两位，需要做整百处理
    """

    def onBars(self, broker_, bars):
        volumeLeft = {}
        volumeBegin = {}

        for instrument in bars.getInstruments():
            bar = bars[instrument]
            # Reset the volume available for each instrument.
            # if bar.getFrequency() == wk_pyalgotrade.bar.Frequency.TRADE:
            if bar.getFrequency() == Frequency.TRADE:
                volumeLeft[instrument] = bar.getVolume() * 100
            elif self.__volumeLimit is not None:
                # We can't round here because there is no order to request the instrument traits.
                """
                最大可成交数量计算
                """
                volumeLeft[instrument] = bar.getVolume() * self.__volumeLimit * 100

                """
                进行整百处理, chenxiangdong, 20170723
                """
                volumeLeft[instrument] = round_100_shares(volumeLeft[instrument])
                # self.__logger.info("%s input volume is %s, real useful volume is %s" %(instrument,bar.getVolume()*100,volumeLeft[instrument]))

            # Reset the volume used for each instrument.
            self.__volumeUsed[instrument] = 0.0

            """
            每天开盘时持仓量的更新
            """
            # volumeBegin[instrument] = broker_.getPositions().get(instrument)
            volumeBegin[instrument] = broker_.get_quantity(instrument)
            # volumeBegin[instrument] = copy.deepcopy(broker_.getPositions().get(instrument))
            if volumeBegin[instrument] == None:
                volumeBegin[instrument] = 0
            # self.__logger.info("%s, original volume can use to sell today is %s" %(instrument, volumeBegin[instrument]))

        self.__volumeLeft = volumeLeft
        self.__volume_at_begin = copy.deepcopy(volumeBegin)

        if self.__trade_rule != TradeRule.NEVER:
            self.__sell_at_bar = {}
            self.__buy_at_bar = {}

    @property
    def sell_volume(self):
        return self.__sell_at_bar

    @sell_volume.setter
    def sell_volume(self, value):
        self.__sell_at_bar = value

    @property
    def buy_volume(self):
        return self.__buy_at_bar

    @buy_volume.setter
    def buy_volume(self, value):
        self.__buy_at_bar = value

    def getVolumeLeft(self):
        return self.__volumeLeft

    def getVolumeUsed(self):
        return self.__volumeUsed

    # 新增
    def getVolumeBegin(self):
        return self.__volume_at_begin

    """
    在每个订单完成时，需要更新剩余可成交量   

    chenxiangdong 20170719
    """

    def onOrderFilled(self, broker_, order):
        # Update the volume left.
        if self.__volumeLimit is not None:
            # We round the volume left here becuase it was not rounded when it was initialized.
            volumeLeft = order.getInstrumentTraits().roundQuantity(self.__volumeLeft[order.getInstrument()])
            fillQuantity = order.getExecutionInfo().getQuantity()
            assert volumeLeft >= fillQuantity, \
                "Invalid fill quantity %s. Not enough volume left %s" % (fillQuantity, volumeLeft)
            self.__volumeLeft[order.getInstrument()] = order.getInstrumentTraits().roundQuantity(
                volumeLeft - fillQuantity
            )

            self.__logger.info(
                '%s volume left is %f' % (order.getInstrument(), self.__volumeLeft[order.getInstrument()]))

        # Update the volume used.
        self.__volumeUsed[order.getInstrument()] = order.getInstrumentTraits().roundQuantity(
            self.__volumeUsed[order.getInstrument()] + order.getExecutionInfo().getQuantity()
        )
        self.update_cum_trans_at_bar(order)

    def update_cum_trans_at_bar(self, order):
        instrument = order.getInstrument()
        fill_quantity = order.getExecutionInfo().getQuantity()
        if order.isBuy():
            current_volume = self.__buy_at_bar.get(instrument, 0)
            self.__buy_at_bar[instrument] = current_volume + fill_quantity
        else:
            current_volume = self.__sell_at_bar.get(instrument, 0)
            self.__sell_at_bar[instrument] = current_volume + fill_quantity

    def setVolumeLimit(self, volumeLimit):
        """
        Set the volume limit.

        :param volumeLimit: The proportion of the volume that orders can take up in a bar. Must be > 0 and <= 1.
            If None, then volume limit is not checked.
        :type volumeLimit: float
        """

        if volumeLimit is not None:
            assert volumeLimit > 0 and volumeLimit <= 1, "Invalid volume limit"
        self.__volumeLimit = volumeLimit

    def setSlippageModel(self, slippageModel):
        """
        Set the slippage model to use.

        :param slippageModel: The slippage model.
        :type slippageModel: :class:`wk_pyalgotrade.broker.slippage.SlippageModel`
        """

        self.__slippageModel = slippageModel

    """
    计算可成功成交的数量

    chenxiangdong 20170719
    """

    def __calculateFillSize(self, broker_, order, bar):
        ret = 0
        instrument = order.getInstrument()
        msg = FillInfoTag.NORMAL
        # If self.__volumeLimit is None then allow all the order to get filled.
        """
        没有设置volumeLimit时，让订单全部成交
        """
        if self.__volumeLimit is not None:
            maxVolume = self.__volumeLeft.get(order.getInstrument(), 0)
            maxVolume = order.getInstrumentTraits().roundQuantity(maxVolume)
        else:
            maxVolume = order.getRemaining()

        self.__logger.info("instrument %s max Volume is %f, order stocks remaining to deal is %f" \
                           % (order.getInstrument(), maxVolume, order.getRemaining()))

        """
        下单类型为允许部分成交，成交量为订单量与交易量的最小值
        下单类型为不允许部分成交，当量足够时成交订单量，量不足时成交失败
        """
        if order.getAllOrNone():  # 不允许部分成交
            if order.getRemaining() > maxVolume:  # 订单量大于可交易量
                ret = 0
            else:
                ret = order.getRemaining()
        else:  # 允许部分成交但
            ret = min(order.getRemaining(), maxVolume)
        if ret == 0:
            msg = FillInfoTag.NOT_ALLOW_PARTIAL

        # T+1规则检查
        # 如果是卖出的话，不能超出当天原有的持有量
        # 需要检查
        if order.isSell() and self.__trade_rule == TradeRule.T1:
            remaining_volume = self.__volume_at_begin.get(instrument, 0) - self.__sell_at_bar.get(instrument, 0)
            if order.getAllOrNone() and remaining_volume < ret:
                ret = 0
            else:
                ret = min(ret, remaining_volume)
            if ret < 0:
                print(ret, instrument)
            assert ret >= 0
            if ret == 0:
                msg = FillInfoTag.NOT_ALLOW_T0 if msg == FillInfoTag.NORMAL else FillInfoTag.NOT_ALLOW_T0_PARTIAL

            #
            # ret = min(ret, self.__volumeBegin.get())
            # self.__volumeBegin[order.getInstrument()] -= ret
        return ret, msg

    def fillMarketOrder(self, broker_, order, bar):
        # Calculate the fill size for the order.

        self.__logger.info("call fillMarketOrder")

        """
        计算可以成功成交的数量
        """
        fillSize, msg = self.__calculateFillSize(broker_, order, bar)
        self.__logger.info("%s trade fillSize is %d" % (order.getInstrument(), fillSize))

        """
        当天成交量不足以使订单成交
        """
        if fillSize == 0:
            broker_.getLogger().warning(
                "Not enough volume to fill %s market order [%s] for %s share/s" % (
                    order.getInstrument(),
                    order.getId(),
                    order.getRemaining()
                )
            )

            return FillInfo(0, 0, msg)

        """
        使用收盘价格
        """
        # Unless its a fill-on-close order, use the open price.
        if order.getFillOnClose():

            logger.getLogger().info("use close price")
            price = bar.getClose(broker_.getUseAdjustedValues())

            """
            logger.getLogger().info("price get is %f" %(price))
            """

        else:  # 使用开盘价格

            logger.getLogger().info("use open price")
            # price = bar.getOpen(broker_.getUseAdjustedValues())

            price = bar.get_price(broker_.price_type, broker_.getUseAdjustedValues())

            """
            logger.getLogger().info("price get is %f" %(price))
            """
        assert price is not None

        """
        将价格经过滑点处理
        """
        # Don't slip prices when the bar represents the trading activity of a single trade.
        self.__logger.info("price before slippage is %f" % (price))
        if bar.getFrequency() != Frequency.TRADE:
            # print 'begin calc slippage'
            price = self.__slippageModel.calculatePrice(
                order, price, fillSize, bar, self.__volumeUsed[order.getInstrument()]
            )
        self.__logger.info("price after slippage is %f" % (price))
        return FillInfo(price, fillSize)

    def fillLimitOrder(self, broker_, order, bar):
        # Calculate the fill size for the order.
        fillSize, msg = self.__calculateFillSize(broker_, order, bar)
        if fillSize == 0:
            broker_.getLogger().warning("Not enough volume to fill %s limit order [%s] for %s share/s" % (
                order.getInstrument(), order.getId(), order.getRemaining())
                                        )

            # return None
            return FillInfo(0, 0, msg)

        ret = None

        self.__logger.info("price before limit is %f" % (order.getLimitPrice()))
        if order.in_bar:
            price = get_limit_price_in_bar_trigger(order.getAction(), order.getLimitPrice(),
                                                   broker_.getUseAdjustedValues(), bar)
        else:
            price = get_limit_price_trigger(order.getAction(), order.getLimitPrice(), broker_.getUseAdjustedValues(),
                                            bar)

        if price is not None:

            self.__logger.info("price after limit is %f" % (price))
            ret = FillInfo(price, fillSize)
        else:
            self.__logger.info("after limit check, the price is illegal")
            ret = FillInfo(0, 0, FillInfoTag.INVALID_LIMIT_PRICE)
        return ret

    def fillStopOrder(self, broker_, order, bar):
        ret = None

        # First check if the stop price was hit so the market order becomes active.
        stopPriceTrigger = None
        if not order.getStopHit():
            stopPriceTrigger = get_stop_price_trigger(
                order.getAction(),
                order.getStopPrice(),
                broker_.getUseAdjustedValues(),
                bar
            )
            order.setStopHit(stopPriceTrigger is not None)

        # If the stop price was hit, check if we can fill the market order.
        if order.getStopHit():
            # Calculate the fill size for the order.
            fillSize = self.__calculateFillSize(broker_, order, bar)
            if fillSize == 0:
                """
                broker_.getLogger().debug("Not enough volume to fill %s stop order [%s] for %s share/s" % (
                    order.getInstrument(),
                    order.getId(),
                    order.getRemaining()
                ))
                """
                return None

            # If we just hit the stop price we'll use it as the fill price.
            # For the remaining bars we'll use the open price.
            if stopPriceTrigger is not None:
                price = stopPriceTrigger
            else:
                price = bar.getOpen(broker_.getUseAdjustedValues())
            assert price is not None

            # Don't slip prices when the bar represents the trading activity of a single trade.
            if bar.getFrequency() != Frequency.TRADE:
                price = self.__slippageModel.calculatePrice(
                    order, price, fillSize, bar, self.__volumeUsed[order.getInstrument()]
                )
            ret = FillInfo(price, fillSize)
        return ret

    def fillStopLimitOrder(self, broker_, order, bar):
        ret = None

        # First check if the stop price was hit so the limit order becomes active.
        stopPriceTrigger = None
        if not order.getStopHit():
            stopPriceTrigger = get_stop_price_trigger(
                order.getAction(),
                order.getStopPrice(),
                broker_.getUseAdjustedValues(),
                bar
            )
            order.setStopHit(stopPriceTrigger is not None)

        # If the stop price was hit, check if we can fill the limit order.
        if order.getStopHit():
            # Calculate the fill size for the order.
            fillSize = self.__calculateFillSize(broker_, order, bar)
            if fillSize == 0:
                """
                broker_.getLogger().debug("Not enough volume to fill %s stop limit order [%s] for %s share/s" % (
                    order.getInstrument(),
                    order.getId(),
                    order.getRemaining()
                ))
                """
                return None

            price = get_limit_price_trigger(
                order.getAction(),
                order.getLimitPrice(),
                broker_.getUseAdjustedValues(),
                bar
            )
            if price is not None:
                # If we just hit the stop price, we need to make additional checks.
                if stopPriceTrigger is not None:
                    if order.isBuy():
                        # If the stop price triggered is lower than the limit price, then use that one.
                        # Else use the limit price.
                        price = min(stopPriceTrigger, order.getLimitPrice())
                    else:
                        # If the stop price triggered is greater than the limit price, then use that one.
                        # Else use the limit price.
                        price = max(stopPriceTrigger, order.getLimitPrice())

                ret = FillInfo(price, fillSize)

        return ret


class IntraDayFillStrategy(DefaultStrategy):
    LOGGER_NAME = "IntraDayFillStrategy"

    def __init__(self, volumeLimit=0.25, trade_rule=TradeRule.NEVER):
        super(DefaultStrategy, self).__init__()
        self.__volumeLeft = {}
        self.__volumeUsed = {}
        self.__trade_rule = trade_rule

        # 此处不设置成交量限制
        self.setVolumeLimit(volumeLimit)

        # 默认不开启滑点处理
        self.setSlippageModel(slippage.NoSlippage())

        # 记录当天开盘时持仓量，适用于T+1规则
        self.__volume_at_begin = {}  # 开盘持仓量
        self.__buy_volume = {}  # 个股当日的卖出量
        self.__sell_volume = {}  # 个股当日的卖出量
        self.__logger = logger.getLogger(self.LOGGER_NAME, disable=True)

    def init_volume_at_begin(self, broker_, bars):
        volume_begin = {}
        for instrument in bars.getInstruments():
            # 每天开盘时持仓量的更新
            volume_begin[instrument] = broker_.getPositions().get(instrument, 0)
        self.__volume_at_begin = copy.deepcopy(volume_begin)

    @property
    def sell_volume(self):
        return self.__sell_volume

    @sell_volume.setter
    def sell_volume(self, value):
        self.__sell_volume = value

    @property
    def buy_volume(self):
        return self.__buy_volume

    @buy_volume.setter
    def buy_volume(self, value):
        self.__buy_volume = value

    """
    fillStrategy的onBars每天触发一次，需要更新volumeLeft,volumeBegin
    A股成交量的单位为手，处理时需要乘以100   chenxiangdong 20170719
    volume保留到小数点后两位，需要做整百处理
    """
    def onBars(self, broker_, bars):
        volume_left = {}

        for instrument in bars.getInstruments():
            bar = bars[instrument]
            # Reset the volume available for each instrument.
            # if bar.getFrequency() == wk_pyalgotrade.bar.Frequency.TRADE:
            if bar.getFrequency() == Frequency.TRADE:
                volume_left[instrument] = bar.getVolume() * 100
            elif self.__volumeLimit is not None:
                # We can't round here because there is no order to request the instrument traits.
                """
                最大可成交数量计算
                """
                volume_left[instrument] = bar.getVolume() * self.__volumeLimit * 100

                """
                进行整百处理, chenxiangdong, 20170723
                """
                volume_left[instrument] = round_100_shares(volume_left[instrument])
                # self.__logger.info("%s input volume is %s, real useful volume is %s" %(instrument,bar.getVolume()*100,volumeLeft[instrument]))

            # Reset the volume used for each instrument.
            self.__volumeUsed[instrument] = 0.0

            # 不更新volume_at_begin，因为目前不支持T0

        self.__volumeLeft = volume_left

    def getVolumeLeft(self):
        return self.__volumeLeft

    def getVolumeUsed(self):
        return self.__volumeUsed

    # 新增
    def getVolumeBegin(self):
        return self.__volume_at_begin

    """
    在每个订单完成时，需要更新剩余可成交量   
    chenxiangdong 20170719
    """
    def onOrderFilled(self, broker_, order):
        # Update the volume left.
        if self.__volumeLimit is not None:
            # We round the volume left here becuase it was not rounded when it was initialized.
            volumeLeft = order.getInstrumentTraits().roundQuantity(self.__volumeLeft[order.getInstrument()])
            fillQuantity = order.getExecutionInfo().getQuantity()
            assert volumeLeft >= fillQuantity, \
                "Invalid fill quantity %s. Not enough volume left %s" % (fillQuantity, volumeLeft)
            self.__volumeLeft[order.getInstrument()] = order.getInstrumentTraits().roundQuantity(
                volumeLeft - fillQuantity
            )

            self.__logger.info(
                '%s volume left is %f' % (order.getInstrument(), self.__volumeLeft[order.getInstrument()]))

        # Update the volume used.
        self.__volumeUsed[order.getInstrument()] = order.getInstrumentTraits().roundQuantity(
            self.__volumeUsed[order.getInstrument()] + order.getExecutionInfo().getQuantity()
        )
        self.update_cum_trans_at_bar(order)

    def update_cum_trans_at_bar(self, order):
        instrument = order.getInstrument()
        fill_quantity = order.getExecutionInfo().getQuantity()
        if order.isBuy():
            current_volume = self.__buy_volume.get(instrument, 0)
            self.__buy_volume[instrument] = current_volume + fill_quantity
        else:
            current_volume = self.__sell_volume.get(instrument, 0)
            self.__sell_volume[instrument] = current_volume + fill_quantity

    def setVolumeLimit(self, volumeLimit):
        """
        Set the volume limit.

        :param volumeLimit: The proportion of the volume that orders can take up in a bar. Must be > 0 and <= 1.
            If None, then volume limit is not checked.
        :type volumeLimit: float
        """

        if volumeLimit is not None:
            assert volumeLimit > 0 and volumeLimit <= 1, "Invalid volume limit"
        self.__volumeLimit = volumeLimit

    def setSlippageModel(self, slippageModel):
        """
        Set the slippage model to use.

        :param slippageModel: The slippage model.
        :type slippageModel: :class:`wk_pyalgotrade.broker.slippage.SlippageModel`
        """

        self.__slippageModel = slippageModel

    """
    计算可成功成交的数量

    chenxiangdong 20170719
    """

    def __calculateFillSize(self, broker_, order, bar):
        ret = 0
        instrument = order.getInstrument()
        msg = FillInfoTag.NORMAL
        # If self.__volumeLimit is None then allow all the order to get filled.
        """
        没有设置volumeLimit时，让订单全部成交
        """
        if self.__volumeLimit is not None:
            maxVolume = self.__volumeLeft.get(order.getInstrument(), 0)
            maxVolume = order.getInstrumentTraits().roundQuantity(maxVolume)
        else:
            maxVolume = order.getRemaining()

        self.__logger.info("instrument %s max Volume is %f, order stocks remaining to deal is %f" \
                           % (order.getInstrument(), maxVolume, order.getRemaining()))

        """
        下单类型为允许部分成交，成交量为订单量与交易量的最小值
        下单类型为不允许部分成交，当量足够时成交订单量，量不足时成交失败
        """
        if order.getAllOrNone():  # 不允许部分成交
            if order.getRemaining() > maxVolume:  # 订单量大于可交易量
                ret = 0
            else:
                ret = order.getRemaining()
        else:  # 允许部分成交但
            ret = min(order.getRemaining(), maxVolume)
        if ret == 0:
            msg = FillInfoTag.NOT_ALLOW_PARTIAL

        # T+1规则检查
        # 如果是卖出的话，不能超出当天原有的持有量
        # 需要检查
        if order.isSell() and self.__trade_rule != TradeRule.T0:
            remaining_volume = self.__volume_at_begin.get(instrument, 0) - self.__sell_volume.get(instrument, 0)
            if order.getAllOrNone() and remaining_volume < ret:
                ret = 0
            else:
                ret = min(ret, remaining_volume)
            if ret < 0:
                print(ret, instrument)
            assert ret >= 0
            if ret == 0:
                msg = FillInfoTag.NOT_ALLOW_T0 if msg == FillInfoTag.NORMAL else FillInfoTag.NOT_ALLOW_T0_PARTIAL

        return ret, msg

    def fillMarketOrder(self, broker_, order, bar):
        # Calculate the fill size for the order.

        self.__logger.info("call fillMarketOrder")

        """
        计算可以成功成交的数量
        """
        fillSize, msg = self.__calculateFillSize(broker_, order, bar)
        self.__logger.info("%s trade fillSize is %d" % (order.getInstrument(), fillSize))

        """
        当天成交量不足以使订单成交
        """
        if fillSize == 0:
            broker_.getLogger().warning(
                "Not enough volume to fill %s market order [%s] for %s share/s" % (
                    order.getInstrument(),
                    order.getId(),
                    order.getRemaining()
                )
            )

            return FillInfo(0, 0, msg)

        """
        使用收盘价格
        """
        # Unless its a fill-on-close order, use the open price.
        if order.getFillOnClose():
            assert False

            logger.getLogger().info("use close price")
            price = bar.getClose(broker_.getUseAdjustedValues())

            """
            logger.getLogger().info("price get is %f" %(price))
            """

        else:  # 使用开盘价格

            logger.getLogger().info("use open price")
            # price = bar.getOpen(broker_.getUseAdjustedValues())

            price = bar.get_price(broker_.price_type, broker_.getUseAdjustedValues())

            """
            logger.getLogger().info("price get is %f" %(price))
            """

        assert price is not None

        """
        将价格经过滑点处理
        """
        # Don't slip prices when the bar represents the trading activity of a single trade.
        self.__logger.info("price before slippage is %f" % (price))
        if bar.getFrequency() != Frequency.TRADE:
            # print 'begin calc slippage'
            price = self.__slippageModel.calculatePrice(
                order, price, fillSize, bar, self.__volumeUsed[order.getInstrument()]
            )
        self.__logger.info("price after slippage is %f" % (price))
        return FillInfo(price, fillSize)

    def fillLimitOrder(self, broker_, order, bar):
        # Calculate the fill size for the order.
        fillSize = self.__calculateFillSize(broker_, order, bar)
        if fillSize == 0:
            broker_.getLogger().warning("Not enough volume to fill %s limit order [%s] for %s share/s" % (
                order.getInstrument(), order.getId(), order.getRemaining())
                                        )

            return None

        ret = None

        self.__logger.info("price before limit is %f" % (order.getLimitPrice()))
        price = get_limit_price_trigger(order.getAction(), order.getLimitPrice(), broker_.getUseAdjustedValues(), bar)

        if price is not None:

            self.__logger.info("price after limit is %f" % (price))
            ret = FillInfo(price, fillSize)
        else:
            self.__logger.info("after limit check, the price is illegal")
        return ret

    def fillStopOrder(self, broker_, order, bar):
        ret = None

        # First check if the stop price was hit so the market order becomes active.
        stopPriceTrigger = None
        if not order.getStopHit():
            stopPriceTrigger = get_stop_price_trigger(
                order.getAction(),
                order.getStopPrice(),
                broker_.getUseAdjustedValues(),
                bar
            )
            order.setStopHit(stopPriceTrigger is not None)

        # If the stop price was hit, check if we can fill the market order.
        if order.getStopHit():
            # Calculate the fill size for the order.
            fillSize = self.__calculateFillSize(broker_, order, bar)
            if fillSize == 0:
                """
                broker_.getLogger().debug("Not enough volume to fill %s stop order [%s] for %s share/s" % (
                    order.getInstrument(),
                    order.getId(),
                    order.getRemaining()
                ))
                """
                return None

            # If we just hit the stop price we'll use it as the fill price.
            # For the remaining bars we'll use the open price.
            if stopPriceTrigger is not None:
                price = stopPriceTrigger
            else:
                price = bar.getOpen(broker_.getUseAdjustedValues())
            assert price is not None

            # Don't slip prices when the bar represents the trading activity of a single trade.
            if bar.getFrequency() != Frequency.TRADE:
                price = self.__slippageModel.calculatePrice(
                    order, price, fillSize, bar, self.__volumeUsed[order.getInstrument()]
                )
            ret = FillInfo(price, fillSize)
        return ret

    def fillStopLimitOrder(self, broker_, order, bar):
        ret = None

        # First check if the stop price was hit so the limit order becomes active.
        stopPriceTrigger = None
        if not order.getStopHit():
            stopPriceTrigger = get_stop_price_trigger(
                order.getAction(),
                order.getStopPrice(),
                broker_.getUseAdjustedValues(),
                bar
            )
            order.setStopHit(stopPriceTrigger is not None)

        # If the stop price was hit, check if we can fill the limit order.
        if order.getStopHit():
            # Calculate the fill size for the order.
            fillSize = self.__calculateFillSize(broker_, order, bar)
            if fillSize == 0:
                """
                broker_.getLogger().debug("Not enough volume to fill %s stop limit order [%s] for %s share/s" % (
                    order.getInstrument(),
                    order.getId(),
                    order.getRemaining()
                ))
                """
                return None

            price = get_limit_price_trigger(
                order.getAction(),
                order.getLimitPrice(),
                broker_.getUseAdjustedValues(),
                bar
            )
            if price is not None:
                # If we just hit the stop price, we need to make additional checks.
                if stopPriceTrigger is not None:
                    if order.isBuy():
                        # If the stop price triggered is lower than the limit price, then use that one.
                        # Else use the limit price.
                        price = min(stopPriceTrigger, order.getLimitPrice())
                    else:
                        # If the stop price triggered is greater than the limit price, then use that one.
                        # Else use the limit price.
                        price = max(stopPriceTrigger, order.getLimitPrice())

                ret = FillInfo(price, fillSize)

        return ret


class FutureStrategy(DefaultStrategy):
    LOGGER_NAME = "future_fillStrategy"

    def __init__(self, volume_limit=0.25):
        super().__init__(volume_limit)

    def fillMarketOrder(self, broker_, order, bar):
        """
        简单处理，允许全部成交
        """
        price = bar.get_price(broker_.price_type)
        return FillInfo(price, order.getRemaining())
