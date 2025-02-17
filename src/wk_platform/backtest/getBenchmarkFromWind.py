# -*- coding: utf-8 -*-
"""
Created on Wed Jul 05 09:24:42 2017

@author: cxd

comment: 从 WIND上获取指数数据
"""

import pandas as pd
from WindPy import w
import time


class benchmarkIndex():
    def getCurrentTime(self):
        return time.strftime('[%Y-%m-%d %H:%M:%S]', time.localtime(time.time()))
    
    def getBenchmarkHSAll(self,symbols, start_date, end_date):
        """
        获取上证综指
        """
        for symbol in symbols:
            
            try:
                stock = w.wsd(symbol, "open,high,low,close,volume,pct_chg", start_date, end_date, "PriceAdj=F" )
                index_data = pd.DataFrame()
                
                #print stock.Times
                
                timeList = []
                for t in stock.Times:
                    timeList.append(t.strftime("%Y-%m-%d"))
                
                #print timeList
                
                index_data['Date'] = timeList
                index_data['Open'] = stock.Data[0]
                index_data['High'] = stock.Data[1]
                index_data['Low'] = stock.Data[2]
                index_data['Close'] = stock.Data[3]
                index_data['Volume'] = stock.Data[4]
                index_data['pct_chg'] = stock.Data[5]
                
                index_data.to_csv("benchFile/dataFileBenchmark/" + str(symbol) +".csv", index = False, encoding = "gbk")
                
            except Exception as e:
                print("Exception: %s" %(e))
    
    """
    def getBenchmarkSH50(self, symbol, start_date, end_date):
   
        pass
    
    def getBenchmarkHS300(self):
    
        pass
    def getBenchmarkZZ500(self):
     
        pass
    def getBenchmarkCYB(self):
        pass
    """

def main():
    
    """
    SHAllIndex = "000001.SH"
    SH50Index = "000016.SH"
    HS300Index = "000300.SH"
    ZZ500Index = "399905.SZ"
    CYBIndex = "399006.SZ"
    """
    
    
    indexList = ["000001.SH","000016.SH", "000300.SH","399905.SZ","399006.SZ", "000903.SH","000010.SH"]
    #indexList = ["000001.SH","000016.SH", "000300.SH","399006.SZ"]
    start_date = "20000701"
    end_date = "20180126"
    
    w.start()
    
    benchmark = benchmarkIndex()
    benchmark.getBenchmarkHSAll(indexList, start_date, end_date)
    
    w.stop()


if __name__ == "__main__":
    main()
    
    
    
    
    
    
    
    