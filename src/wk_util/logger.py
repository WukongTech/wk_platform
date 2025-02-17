# -*- coding: utf-8 -*-
# PyAlgoTrade
#
# Copyright 2011-2015 Gabriel Martin Becedillas Ruiz
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
from __future__ import annotations

import logging
import threading
import datetime
import pytz
from logging import Logger

initLock = threading.Lock()
rootLoggerInitialized = False

log_format = "%(asctime)s %(name)s [%(levelname)s] %(filename)s %(funcName)s %(lineno)d %(message)s"
level = logging.INFO


FILE_LOG: str | None = None  # "dataFile/result/log1.txt"
CONSOLE_LOG = False
SHOW_RUNTIME_INFO = True

class DummyLogger(logging.Logger):
    
    def debug(self, msg, *args, **kwargs):
        """
        Delegate a debug call to the underlying logger, after adding
        contextual information from this adapter instance.
        """
        pass

    def info(self, msg, *args, **kwargs):
        """
        Delegate an info call to the underlying logger, after adding
        contextual information from this adapter instance.
        """
        pass

    def warning(self, msg, *args, **kwargs):
        """
        Delegate a warning call to the underlying logger, after adding
        contextual information from this adapter instance.
        """
        pass

    def error(self, msg, *args, **kwargs):
        """
        Delegate an error call to the underlying logger, after adding
        contextual information from this adapter instance.
        """
        pass

    def exception(self, msg, *args, **kwargs):
        """
        Delegate an exception call to the underlying logger, after adding
        contextual information from this adapter instance.
        """
        pass

    def critical(self, msg, *args, **kwargs):
        """
        Delegate a critical call to the underlying logger, after adding
        contextual information from this adapter instance.
        """
        pass

    def log(self, level, msg, *args, **kwargs):
        """
        Delegate a log call to the underlying logger, after adding
        contextual information from this adapter instance.
        """
        pass

    def setLevel(self, level=None):
        pass


def init_handler(handler):
    handler.setFormatter(Formatter(log_format))


def init_logger(logger):
    

    #global level
    logger.setLevel(level)
    logging.basicConfig(level=logging.DEBUG)

    if FILE_LOG is not None:
        fileHandler = logging.FileHandler(FILE_LOG)
        init_handler(fileHandler)
        logger.addHandler(fileHandler)

    if CONSOLE_LOG:
        consoleHandler = logging.StreamHandler()
        init_handler(consoleHandler)
        logger.addHandler(consoleHandler)


def initialize(): #添加参数
    
    global rootLoggerInitialized
    with initLock:
        if not rootLoggerInitialized:                #初始化root类型
            init_logger(logging.getLogger())
            rootLoggerInitialized = True
 

def getLogger(name=None, disable=True):
    
    if disable:
        # logging.setLoggerClass(DummyLogger)
        return DummyLogger(name)
    else:
        initialize()
        return logging.getLogger(name)
   
    


# This formatter provides a way to hook in formatTime.
class Formatter(logging.Formatter):
    DATETIME_HOOK = None

    def formatTime(self, record, datefmt=None):
        newDateTime = None

        if Formatter.DATETIME_HOOK is not None:
            newDateTime = Formatter.DATETIME_HOOK()

        if newDateTime is None:
            ret = super(Formatter, self).formatTime(record, datefmt)
        else:
            ret = str(newDateTime)
        return ret


def log_time():
    return datetime.datetime.now(pytz.timezone('PRC')).strftime("%Y-%m-%d %H:%M:%S")


def console_log(*args):
    if not SHOW_RUNTIME_INFO:
        return
    time_str = f"[{log_time()}]"
    print(time_str, *args)


def show_runtime_info(flag: bool):
    global SHOW_RUNTIME_INFO
    SHOW_RUNTIME_INFO = flag


