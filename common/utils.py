#coding:utf-8


import math
import datetime
import logging
import logging.config
logging.config.fileConfig("../common/log.conf")


#牛顿冷却系数
def cal_time_decay(a, begin_time):
    now = datetime.datetime.today()
    delta = round((now - begin_time).total_seconds() / 86400)
    return 1 * math.exp(-float(a) * delta)

