#coding:utf-8

import sys
sys.path.append("..")
import os

import torndb
from common.mysql_conf_api import MySQLConfigApi
import datetime
import pickle
import logging

def dump_user_id():
    cmd = "rm -rf data/user_id.txt"
    os.system(cmd)
    #取最近三个月下过订单的用户作为活跃用户
    today = datetime.datetime.today()
    #暂时用10天下过订单的用户做小规模测试
    date_begin = (today - datetime.timedelta(days=10)).strftime('%Y-%m-%d')
    date_end = today.strftime('%Y-%m-%d')

    host, port, user, pwd, db = MySQLConfigApi.get_param_from_ini_file('higo_order', 0, False)
    db = torndb.Connection(host + ':' + port, db, user, pwd)
    try:
        sql = "select distinct buyer_id from t_pandora_order where order_ctime >= '%s' and order_ctime <= '%s'" % (date_begin, date_end)
        res = db.query(sql)
        user_id_list = list()
        for item in res:
            user_id_list.append(str(item['buyer_id']))
        with open("data/user_id.txt", 'w') as f:
            for uid in user_id_list:
                f.write(uid + "\n")
    except Exception, e:
        print e
    finally:
        db.close()
    return
    

def dump_user_click_goods_id():
    return

def dump_user_add_cart_goods_id():
    return

# 获取中期偏好宝贝goods_id
def dump_user_order_goods_id():
    #获取用户三个月内下订单的宝贝，计算中期偏好
    cmd = "rm -rf ./data/user_order_goodsid.txt"
    os.system(cmd)
    today = datetime.datetime.today()
    date_begin = (today - datetime.timedelta(days=90)).strftime('%Y-%m-%d')
    date_end = today.strftime('%Y-%m-%d')

    host, port, user, pwd, db = MySQLConfigApi.get_param_from_ini_file('higo_order', 0, False)
    db_order = torndb.Connection(host + ':' + port, db, user, pwd)
    user_id = []
    with open('data/user_id.txt', 'r') as f:
        for line in f.readlines():
            line = line.strip()
            user_id.append(line)
    try:
        ret = []
        with open('data/user_order_goodsid.txt', 'w') as f:
            for uid in user_id:
                sql = "select order_id, goods_id, order_ctime from (select o.order_id as order_id ,goods_id as goods_id, order_ctime from t_pandora_order o left join t_pandora_order_item i on o.order_id = i.order_id where o.buyer_id=%s and o.order_ctime >= '%s' and o.order_ctime <= '%s') t" % (uid, date_begin, date_end)
                res = db_order.query(sql)
                for line in res:
                    if line['goods_id'] == None:
                        continue
                    line['uid'] = uid
                    line['order_ctime'] = line['order_ctime'].strftime('%Y-%m-%d')
                    ret.append(line)
            pickle.dump(ret, f, 1)
                    
    except Exception,e:
        print e
    finally:
        db_order.close()
    return

def dump_user_pay_goods_id():
    return

def dump_user_favorite_goods_id():
    return


def main():
    #dump_user_id()
    dump_user_order_goods_id()
        

if __name__ == "__main__":
    main()
