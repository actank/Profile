#coding:utf-8

import sys
sys.path.append("..")

import torndb
from common.mysql_conf_api import MySQLConfigApi
import datetime
import pickle

def dump_user_id():
    #取最近三个月下过订单的用户作为活跃用户
    today = datetime.datetime.today()
    date_begin = (today - datetime.timedelta(days=90)).strftime('%Y-%m-%d')
    date_end = today.strftime('%Y-%m-%d')

    host, port, user, pwd, db = MySQLConfigApi.get_param_from_ini_file('higo_order', 0, False)
    db = torndb.Connection(host + ':' + port, db, user, pwd)
    try:
        sql = "select distinct buyer_id from t_pandora_order where order_ctime >= '%s' and order_ctime <= '%s'" % (date_begin, date_end)
        res = db.query(sql)
        with open("data/user_id.txt", 'w') as f:
            pickle.dump(res, f)
    except Exception, e:
        print e
    finally:
        db.close()
    return
    

def dump_user_click_goods_id():
    return

def dump_user_add_cart_goods_id():
    return

def dump_user_order_goods_id():
    today = datetime.datetime.today()
    date_begin = (today - datetime.timedelta(days=90)).strftime('%Y-%m-%d')
    date_end = today.strftime('%Y-%m-%d')

    host, port, user, pwd, db = MySQLConfigApi.get_param_from_ini_file('higo_order', 0, False)
    db_order = torndb.Connection(host + ':' + port, db, user, pwd)
    user_id = {}
    with open('data/user_id.txt', 'r') as f:
        user_id = pickle.load(f)
    try:
        pass
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
