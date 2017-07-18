#coding:utf-8

import sys
sys.path.append("..")
import os

import torndb
from common.mysql_conf_api import MySQLConfigApi
import datetime
import pickle
import traceback
import argparse
from common.utils import *

logger = logging.getLogger("info") 


def dump_user_id():
    cmd = "rm -rf data/user_id.txt"
    os.system(cmd)
    #取最近三个月下过订单的用户作为活跃用户
    today = datetime.datetime.today()
    date_begin = (today - datetime.timedelta(days=90)).strftime('%Y-%m-%d')
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
def dump_user_order_goods_id(periods):

    if periods == "long" :
        days = 210
    elif periods == "middle" :
        days = 90
    elif periods == "short":
        days = 30
    else:
        return -1

    logger.info("begin dump user " + periods + " action info")
    #获取用户下订单的宝贝，计算长中短期偏好
    cmd = "rm -rf ./data/user_order_goodsid_" + periods + ".txt"
    os.system(cmd)
    today = datetime.datetime.today()
    date_begin = (today - datetime.timedelta(days=days)).strftime('%Y-%m-%d')
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
        with open('data/user_action_goodsid.txt', 'w') as f:
            for uid in user_id:
                sql = "select order_id, goods_id, order_ctime from (select o.order_id as order_id ,goods_id as goods_id, order_ctime from t_pandora_order o left join t_pandora_order_item i on o.order_id = i.order_id where o.buyer_id=%s and o.order_ctime >= '%s' and o.order_ctime <= '%s' and o.order_status=2) t" % (uid, date_begin, date_end)
                res = db_order.query(sql)
                for line in res:
                    if line['goods_id'] == None:
                        continue
                    line['uid'] = uid
                    line['order_ctime'] = line['order_ctime'].strftime('%Y-%m-%d')
                    ret.append(line)
                    f.write("%s\t%s\t%s\t%s\torder\n" % (uid, line['order_id'], line['goods_id'], line['order_ctime']))
                    
    except Exception,e:
        print e
        print traceback.print_exc()
    finally:
        db_order.close()

    logger.info("dump user action info success")
    return

def dump_user_cart_goods_id(periods):
    return

def dump_user_favorite_goods_id(periods):
    return


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p','--periods', help="画像类型，长中短 long|middle|short")
    args = parser.parse_args()  
    if args.periods == None:
        print "解析失败"
        return
    if args.periods not in ['long', 'middle', 'short']:
        print "periods解析失败"
        return
    #dump_user_id()
    dump_user_order_goods_id(args.periods)
    dump_user_cart_goods_id(args.periods)
    dump_user_favorite_goods_id(args.periods)
    return
        

if __name__ == "__main__":
    main()
