#coding:utf-8
import sys
reload(sys)
sys.path.append("..")
sys.setdefaultencoding( "utf-8" )
import torndb
from common.mysql_conf_api import MySQLConfigApi
import datetime
import pickle
import logging
import traceback
import gc
import os
from utils import *


def get_goods_lv2_category_info():
    #获取用户下过单的宝贝id
    cmd = "rm -rf data/user_n_category_info.txt"
    os.system(cmd)
    user_order_goodsid_list = []
    with open("data/user_order_goodsid.txt", "r") as f:
        user_order_goodsid_list = pickle.load(f)
    #扩展三级类目信息
    host, port, user, pwd, db = MySQLConfigApi.get_param_from_ini_file('higo_goods', 0, False)
    db = torndb.Connection(host + ':' + port, db, user, pwd)
    f = open("data/user_n_category_info.txt", "w")
    try:
        for goods in user_order_goodsid_list:
            if goods['goods_id'] == None:
                continue
            sql = "select n_category_id, n_category_name from t_pandora_goods where goods_id=%s" % (goods['goods_id'])
            res = db.query(sql)
            if len(res) == 0:
                continue
            goods['n_category_id'] = res[0]['n_category_id']
            goods['n_category_name'] = res[0]['n_category_name']
            #action = order
            f.write("%s\t%s\t%s\t%s\torder\t%s\n" % (goods['uid'], str(goods['goods_id']), str(goods['n_category_id']), goods['n_category_name'], goods['order_ctime']))
    except Exception, e:
        print e
        print traceback.print_exc()
    finally:
        f.close()
        db.close()

    return

#计算三级类目偏好
#preference_weight = action_weight * time_weight * goods_weight
def cal_user_ncategory_preference():
    action_weitht = {
    'click_weight' : 1,
    'like_action_weight': 2,
    'cart_action_weight' : 5,
    'order_action_weight' : 10 
    }

    uid_n_category_id = {}
    n_category_id_2_name_map = {}
    sum_user_n_category_id_action_num = {}
    f1 = open("data/user_n_categoy_preference.txt", "w")
    with open("data/user_n_category_info.txt") as f:
        for line in f:
            uid, goods_id, n_category_id, n_category_name, action,order_ctime = line.strip().split("\t")
            if n_category_id not in n_category_id_2_name_map:
                n_category_id_2_name_map[n_category_id] = n_category_name
            if uid not in sum_user_n_category_id_action_num:
                sum_user_n_category_id_action_num[uid][n_category_id] = 1 * action_weight[action] * cal_time_decay(datetime.datetime.strptime(order_ctime))
                
    f1.close()
def main():
    
    get_goods_lv2_category_info()
    gc.enable()
    gc.collect()
    gc.disable()
    #cal_user_ncategory_preference()
    return

if __name__ == "__main__":
    main()
