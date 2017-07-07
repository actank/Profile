#coding:utf-8
import sys
sys.path.append("..")
import torndb
from common.mysql_conf_api import MySQLConfigApi
import datetime
import pickle
import logging
import traceback
import gc


def get_goods_lv2_category_info():
    #获取用户下过单的宝贝id
    user_order_goodsid_list = []
    with open("data/user_order_goodsid.txt", "r") as f:
        user_order_goodsid_list = pickle.load(f)
    #扩展三级类目信息
    host, port, user, pwd, db = MySQLConfigApi.get_param_from_ini_file('higo_goods', 0, False)
    db = torndb.Connection(host + ':' + port, db, user, pwd)
    f = open("data/user_n_category_info.txt", "w")
    try:
        for goods in user_order_goodsid_list:
            sql = "select n_category_id, n_category_name from t_pandora_goods where goods_id=%s" % (goods['goods_id'])
            res = db.query(sql)
            if len(res) == 0:
                continue
            goods['n_category_id'] = res[0]['n_category_id']
            goods['n_category_name'] = res[0]['n_category_name']
            f.write("%s\t%s\t%s\t%s\t%s\n" % (goods['uid'], goods['goods_id'], goods['n_categoy_id'], goods['n_category_name']), goods['order_ctime'])

    except Exception, e:
        print e
        print traceback.print_exc()
    finally:
        f.close()
        db.close()

    return

def cal_user_ncategory_preference():
    uid_n_category_id = {}
    n_category_id_2_name_map = {}
    sum_n_category_id_action_num = {}
    f1 = open("data/user_n_categoy_preference.txt", "w")
    with open("data/user_n_category_info.txt") as f:
        for line in f:
            uid, goods_id, n_category_id, n_category_name, order_ctime = line.strip().split("\t")
            print uid, goods_id, n_categoy_id, n_category_name, order_ctime
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
