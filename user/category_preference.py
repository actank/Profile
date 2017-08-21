#coding:utf-8
import sys
reload(sys)
sys.path.append("..")
sys.setdefaultencoding( "utf-8" )
import torndb
from common.mysql_conf_api import MySQLConfigApi
import datetime
import pickle
import traceback
import gc
import os
from common.utils import *
import argparse
logger = logging.getLogger("info") 


def get_goods_lv3_category_info():
    #获取用户下过单的宝贝id
    cmd = "rm -rf data/user_n_category_info.txt"
    os.system(cmd)
    user_order_goodsid_list = []
    with open("data/user_action_goodsid.txt", "r") as f:
        for line in f:
            if len(line.split("\t")) != 4:
                continue
            uid, goods_id, ctime, action = line.strip().split("\t")
            if action == "order":
                user_order_goodsid_list.append({'uid' : uid, 'goods_id' : goods_id, 'order_ctime' : ctime})

    #扩展三级类目信息
    host, port, user, pwd, db = MySQLConfigApi.get_param_from_ini_file('higo_goods', 0, False)
    db = torndb.Connection(host + ':' + port, db, user, pwd)
    logger.info("begin dump lv3 category info")
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
def cal_user_lv3_preference(periods):
    cmd = "rm -rf data/user_" + periods + "_lv3_preference.txt"
    os.system(cmd)
    action_weight = {
    'click' : 1,
    'like': 2,
    'cart' : 5,
    'order' : 10 
    }

    uid_n_category_id = {}
    n_category_id_2_name_map = {}
    sum_user_n_category_id_action_num = {}
    max_n_category_id_weight = {}
    f1 = open("data/user_" + periods + "_lv3_preference.txt", "w")
    with open("data/user_n_category_info.txt") as f:
        for line in f:
            uid, goods_id, n_category_id, n_category_name, action,order_ctime = line.strip().split("\t")
            if n_category_id not in n_category_id_2_name_map:
                n_category_id_2_name_map[n_category_id] = n_category_name
            if n_category_id not in  max_n_category_id_weight:
                max_n_category_id_weight[n_category_id] = 0
            if uid not in sum_user_n_category_id_action_num:
                sum_user_n_category_id_action_num.setdefault(uid, {n_category_id:0})
                #本身取三个月订单数据就是短期画像，时间衰减较弱
                #设置时间衰减为0.005，半衰期三个月内影响都不明显，对最近的加权较多，为了提高应季新品的加权效果
                time_decay = cal_time_decay(0.003 , datetime.datetime.strptime(order_ctime, "%Y-%m-%d"))
                sum_user_n_category_id_action_num[uid][n_category_id] = 1 * action_weight[action] * time_decay
            else:
                if n_category_id not in sum_user_n_category_id_action_num[uid]:
                    sum_user_n_category_id_action_num[uid].setdefault(n_category_id, 0)
                #设置时间衰减为0.005，半衰期在三个月左右
                time_decay = cal_time_decay(0.003, datetime.datetime.strptime(order_ctime, "%Y-%m-%d"))
                sum_user_n_category_id_action_num[uid][n_category_id] += action_weight[action] * time_decay
            if sum_user_n_category_id_action_num[uid][n_category_id] > max_n_category_id_weight[n_category_id] : 
                max_n_category_id_weight[n_category_id] = sum_user_n_category_id_action_num[uid][n_category_id]
    #归一化
    for uid, ncategory_dict in sum_user_n_category_id_action_num.items():
        for ncategory_id, origin_score in ncategory_dict.items():
            weight = ("%.2f" % (0.01 + (origin_score - 0) / max_n_category_id_weight[ncategory_id]))
            f1.write("%s{\c}%s{\c}%s{\c}%s\n" % (uid, ncategory_id, n_category_id_2_name_map[ncategory_id], weight))
 
    f1.close()
    return
def cal_user_lv3_preference_new(periods):
    #cmd = "rm -rf data/user_" + periods + "_category_preference.txt"
    #os.system(cmd)
    action_weight = {
    'click' : 1,
    'like': 2,
    'cart' : 5,
    'order' : 10 
    }

    #tf
    uid_category_id_map = {}
    category_id_2_name_map = {}
    #idf
    category_id_uid_map = {}
    idf_map = {}
    weight_map = {}
    uid_sum = 0
    f1 = open("data/user_" + periods + "_lv3_preference.txt", "w")
    with open("data/user_n_category_info.txt") as f:
        for line in f:
            if len(line.split("\t")) != 6:
                continue
            uid, goods_id, category_id, category_name, action,order_ctime = line.strip().split("\t")
            if category_id not in category_id_2_name_map:
                category_id_2_name_map[category_id] = category_name
            if uid not in uid_category_id_map:
                uid_category_id_map.setdefault(uid, {})
            if category_id not in uid_category_id_map[uid]:
                uid_category_id_map[uid][category_id] = 1
            else:
                uid_category_id_map[uid][category_id] += 1
            if category_id not in category_id_uid_map:
                category_id_uid_map[category_id] = [uid]
            elif uid not in category_id_uid_map[category_id]:
                category_id_uid_map[category_id].append(uid)
        uid_sum = len(uid_category_id_map.keys())
        for k, l in category_id_uid_map.items():
            idf_map[k] = math.log(uid_sum / (len(l) + 1), 10)

        for uid in uid_category_id_map.keys():
            weight_map[uid] = {}
            #euclid norm
            norm = 0.0
            for category_id in uid_category_id_map[uid].keys():

                weight_map[uid][category_id] = uid_category_id_map[uid][category_id] * idf_map[category_id]
                norm += weight_map[uid][category_id]**2
            norm = math.sqrt(norm)
            for category_id in uid_category_id_map[uid].keys():
                weight_map[uid][category_id] = weight_map[uid][category_id] / norm
                f1.write("%s{\c}%s{\c}%s{\c}%s\n" % (uid, category_id, category_id_2_name_map[category_id], weight_map[uid][category_id]))

    f1.close()

    return
def load_to_hive():
    return
def load_to_redis():
    return

def cal_user_lv2_preference():
    return
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p','--periods', help="品牌偏好属性类型，长中短 long|middle|short")
    args = parser.parse_args()  
    if args.periods == None:
        print "解析失败"
        return
    if args.periods not in ['long', 'middle', 'short']:
        print "periods解析失败"
        return

    #get_goods_lv3_category_info()
    #gc.enable()
    #gc.collect()
    #gc.disable()
    #cal_user_lv3_preference(args.periods)

    cal_user_lv3_preference_new(args.periods)
    load_to_hive()
    return

if __name__ == "__main__":
    main()
