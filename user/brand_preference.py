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
import math
import gc
import os
from common.utils import *
import argparse 
logger = logging.getLogger("info") 


def get_goods_brand_info():
    #获取用户下过单的宝贝id
    cmd = "rm -rf data/user_brand_info.txt"
    os.system(cmd)
    user_order_goodsid_list = []
    user_cart_goodsid_list = []
    user_see_goodsid_list = []
    with open("data/user_action_goodsid.txt", "r") as f:
        for line in f:
            if len(line.split("\t")) != 4:
                continue
            uid, goods_id, ctime, action= line.strip().split("\t")
            if action == "order":
                user_order_goodsid_list.append({'uid' : uid, 'goods_id' : goods_id, 'order_ctime' : ctime})
            elif action == "cart":
                user_cart_goodsid_list.append({'uid' : uid, 'goods_id' : goods_id, 'cart_ctime' : ctime})
            elif action == "see":
                user_see_goodsid_list.append({'uid' : uid, 'goods_id' : goods_id, 'see_ctime' : ctime})
    #扩展品牌信息
    host, port, user, pwd, db = MySQLConfigApi.get_param_from_ini_file('higo_goods', 0, False)
    db = torndb.Connection(host + ':' + port, db, user, pwd)
    f = open("data/user_brand_info.txt", "w")
    try:
        for goods in user_order_goodsid_list:
            if goods['goods_id'] == None:
                continue
            sql = "select brand_id, brand_name from t_pandora_goods where goods_id=%s" % (goods['goods_id'])
            res = db.query(sql)
            if len(res) == 0:
                continue
            goods['brand_id'] = res[0]['brand_id']
            goods['brand_name'] = res[0]['brand_name']
            if goods['brand_id'] == None:
                continue
            if goods['brand_name'] == None:
                continue
            #action = order
            f.write("%s{\c}%s{\c}%s{\c}%s{\c}order{\c}%s\n" % (goods['uid'], str(goods['goods_id']), str(goods['brand_id']), goods['brand_name'], goods['order_ctime']))
        for goods in user_cart_goodsid_list:
            if goods['goods_id'] == None:
                continue
            sql = "select brand_id, brand_name from t_pandora_goods where goods_id=%s" % (goods['goods_id'])
            res = db.query(sql)
            if len(res) == 0:
                continue
            goods['brand_id'] = res[0]['brand_id']
            goods['brand_name'] = res[0]['brand_name']
            if goods['brand_id'] == None:
                continue
            if goods['brand_name'] == None:
                continue
            #action = cart
            f.write("%s{\c}%s{\c}%s{\c}%s{\c}cart{\c}%s\n" % (goods['uid'], str(goods['goods_id']), str(goods['brand_id']), goods['brand_name'], goods['cart_ctime']))
        for goods in user_see_goodsid_list:
            if goods['goods_id'] == None or goods['goods_id'] == '':
                continue
            sql = "select brand_id, brand_name from t_pandora_goods where goods_id=%s" % (goods['goods_id'])
            res = db.query(sql)
            if len(res) == 0:
                continue
            goods['brand_id'] = res[0]['brand_id']
            goods['brand_name'] = res[0]['brand_name']
            if goods['brand_id'] == None:
                continue
            if goods['brand_name'] == None:
                continue
            #action = see
            f.write("%s{\c}%s{\c}%s{\c}%s{\c}see{\c}%s\n" % (goods['uid'], str(goods['goods_id']), str(goods['brand_id']), goods['brand_name'], goods['see_ctime']))


    except Exception, e:
        print e
        print traceback.print_exc()
    finally:
        f.close()
        db.close()
    return

#计算品牌偏好
#preference_weight = action_weight * time_weight * goods_weight
def cal_user_brand_preference(periods):
    cmd = "rm -rf data/user_" + periods + "_brand_preference.txt"
    os.system(cmd)
    action_weight = {
    'click' : 1,
    'like': 2,
    'cart' : 5,
    'order' : 10 
    }

    uid_brand_id = {}
    brand_id_2_name_map = {}
    sum_user_brand_id_action_num = {}
    max_brand_id_weight = {}
    f1 = open("data/user_" + periods + "_brand_preference.txt", "w")
    with open("data/user_brand_info.txt") as f:
        for line in f:
            if len(line.split("{\c}")) != 6:
                continue
            uid, goods_id, brand_id, brand_name, action,order_ctime = line.strip().split("{\c}")
            if brand_id not in brand_id_2_name_map:
                brand_id_2_name_map[brand_id] = brand_name
            if brand_id not in  max_brand_id_weight:
                max_brand_id_weight[brand_id] = 0
            if uid not in sum_user_brand_id_action_num:
                sum_user_brand_id_action_num.setdefault(uid, {brand_id:0})
                #time_decay 对品牌的效果并不好，品牌本身受时间衰减影响较弱，设置a为0.03，半衰期在三个月左右，目前策略不设置半衰期，直接置为1.0，对于不同品牌的季节偏好，采用长中短期用户画像分别计算的策略来计算品牌偏好
                time_decay = cal_time_decay(0.03, datetime.datetime.strptime(order_ctime, "%Y-%m-%d"))
                time_decay = 1.0
                sum_user_brand_id_action_num[uid][brand_id] = 1 * action_weight[action] * time_decay 
            else:
                if brand_id not in sum_user_brand_id_action_num[uid]:
                    sum_user_brand_id_action_num[uid].setdefault(brand_id, 0)
                time_decay = cal_time_decay(0.03, datetime.datetime.strptime(order_ctime, "%Y-%m-%d"))
                time_decay = 1.0
                sum_user_brand_id_action_num[uid][brand_id] += action_weight[action] * time_decay        
            if sum_user_brand_id_action_num[uid][brand_id] > max_brand_id_weight[brand_id] : 
                    max_brand_id_weight[brand_id] = sum_user_brand_id_action_num[uid][brand_id]
    #归一化
    for uid, brand_dict in sum_user_brand_id_action_num.items():
        for brand_id, origin_score in brand_dict.items():
            weight = ("%.2f" % (0.01 + (origin_score - 0) / max_brand_id_weight[brand_id]))
            if float(weight) > 1.00 :
                weight = 1.00
            f1.write("%s{\c}%s{\c}%s{\c}%s\n" % (uid, brand_id, brand_id_2_name_map[brand_id], weight))
 
    f1.close()
    return

def cal_user_brand_preference_new(periods):
    #cmd = "rm -rf data/user_" + periods + "_brand_preference.txt"
    #os.system(cmd)
    action_weight = {
    'click' : 1,
    'like': 2,
    'cart' : 5,
    'order' : 10 
    }

    #tf
    uid_brand_id_map = {}
    brand_id_2_name_map = {}
    #idf
    brand_id_uid_map = {}
    idf_map = {}
    weight_map = {}
    uid_sum = 0
    f1 = open("data/user_" + periods + "_brand_preference.txt", "w")
    with open("data/user_brand_info.txt") as f:
        for line in f:
            if len(line.split("{\c}")) != 6:
                continue
            uid, goods_id, brand_id, brand_name, action,order_ctime = line.strip().split("{\c}")
            if brand_id not in brand_id_2_name_map:
                brand_id_2_name_map[brand_id] = brand_name
            if uid not in uid_brand_id_map:
                uid_brand_id_map.setdefault(uid, {})
            if brand_id not in uid_brand_id_map[uid]:
                uid_brand_id_map[uid][brand_id] = 1
            else:
                uid_brand_id_map[uid][brand_id] += 1
            if brand_id not in brand_id_uid_map:
                brand_id_uid_map[brand_id] = [uid]
            elif uid not in brand_id_uid_map[brand_id]:
                brand_id_uid_map[brand_id].append(uid)
        uid_sum = len(uid_brand_id_map.keys())
        for k, l in brand_id_uid_map.items():
            idf_map[k] = math.log(uid_sum / (len(l) + 1), 10)

        for uid in uid_brand_id_map.keys():
            weight_map[uid] = {}
            #euclid norm
            norm = 0.0
            for brand_id in uid_brand_id_map[uid].keys():

                weight_map[uid][brand_id] = uid_brand_id_map[uid][brand_id] * idf_map[brand_id]
                norm += weight_map[uid][brand_id]**2
            norm = math.sqrt(norm)
            for brand_id in uid_brand_id_map[uid].keys():
                weight_map[uid][brand_id] = weight_map[uid][brand_id] / norm
                f1.write("%s{\c}%s{\c}%s{\c}%s\n" % (uid, brand_id, brand_id_2_name_map[brand_id], weight_map[uid][brand_id]))

    f1.close()
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

    get_goods_brand_info()
    cal_user_brand_preference_new(args.periods)
    return

if __name__ == "__main__":
    main()
