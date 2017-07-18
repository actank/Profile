#coding:utf-8
import sys
reload(sys)
sys.path.append("..")
sys.setdefaultencoding("utf-8")
import os
import redis
import json
import time
import argparse
from common.utils import *

logger = logging.getLogger("info") 




version_key = 'user_profile_version'
dict_key_pre = 'user_profile_'
version = '0.1'

# 测试
# host = '172.17.30.91'll
# port = 6379
# db = 0

# 线上
#host = '10.8.9.83'
host = '10.20.252.2'
port = 6001
db = 0

user_profile = {}

def load_user_brand_preference(periods):
    with open ("data/user_" + periods + "_brand_preference.txt") as f:
        key = ""
        key_bak = ""
        weight_list = []
        for line in f:
            uid, brand_id, brand_name, weight = line.strip().split("{\c}")
            key = uid
            if key_bak != key:
                if key_bak != "":
                    if key_bak not in user_profile:
                        user_profile[key_bak] = {}
                    user_profile[key_bak].setdefault(periods + "_brand_preference", weight_list)
                    weight_list = []
                key_bak = key
            #品牌有持久性，weight大于0.05即可认为有兴趣
            if float(weight) < 0.05:
                continue
            weight_list.append({"brand_id" : brand_id, "brand_name" : brand_name, "weight" : weight})
    return

def load_user_lv3_preference(periods):
    with open ("data/user_" + periods + "_lv3_preference.txt") as f:
        key = ""
        key_bak = ""
        weight_list = []
        for line in f:
            uid, n_category_id, n_category_name, weight = line.strip().split("{\c}")
            key = uid
            if key_bak != key:
                if key_bak != "":
                    if key_bak not in user_profile:
                        user_profile[key_bak] = {}
                    user_profile[key_bak][periods + "_lv3_preference"] = weight_list
                    weight_list = []
                key_bak = key
            #weight小于0.2直接过滤
            if float(weight) < 0.2:
                continue
            weight_list.append({"lv3_category_id" : n_category_id, "lv3_category_name" : n_category_name, "weight" : weight})
    return

def load_user_static_attr():
    with open("data/user_static_attr.txt", "r") as f:
        for line in f:
            line = line.strip().encode(encoding='utf-8')
            user_attr = json.loads(line, encoding='utf-8')
            user_id = user_attr['uid'].encode('utf-8')
            sex = user_attr['sex'].encode('utf-8')
            if user_profile.has_key(user_id):
                user_profile[user_id]['static_attr'] = {'user_id' : user_id, 'sex' : sex}
        for uid in user_profile:
            if not user_profile[uid].has_key('static_attr'):
                user_profile[uid]['static_attr'] = {'user_id' : user_id, 'sex' : ''}
    return
            

def write_to_redis():
    conn = redis.Redis(host, port, db)
    key = dict_key_pre + version
    for uid, profile in user_profile.items():
        json_profile = json.dumps(profile)
        logger.info("user_id:%s, profile:%s" % (uid, json_profile))
        conn.hset(key, uid, profile)
        time.sleep(0.01)
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

    load_user_brand_preference(args.periods)
    load_user_lv3_preference(args.periods)
    load_user_static_attr()
    write_to_redis()
    return

if __name__ == "__main__":
    main()
