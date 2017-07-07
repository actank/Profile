#coding:utf-8
import sys
import redis
import json
import time


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

def load_user_brand_preference():
    with open ("data/user_brand_preference.txt") as f:
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
                    user_profile[key_bak].setdefault("brand_preference", weight_list)
                    weight_list = []
                key_bak = key
            if float(weight) < 0.2:
                continue
            weight_list.append({"brand_id" : brand_id, "brand_name" : brand_name, "weight" : weight})
    return

def load_user_n_category_preference():
    with open ("data/user_n_category_preference.txt") as f:
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
                    user_profile[key_bak]["n_category_preference"] = weight_list
                    weight_list = []
                key_bak = key
            if float(weight) < 0.2:
                continue
            weight_list.append({"n_category_id" : n_category_id, "n_category_name" : n_category_name, "weight" : weight})

    return

def write_to_redis():
    conn = redis.Redis(host, port, db)
    key = dict_key_pre + version
    for uid, profile in user_profile.items():
        json_profile = json.dumps(profile)
        conn.hset(key, uid, profile)
        print uid
        time.sleep(0.01)
    return

def main():
    load_user_brand_preference()
    load_user_n_category_preference()
    write_to_redis()

if __name__ == "__main__":
    main()
