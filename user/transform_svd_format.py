#coding:utf-8

import sys
import os
import gc



def transform_train_data():
    uid_index = []
    brand_index = []

    f1 = open("./data/svd_train.txt", "w")
    fu = open("./data/uid_index.txt", "w")
    fb = open("./data/brand_index.txt", "w")
    with open("./data/user_long_brand_preference.txt", "r") as f:
        for line in f:
            line = line.strip().split("{\c}")
            uid = line[0]
            brand_id = line[1]
            score = line[3]
            if uid not in uid_index:
                uid_index.append(uid)
            if brand_id not in brand_index:
                brand_index.append(brand_id)
    for k in range(len(uid_index)):
        fu.write("%s %s\n" % (uid_index[k], k))
    for k in range(len(brand_index)):
        fb.write("%s %s\n" % (brand_index[k], k))
    fu.close()
    fb.close()
    uid_index = {}
    brand_index = {}
    gc.enable()
    gc.collect()
    gc.disable()
    with open("./data/uid_index.txt", "r") as f:
        for line in f:
            line = line.strip().split(" ")
            uid_index[line[0]] = line[1]

    with open("./data/brand_index.txt", "r") as f:
        for line in f:
            line = line.strip().split(" ")
            brand_index[line[0]] = line[1]
    with open("./data/user_long_brand_preference.txt", "r") as f:
        for line in f:
            line = line.strip().split("{\c}")
            uid = line[0]
            brand_id = line[1]
            score = line[3]
            f1.write("%s 0 1 1 %s:1 %s:1\n" % (score, uid_index[uid], brand_index[brand_id]))
    f1.close()
    return

def prepare_predict_data():
    gc.enable()
    gc.collect()
    gc.disable()
    uid_index = {}
    brand_index = {}
    with open("./data/uid_index.txt", "r") as f:
        for line in f:
            line = line.strip().split(" ")
            uid_index[line[0]] = line[1]

    with open("./data/brand_index.txt", "r") as f:
        for line in f:
            line = line.strip().split(" ")
            brand_index[line[0]] = line[1]
    with open("data/svd_predict.txt", "w") as f:
        for uid in uid_index.keys():
            for brand_id in brand_index.keys():
                f.write("%s 0 1 1 %s:1 %s:1\n" % (0.0, uid_index[uid], brand_index[brand_id]))

    return


if __name__ == "__main__":
    transform_train_data()
    #prepare_predict_data()
