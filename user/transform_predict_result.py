#coding:utf-8

import os
import sys


def main():
    uid_index = {}
    brand_index = {}
    brand_info = {}
    with open("./data/uid_index.txt", "r") as f:
        for line in f:
            line = line.strip().split(" ")
            uid_index[line[1]] = line[0]

    with open("./data/brand_index.txt", "r") as f:
        for line in f:
            line = line.strip().split(" ")
            brand_index[line[1]] = line[0]
    with open("./brand_info", "r") as f:
        for line in f:
            ll = line.strip().split(" ")
            brand_id = ll[0]
            brand_name = " ".join(ll[1:])
            brand_info[brand_id] = brand_name
    fpredict_result = open("./pred.txt", "r")
    fpredict_data = open("./data/svd_predict.txt", "r")
    ftransformed_result = open("./brand_preference.txt", "w")
    for line in fpredict_data:
        line = line.strip().split(" ")
        uid_idx = line[4].split(":")[0]
        uid = uid_index[uid_idx]
        brand_idx = line[5].split(":")[0]
        brand_id = brand_index[brand_idx]
        pred = fpredict_result.readline()
        pred = pred.strip()

        if brand_id not in brand_info:
            continue
        brand_name = brand_info[brand_id]
        ftransformed_result.write("%s{\c}%s{\c}%s{\c}%s\n" % (uid, brand_id, brand_name, pred))
        
    fpredict_result.close()
    fpredict_data.close()
    ftransformed_result.close()

    
    return

if __name__ == "__main__":
    main()
