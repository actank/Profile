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
import json
import os
from common.utils import *
import argparse 
logger = logging.getLogger("info") 



user_static_attr_list = []

def main():
    with open("data/user_id.txt") as f:
        host, port, user, pwd, db = MySQLConfigApi.get_param_from_ini_file('higo_account', 0, False)
        db_account = torndb.Connection(host + ':' + port, db, user, pwd)
        for line in f:
            user_id = line.strip()
            try:
                sql = "select identity_card_number from t_pandora_address where buyer_id=%s" % (user_id)
                res = db_account.query(sql)
                if len(res) == 0:
                    continue
                id_card_num = res[0]['identity_card_number']
                sex = "female"
                if len(id_card_num) == 15:
                    check_num = id_card_num[len(id_card_num) - 1]
                    if check_num.isdigit():
                        if int(check_num) % 2 == 1:
                            sex = "male"
                    else:
                        sex = "female"
                elif len(id_card_num) == 18:
                    check_num = id_card_num[len(id_card_num) - 2]
                    if check_num.isdigit():
                        if int(check_num) % 2 == 1:
                            sex = "male"
                else:
                    continue
                user_static_attr_list.append({"uid" : user_id, "sex" : sex})

            except Exception, e:
                print user_id
                print e
                print traceback.print_exc()
    cmd = "rm -rf data/user_static_attr.txt"
    os.system(cmd)
    output = open("data/user_static_attr.txt", "w")
    for item in user_static_attr_list:
        output.write(json.dumps(item) + "\n")
    output.close()

    return

if __name__ == "__main__":
    main()
