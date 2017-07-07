#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os
import sys

class MySQLConfigApi:
    # ini file
    ini_file = "/home/work/conf/mysql/higo.mysql.ini"

    db_list = list()
    db_is_not_exist = True

    # read param from mysql ini file
    @classmethod
    def get_param_from_ini_file(cls, dbname, write_or_read, online=True):
        host, port, user, pwd, db = 0, 0, 0, 0, dbname
        if not online:
            cls.ini_file = '/home/work/conf/script/higo.mysql.ini'
        # open file
        f = open(cls.ini_file, 'r')
        for line in f:

            # get db and master
            db = line.split('db=')[1].split(' ')[0].strip()
            #host = line.split('host=')[1].split(' ')[0].strip()
            #port = line.split('port=')[1].split(' ')[0].strip()
            #weight = line.split('weight=')[1].split(' ')[0].strip()
            #user = line.split('user=')[1].split(' ')[0].strip()
            #pwd = line.split('pass=')[1].split(' ')[0].strip()
            master = line.split('master=')[1].strip()
            cls.db_list.append(db)

            # match dbname and master_slave
            if db == dbname and int(master) == write_or_read:
                # get all params value
                db = line.split('db=')[1].split(' ')[0].strip()
                host = line.split('host=')[1].split(' ')[0].strip()
                port = line.split('port=')[1].split(' ')[0].strip()
                # weight = line.split('weight=')[1].split(' ')[0].strip()
                user = line.split('user=')[1].split(' ')[0].strip()
                pwd = line.split('pass=')[1].split(' ')[0].strip()
                master = line.split('master=')[1].strip()

                cls.db_is_not_exist = False
                break
            else:
                continue

        # close file
        f.close()
        cls.db_list = list(set(cls.db_list))
        #return '%s:%s' % (host, port), user, pwd, db
        return host, port, user, pwd, db

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print 'No db name specified.'
        exit()
    else:
        host, user, pwd, db = MySQLConfigApi.get_param_from_ini_file(sys.argv[1], 1)

        if MySQLConfigApi.db_is_not_exist:
            print '%s is not exist.' % sys.argv[1]
            print 'higo db...: %s' % str(MySQLConfigApi.db_list)
            exit()
        cmd = 'mysql -h%s -P%s -u%s -p"%s" %s' % (host.split(':')[0], host.split(':')[1], user, pwd, db)
        os.system(cmd)
