#!/usr/bin/env python
# -*- coding: UTF-8 -*-

"""
仅在所有node都running&&synced检测：
1、3个node存在wsrep_cluster_state_uuid不一致情况，脑裂
2、wsrep_cluster_state_uuid一致时，wsrep_cluster_size 不一致
"""

import MySQLdb
import sys
import time

mysql_user = "haproxy"
mysql_pw = ""

DB1 = "10.68.201.43"
DB2 = "10.68.201.44"
DB3 = "10.68.201.45"

def get_cluster_info(host):
    conn = MySQLdb.connect(host=host, port=3306, user=mysql_user, passwd=mysql_pw, db='information_schema')
 
     # prepare a cursor object using cursor() method
    cursor = conn.cursor()

    # execute SQL query using execute() method.
    cursor.execute("show status like 'wsrep%';")

    # Fetch a single row using fetchone() method.
    data = cursor.fetchall()

    collect_keys = ['wsrep_cluster_size','wsrep_cluster_status','wsrep_local_state_comment','wsrep_cluster_state_uuid']

    collect_data = {}

    for i in range(0, cursor.rowcount, 1):
        row_data = data[i]
        key = row_data[0]
        if key in collect_keys:
            collect_data[key] = row_data[1]

    # disconnect from server
    conn.close()
    return collect_data


def check_active(host):
    retry = 0
    while retry < 2:
        try:
           conn = MySQLdb.connect(host=host, port=3306, user=mysql_user, passwd=mysql_pw, db='information_schema')
           info = get_cluster_info(host)
           db_status = info['wsrep_local_state_comment']
           if db_status == "Synced":
              return True
           else:
              return False
        except Exception as e:
           time.sleep(5)
           retry = retry + 1
    return False


def  get_cluster_uuid(host):
     info = get_cluster_info(host)
     return info['wsrep_cluster_state_uuid']

def  get_cluster_size(host):
     info = get_cluster_info(host)
     return info['wsrep_cluster_size']

def main():
    
    #判断数据库是否全部正常
    if check_active(DB1) and check_active(DB2) and check_active(DB3):    
        #print "All DB running..."
        pass
    else:
        #print "Not all DB running..."
        print 0
        sys.exit(0)
       

    #uuid全部相等正常
    if get_cluster_uuid(DB1) == get_cluster_uuid(DB2) and get_cluster_uuid(DB1) == get_cluster_uuid(DB3) and get_cluster_uuid(DB2) == get_cluster_uuid(DB3):
        # size全部相等
        if get_cluster_size(DB1) == get_cluster_size(DB2) and get_cluster_size(DB1) == get_cluster_size(DB3) and get_cluster_size(DB2) == get_cluster_size(DB3):
           print 0
        else: 
           print 1
    else:
        print 1
    

if __name__ == "__main__":
    sys.exit(main())

