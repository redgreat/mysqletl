#!/usr/bin/env python3
#-*- coding:utf-8 -*-

import pymysql
import time
from pyetl.task import Task
from pyetl.reader import DatabaseReader
from pyetl.writer import DatabaseWriter

"""定义连接地址"""
try:
    db_src = pymysql.connect(host='',
                             database='',
                             user='',
                             password='',
                             charset='utf8')

except Exception as db_error:
    print("{}:源库连接失败：错误原因：{}".format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())), db_error))
try:
    db_tar = pymysql.connect(host='',
                             database='',
                             user='',
                             password='',
                             charset='utf8')
except Exception as db_error:
    print("{}:目标库连接失败：错误原因：{}".format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())), db_error))

reader = DatabaseReader(db_src, table_name='hangfire_aggregatedcounter')
writer = DatabaseWriter(db_tar, table_name='hangfire_aggregatedcounter')
Task(reader, writer).start()
