#!/usr/bin/env python3
#-*- coding:utf-8 -*-

import pymysql
import time
from pyetl.task import Task
from pyetl.reader import DatabaseReader
from pyetl.writer import DatabaseWriter

"""提示信息"""
prompt = """
\033[1;33m迁移注意事项\033[0m
1.如果选择不迁移全部源表，视图可能因表依赖原因迁移失败;
2.作业迁移后状态默认\033[1;31mDISABLE\033[0m，需要自行开启;
"""
print (prompt)

"""入参名称定义"""
in_src_dbname = input("请输入源数据库名称(默认serviceordercenter)>>>：") or 'serviceordercenter'
in_tar_dbname = input("请输入目标数据库名称(默认serviceordercenter)>>>：") or 'serviceordercenter'

"""定义连接地址"""
try:
    db_src = pymysql.connect(host='',
                             database=in_src_dbname,
                             user='',
                             password='',
                             charset='utf8')

except Exception as db_error:
    print ("{}:源库连接失败：错误原因：{}".format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),db_error))
try:
    db_tar = pymysql.connect(host='',
                             database=in_tar_dbname,
                             user='',
                             password='',
                             charset='utf8')

except Exception as db_error:
    print ("{}:目标库连接失败：错误原因：{}".format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),db_error))
db_start_time = time.time()


"""函数/过程/视图的迁移"""
"""迁移函数"""
cur_src_dic = db_src.cursor(cursor=pymysql.cursors.DictCursor)
cur_tar = db_tar.cursor()
"""获取所有函数"""
sql_get_function = 'SELECT ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_SCHEMA=\'{}\' AND  ROUTINE_TYPE = \'FUNCTION\';'.format(in_src_dbname)

cur_src_dic.execute(sql_get_function)
src_fun = cur_src_dic.fetchall()

for fun_name_dic in src_fun:
    fun_name = fun_name_dic.get('ROUTINE_NAME')
    """函数创建语句"""
    sql_show_function = 'SHOW CREATE FUNCTION `{}`;'.format(fun_name)

    cur_src_dic.execute(sql_show_function)
    fun_desc = cur_src_dic.fetchone().get('Create Function')

    sql_drop_function = 'DROP FUNCTION IF EXISTS `{}`;'.format(fun_name)
    sql_create_function = '{};'.format(fun_desc)

    try:
        cur_tar.execute(sql_drop_function)
        cur_tar.execute(sql_create_function)
        db_tar.commit()
        print ("{}:函数{}创建成功...".format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),fun_name))
    except Exception as db_error:
        db_tar.rollback()
        print ("{}:函数{}创建失败，原因：{}".format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),fun_name,db_error))
    pass
cur_src_dic.close()
cur_tar.close()
"""迁移视图"""
cur_src_dic = db_src.cursor(cursor=pymysql.cursors.DictCursor)
cur_tar = db_tar.cursor()
"""获取所有视图"""
sql_get_view = 'SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA=\'{}\';'.format(in_src_dbname)

cur_src_dic.execute(sql_get_view)
src_view = cur_src_dic.fetchall()

for view_name_dic in src_view:
    view_name = view_name_dic.get('TABLE_NAME')
    """视图创建语句"""
    sql_show_view = 'SHOW CREATE VIEW `{}`;'.format(view_name)

    cur_src_dic.execute(sql_show_view)
    view_desc = cur_src_dic.fetchone().get('Create View')

    sql_drop_view = 'DROP VIEW IF EXISTS `{}`;'.format(view_name)
    sql_create_view = '{};'.format(view_desc)

    try:
        cur_tar.execute(sql_drop_view)
        cur_tar.execute(sql_create_view)
        db_tar.commit()
        print ("{}:视图{}创建成功...".format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),view_name))
    except Exception as db_error:
        db_tar.rollback()
        print ("{}:视图{}创建失败，原因：{}".format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),view_name,db_error))
    pass
cur_src_dic.close()
cur_tar.close()

"""迁移存储过程"""
cur_src_dic = db_src.cursor(cursor=pymysql.cursors.DictCursor)
cur_tar = db_tar.cursor()
"""获取所有过程"""
sql_get_proc = 'SELECT ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_SCHEMA=\'{}\' AND  ROUTINE_TYPE = \'PROCEDURE\';'.format(in_src_dbname)

cur_src_dic.execute(sql_get_proc)
src_proc = cur_src_dic.fetchall()

for proc_name_dic in src_proc:
    proc_name = proc_name_dic.get('ROUTINE_NAME')
    """过程创建语句"""
    sql_show_proc = 'SHOW CREATE PROCEDURE `{}`;'.format(proc_name)

    cur_src_dic.execute(sql_show_proc)
    proc_desc = cur_src_dic.fetchone().get('Create Procedure')

    sql_drop_proc = 'DROP PROCEDURE IF EXISTS `{}`;'.format(proc_name)
    sql_create_proc = '{};'.format(proc_desc)

    try:
        cur_tar.execute(sql_drop_proc)
        cur_tar.execute(sql_create_proc)
        db_tar.commit()
        print ("{}:存储过程{}创建成功...".format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),proc_name))
    except Exception as db_error:
        db_tar.rollback()
        print ("{}:存储过程{}创建失败，原因：{}".format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),proc_name,db_error))
    pass
cur_src_dic.close()
cur_tar.close()

"""迁移作业"""
cur_src_dic = db_src.cursor(cursor=pymysql.cursors.DictCursor)
cur_tar = db_tar.cursor()
"""获取所有作业"""
sql_get_event = 'SELECT EVENT_NAME FROM INFORMATION_SCHEMA.EVENTS WHERE EVENT_SCHEMA=\'{}\';'.format(in_src_dbname)

cur_src_dic.execute(sql_get_event)
src_event = cur_src_dic.fetchall()

for event_name_dic in src_event:
    event_name = event_name_dic.get('EVENT_NAME')
    """过程创建语句"""
    sql_show_event = 'SHOW CREATE EVENT `{}`;'.format(event_name)

    cur_src_dic.execute(sql_show_event)
    event_desc = cur_src_dic.fetchone().get('Create Event')

    sql_drop_event = 'DROP EVENT IF EXISTS `{}`;'.format(event_name)
    sql_create_event = '{};'.format(event_desc)
    sql_create_event = sql_create_event.replace('ENABLE','DISABLE')

    try:
        cur_tar.execute(sql_drop_event)
        cur_tar.execute(sql_create_event)
        db_tar.commit()
        print ("{}:作业{}创建成功...".format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),event_name))
    except Exception as db_error:
        db_tar.rollback()
        print ("{}:作业{}创建失败，原因：{}".format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),event_name,db_error))
    pass
cur_src_dic.close()
cur_tar.close()

"""关闭数据库连接"""
db_end_time = time.time()
db_deltatime = db_end_time - db_start_time
db_totalhour = int(db_deltatime / 3600)
db_totalminute = int((db_deltatime -db_totalhour * 3600) / 60)
db_totalsecond = int(db_deltatime - db_totalhour * 3600 - db_totalminute * 60)
print("{}:已将源库{}全部表迁移至{},总计耗时:{}小时{}分{}秒".format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),in_src_dbname,in_tar_dbname,db_totalhour,db_totalminute,db_totalsecond))
db_src.close()
db_tar.close()

