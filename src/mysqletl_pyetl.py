#!/usr/bin/env python3
#-*- coding:utf-8 -*-

import pymysql
import time
from pyetl.task import Task
from pyetl.reader import DatabaseReader
from pyetl.writer import DatabaseWriter

# """提示信息"""
# prompt = """
# \033[1;33m迁移注意事项\033[0m
# 1.如果选择不迁移全部源表，视图可能因表依赖原因迁移失败;
# 2.作业迁移后状态默认\033[1;31mDISABLE\033[0m，需要自行开启;
# """
# print (prompt)

"""入参名称定义"""
in_src_dbname = input("请输入源数据库名称(默认serviceordercenter)>>>：") or 'serviceordercenter'
in_tar_dbname = input("请输入目标数据库名称(默认serviceordercenteretl)>>>：") or 'serviceordercenteretl'
in_betchnum = input("请输入每批次迁移表数据行数(默认1000)") or 1000
is_alltable = input("""
请选择表迁移范围(默认2)>>
1.自动扫描全部表并迁移;
2.只迁移配置表(tm_ifmovedata)内所有表;
3.只迁移配置表(tm_ifmovedata)内需要迁移结构的表(IF_DATA=0);
4.只迁移配置表(tm_ifmovedata)内需要迁移结构和数据的表(IF_DATA=1);
5.输入表名迁移指定表;
6 or else.跳过表数据迁移;
\033[1;33m表迁移注意事项：\033[0m如果选择不迁移全部源表，视图可能因表依赖原因迁移失败;
""") or '2'
in_betchnum = int(in_betchnum)

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
totalcnt = 0

cur_src_dic = db_src.cursor(cursor=pymysql.cursors.DictCursor)
if is_alltable == '1':
    """自动查询所有表"""
    sql_get_tables = 'SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=\'{}\' AND TABLE_TYPE=\'BASE TABLE\';'.format(in_src_dbname)

    cur_src_dic.execute(sql_get_tables)
    src_table = cur_src_dic.fetchall()

elif is_alltable == '2':
    """配置内所有表"""
    sql_get_tables = 'SELECT TABLE_NAME FROM tm_ifmovedata WHERE 1 = 1;'

    cur_src_dic.execute(sql_get_tables)
    src_table = cur_src_dic.fetchall()

elif is_alltable == '3':
    """配置内只迁移结构的表"""
    sql_get_tables = 'SELECT TABLE_NAME FROM tm_ifmovedata WHERE IF_DATA = 0;'

    cur_src_dic.execute(sql_get_tables)
    src_table = cur_src_dic.fetchall()

elif is_alltable == '4':
    """配置内迁移结构和数据的表"""
    sql_get_tables = 'SELECT TABLE_NAME FROM tm_ifmovedata WHERE IF_DATA = 1;'

    cur_src_dic.execute(sql_get_tables)
    src_table = cur_src_dic.fetchall()

elif is_alltable == '5':
    in_pre_tbname = input("请输入需要迁移指定表名：")
    cur_src_dic.execute('SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=\'{}\' AND TABLE_TYPE=\'BASE TABLE\' AND TABLE_NAME = \'{}\' LIMIT 1;'.format(in_src_dbname,in_pre_tbname))
    restb = cur_src_dic.fetchone().get('TABLE_NAME')

    if restb is not None:
        src_table = ([{"TABLE_NAME":"{}".format(in_pre_tbname)}])
    else:
        is_alltable = '-1'
        print("{}:表名{}在源库不存在，跳过表迁移！".format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),in_pre_tbname))

cur_src_dic.close()

if is_alltable in ('1', '2', '3', '4', '5'):
    """循环表"""
    print('{}:开始迁移表...'.format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))))
    for tb_name_dic in src_table:

        tb_start_time = time.time()
        res = 0
        tb_name = tb_name_dic.get('TABLE_NAME')
        """获取表结构"""
        sql_show_table = 'SHOW CREATE TABLE `{}`;'.format(tb_name)

        """开启游标(防止数据迁移占用连接时间太久，每张表单独开一次)"""
        cur_src_dic = db_src.cursor(cursor=pymysql.cursors.DictCursor)
        cur_tar = db_tar.cursor()

        """获取创建表语句"""
        cur_src_dic.execute(sql_show_table)
        tb_desc = cur_src_dic.fetchone().get('Create Table')

        """创建表语句生成"""
        sql_drop_tartable = 'DROP TABLE IF EXISTS `{}`;'.format(tb_name)
        sql_create_tartable = """{};""".format(tb_desc)

        """检查目标表是否有此表"""
        sql_check_tartable = 'SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = \'{}\' AND TABLE_TYPE = \'BASE TABLE\' AND TABLE_NAME = \'{}\';'.format(in_tar_dbname,tb_name)

        """检查源库配置表(0只迁表结构,1迁移数据和表结构)"""
        sql_withdata_srctable = 'SELECT `IF_DATA` FROM `tm_ifmovedata` WHERE `TABLE_NAME` = \'{}\' LIMIT 1;'.format(tb_name)

        """创建目标表结构"""
        if not cur_tar.execute(sql_check_tartable):
            try:
                #print ("{}:执行建表{}...".format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),tb_name))
                cur_tar.execute('SET FOREIGN_KEY_CHECKS = 0;')
                cur_tar.execute(sql_drop_tartable)
                cur_tar.execute(sql_create_tartable)
                cur_tar.execute('SET FOREIGN_KEY_CHECKS = 1;')
                db_tar.commit()
                #print ("{}:表{}创建成功...".format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),tb_name))
                res = 1
            except Exception as db_error:
                cur_tar.execute('SET FOREIGN_KEY_CHECKS = 1;')
                db_tar.rollback()
                res = -1
                print( "{}:表{}迁移失败，建表语句：\n{}\n错误：\"{}\"".format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())), tb_name, sql_create_tartable, db_error))
            pass
        else:
            pass
            #print ("{}:目标库表{}已存在，直接进行数据迁移...".format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),tb_name))
        cur_src_dic.execute(sql_withdata_srctable)
        if_data_dic = cur_src_dic.fetchone()
        if_data = if_data_dic.get('IF_DATA') if if_data_dic is not None else 0

        if (res == 0 or res == 1) and (if_data != 0):

            """清空目标表数据"""
            sql_truncate_tartable = """TRUNCATE TABLE `{}`;""".format(tb_name)
            try:
                cur_tar.execute('SET FOREIGN_KEY_CHECKS = 0;')
                cur_tar.execute(sql_truncate_tartable)
                cur_tar.execute('SET FOREIGN_KEY_CHECKS = 1;')
                db_tar.commit()
                #print ("{}:目标表{}已清空！...".format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),tb_name))
            except Exception as db_error:
                cur_tar.execute('SET FOREIGN_KEY_CHECKS = 1;')
                db_tar.rollback()
                print ("{}:清空目标表{}失败！".format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),tb_name))

            """逐行迁移数据"""
            try:
                reader = DatabaseReader(db_src,table_name=tb_name)
                writer = DatabaseWriter(db_tar,table_name=tb_name)
                Task(reader,writer).start()
                db_tar.commit()
                tb_end_time = time.time()
                tb_totaltime = int(tb_end_time - tb_start_time)
                print ("{}:表{}数据迁移完成,总计耗时:{}秒...".format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),tb_name,tb_totaltime))
            except Exception as db_error:
                db_tar.rollback()
                try:
                    cur_src_dic.execute('UPDATE tm_ifmovedata SET IS_FAIL = 1 WHERE TABLE_NAME = \'{}\''.format(table_name))
                    cur_src_dic.commit()
                except Exception as db_errors:
                    db_tar.rollback()
                print ("{}:表{}数据迁移失败,错误原因{}!".format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),tb_name,db_error))

        """关闭游标"""
        cur_src_dic.close()
        cur_tar.close()

"""函数/过程/视图的迁移"""
is_allfun = input("""
请选择函数迁移范围(默认1)>>
1.自动扫描全部函数并迁移;
2.输入函数名;
3 or else.跳过函数迁移;
\033[1;33m函数迁移注意事项：\033[0m迁移部分函数可能造成视图创建失败！
""") or '1'

"""迁移函数"""
cur_src_dic = db_src.cursor(cursor=pymysql.cursors.DictCursor)
cur_tar = db_tar.cursor()

if is_allfun == '1':
    """获取所有函数"""
    sql_get_function = 'SELECT ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_SCHEMA=\'{}\' AND  ROUTINE_TYPE = \'FUNCTION\';'.format(in_src_dbname)

    cur_src_dic.execute(sql_get_function)
    src_fun = cur_src_dic.fetchall()
elif is_allfun == '2':
    in_pre_funname = input("请输入需要迁移函数名：")
    existsfun = cur_src_dic.execute('SELECT ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_SCHEMA=\'{}\' AND  ROUTINE_TYPE = \'FUNCTION\' AND ROUTINE_NAME = \'{}\' LIMIT 1;'.format(in_src_dbname,in_pre_funname))
    resfun = cur_src_dic.fetchone().get('ROUTINE_NAME')

    if resfun is not None:
        src_fun = ([{"ROUTINE_NAME":"{}".format(in_pre_funname)}])
    else:
        is_allfun = '-1'
        print("{}:函数名{}在源库不存在，跳过函数迁移！".format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),in_pre_funname))

if is_allfun in ('1', '2'):
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

is_allview = input("""
请选择视图迁移范围(默认1)>>
1.自动扫描全部视图并迁移;
2.输入视图名;
3 or else.跳过视图迁移;
\033[1;33m视图迁移注意事项：\033[0m视图依赖源库表和上一步函数，可能因表、函数缺失迁移失败;
""") or '1'

"""迁移视图"""
cur_src_dic = db_src.cursor(cursor=pymysql.cursors.DictCursor)
cur_tar = db_tar.cursor()

if is_allview == '1':
    """获取所有视图"""
    sql_get_view = 'SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA=\'{}\';'.format(in_src_dbname)

    cur_src_dic.execute(sql_get_view)
    src_view = cur_src_dic.fetchall()
elif is_allview == '2':
    in_pre_viewname = input("请输入需要迁移视图名：")
    existsview = cur_src_dic.execute('SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA=\'{}\' AND TABLE_NAME = \'{}\' LIMIT 1;'.format(in_src_dbname,in_pre_viewname))
    resview = cur_src_dic.fetchone().get('TABLE_NAME')

    if resview is not None:
        src_view = ([{"TABLE_NAME":"{}".format(in_pre_viewname)}])
    else:
        is_allview = '-1'
        print("{}:视图名{}在源库不存在，跳过视图迁移！".format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),in_pre_viewname))
if is_allview in ('1', '2'):
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

is_allproc = input("""
请选择过程迁移范围(默认1)>>
1.自动扫描全部过程并迁移;
2.输入过程名;
3 or else.跳过过程迁移.""") or '1'

"""迁移存储过程"""
cur_src_dic = db_src.cursor(cursor=pymysql.cursors.DictCursor)
cur_tar = db_tar.cursor()

if is_allproc == '1':
    """获取所有过程"""
    sql_get_proc = 'SELECT ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_SCHEMA=\'{}\' AND  ROUTINE_TYPE = \'PROCEDURE\';'.format(in_src_dbname)

    cur_src_dic.execute(sql_get_proc)
    src_proc = cur_src_dic.fetchall()

elif is_allproc == '2':
    in_pre_procname = input("请输入需要迁移过程名：")
    existsproc = cur_src_dic.execute('SELECT ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_SCHEMA=\'{}\' AND  ROUTINE_TYPE = \'PROCEDURE\' AND ROUTINE_NAME = \'{}\' LIMIT 1;'.format(in_src_dbname,in_pre_procname))
    resproc = cur_src_dic.fetchone().get('ROUTINE_NAME')

    if resproc is not None:
        src_proc = ([{"ROUTINE_NAME":"{}".format(in_pre_procname)}])
    else:
        is_allproc = '-1'
        print("{}:过程名{}在源库不存在，跳过过程迁移！".format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),in_pre_procname))

if is_allproc in ('1', '2'):
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

is_allevent = input("""
请选择作业迁移范围(默认1)>>
1.自动扫描全部作业并迁移;
2.输入作业名;
3 or else.跳过作业迁移.
\033[1;33m注意事项：\033[0m作业迁移后状态默认\033[1;31mDISABLE\033[0m，需要自行开启;
""") or '1'

"""迁移作业"""
cur_src_dic = db_src.cursor(cursor=pymysql.cursors.DictCursor)
cur_tar = db_tar.cursor()

if is_allevent == '1':
    """获取所有作业"""
    sql_get_event = 'SELECT EVENT_NAME FROM INFORMATION_SCHEMA.EVENTS WHERE EVENT_SCHEMA=\'{}\';'.format(in_src_dbname)

    cur_src_dic.execute(sql_get_event)
    src_event = cur_src_dic.fetchall()
elif is_allevent == '2':
    in_pre_eventname = input("请输入需要迁移作业名：")
    existsevent = cur_src_dic.execute('SELECT EVENT_NAME FROM INFORMATION_SCHEMA.EVENTS WHERE EVENT_SCHEMA=\'{}\' AND EVENT_NAME = \'{}\' LIMIT 1;'.format(in_src_dbname,in_pre_eventname))
    resevent = cur_src_dic.fetchone().get('EVENT_NAME')

    if resevent is not None:
        src_event = ([{"EVENT_NAME":"{}".format(in_pre_eventname)}])
    else:
        is_allevent = '-1'
        print("{}:作业名{}在源库不存在，跳过作业迁移！".format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),in_pre_eventname))

if is_allevent in ('1', '2'):
    for event_name_dic in src_event:
        event_name = event_name_dic.get('EVENT_NAME')
        """作业创建语句"""
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