#!/usr/bin/env python3
#-*- coding:utf-8 -*-

import pymysql
import time
from pyetl.task import Task
from pyetl.reader import DatabaseReader
from pyetl.writer import DatabaseWriter

"""输入自定义参数"""
in_src_dbname = input("请输入源数据库名称(默认serviceordercenter)>>>：") or 'serviceordercenter'
in_tar_dbname = input("请输入目标数据库名称(默认serviceordercenteretl)>>>：") or 'serviceordercenteretl'
# in_tbname = input("请输入需要迁移表名称(默认空，全部迁移)>>>：") or ''

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
    db_tar = pymysql.connect(host='m',
                             #port=4000,
                             database=in_tar_dbname,
                             user='',
                             password='',
                             charset='utf8')

    #db_tar = pymysql.connect(host='',
    #                         port=3306,
    #                         database='testwong',
    #                         user='root',
    #                         password='',
    #                         charset='utf8')

except Exception as db_error:
    print ("{}:目标库连接失败：错误原因：{}".format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),db_error))
db_start_time = time.time()
totalcnt = 0

"""获所有表"""
sql_get_tables = """SELECT TABLE_NAME 
           FROM INFORMATION_SCHEMA.TABLES 
           WHERE TABLE_SCHEMA=\'{}\' 
           AND TABLE_TYPE=\'BASE TABLE\'""".format(in_src_dbname)

cur_src_dic = db_src.cursor(cursor=pymysql.cursors.DictCursor)
cur_src_dic.execute(sql_get_tables)
src_table = cur_src_dic.fetchall()
cur_src_dic.close()

"""循环表"""
for tb_name_dic in src_table:

    tb_start_time = time.time()
    res = 0
    tb_name = tb_name_dic.get('TABLE_NAME')
    """获取表结构"""
    sql_show_table = """SHOW CREATE TABLE `{}`;""".format(tb_name)

    """开启游标(防止数据迁移占用连接时间太久，每张表单独开一次)"""
    cur_src_dic = db_src.cursor(cursor=pymysql.cursors.DictCursor)
    cur_tar = db_tar.cursor()

    """获取创建表语句"""
    cur_src_dic.execute(sql_show_table)
    tb_desc = cur_src_dic.fetchone().get('Create Table')

    """创建表语句生成"""
    sql_create_tartable = """SET FOREIGN_KEY_CHECKS = 0; DROP TABLE IF EXISTS `{}`;\n{}; SET FOREIGN_KEY_CHECKS = 1;""".format(tb_name,tb_desc)

    """检查目标表是否有此表"""
    sql_check_tartable = """SELECT TABLE_NAME
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = \'{}\'
    AND TABLE_TYPE = \'BASE TABLE\'
    AND TABLE_NAME = \'{}\';""".format(in_tar_dbname,tb_name)

    """检查源库配置表(0只迁表结构,1迁移数据和表结构)"""
    sql_withdata_srctable = """SELECT `IF_DATA`
    FROM `tm_ifmovedata`
    WHERE `TABLE_NAME` = \'{}\'
    LIMIT 1;""".format(tb_name)

    """创建目标表结构"""
    if not cur_tar.execute(sql_check_tartable):
        try:
            #print ("{}:执行建表{}...".format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),tb_name))
            cur_tar.execute(sql_create_tartable)
            db_tar.commit()
            #print ("{}:表{}创建成功...".format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),tb_name))
            res = 1
        except Exception as db_error:
            db_tar.rollback()
            res = -1
            print( "{}:表{}迁移失败，建表语句：\n{}\n错误：\"{}\"".format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())), tb_name, sql_create_tartable, db_error))
        pass
    else:
        pass
        #print ("{}:目标库表{}已存在，直接进行数据迁移...".format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),tb_name))
    cur_src_dic.execute(sql_withdata_srctable)
    if_data = cur_src_dic.fetchone().get('IF_DATA')

    if (res == 0 or res == 1) and (if_data != 0):

        """清空目标表数据"""
        sql_truncate_tartable = """SET FOREIGN_KEY_CHECKS = 0; TRUNCATE TABLE `{}`; SET FOREIGN_KEY_CHECKS = 1;""".format(tb_name)
        try:
            cur_tar.execute(sql_truncate_tartable)
            db_tar.commit()
            #print ("{}:目标表{}已清空！...".format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),tb_name))
        except Exception as db_error:
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
                cur_src_dic.execute("""UPDATE tm_ifmovedata SET IS_FAIL = 1 WHERE TABLE_NAME = \'{}\'""".format(table_name))
                cur_src_dic.commit()
            except Exception as db_errors:
                db_tar.rollback()
            print ("{}:表{}数据迁移失败,错误原因{}!".format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),tb_name,db_error))

    """关闭游标"""
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