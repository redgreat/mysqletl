#!/usr/bin/env python3
#-*- coding:utf-8 -*-

import pymysql
import time
import datetime

"""入参名称定义"""

in_src_dbname = input("请输入源数据库名称(默认whcenter)>>>：") or 'whcenter'
in_tar_dbname = input("请输入目标数据库名称(默认testwong)>>>：") or 'testwong'
in_betchnum = input("请输入每批次迁移表数据行数(默认1000)") or 1000

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
                             port=4000,
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

"""开启游标"""
"""
cur_src = db_src.cursor()
cur_src_dic = db_src.cursor(cursor=pymysql.cursors.DictCursor)
cur_tar = db_tar.cursor()
cur_tar_dict = db_tar.cursor(cursor=pymysql.cursors.DictCursor)
"""

"""判断目标库是否存在"""
# sql_check_tardb = """SELECT 1
# FROM INFORMATION_SCHEMA.SCHEMATA
# WHERE SCHEMA_NAME=\'{}\';""".format(in_tar_dbname)

"""建库语句"""
# sql_get_srcdb = """SHOW CREATE DATABASE \'{}\';""".format(in_tar_dbname)

"""获所有表"""
sql_get_tables = """SELECT TABLE_NAME 
           FROM INFORMATION_SCHEMA.TABLES 
           WHERE TABLE_SCHEMA=\'{}\' 
           AND TABLE_TYPE=\'BASE TABLE\'""".format(in_src_dbname)

cur_src_dic = db_src.cursor(cursor=pymysql.cursors.DictCursor)
cur_src_dic.execute(sql_get_tables)
src_table = cur_src_dic.fetchall()
cur_src_dic.close()

# cur_src_dic = db_src.cursor(cursor=pymysql.cursors.DictCursor)
# cur_tar = db_tar.cursor()
#
# try:
#     if not cur_tar.execute(sql_check_tardb):
#         cur_src_dic.execute(sql_get_srcdb)
#         db_desc = cur_src_dic.fetchone().get('Create Dataabse').replace(in_src_dbname,in_tar_dbname)
#         print (db_desc)
#         #cur_tar.execute(db_desc)
#         db_tar.commit()
#         print ("{}:目标数据库{}在目标实例不存在，自动创建成功...").format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),in_tar_dbname)
#     cur_src_dic.execute(sql_get_tables)
#     src_table = cur_src_dic.fetchall()
#
#     cur_src_dic.close()
#     cur_tar.close()
# except Exception as db_error:
#     db_tar.rollback()
#     print ("{}:目标数据库{}在目标实例不存在，并创建失败！").format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),in_tar_dbname)

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

    """
    cur_src = db_src.cursor()
    cur_src.execute(sql_showtable)
    tb_desc = cur_src.fetchone()[1]
    cur_src.close()
    """

    """获取创建表语句"""
    cur_src_dic.execute(sql_show_table)
    tb_desc = cur_src_dic.fetchone().get('Create Table')

    """创建表语句生成"""
    sql_create_tartable = """DROP TABLE IF EXISTS `{}`;\n{};""".format(tb_name,tb_desc)

    """检查源表结构 (仅目标为异构数据库使用，入TiDB)"""
    """
    sql_check_srctable = "SELECT 1 \
    FROM INFORMATION_SCHEMA.TABLES A, \
    INFORMATION_SCHEMA.COLUMNS B \
    WHERE A.TABLE_SCHEMA=B.TABLE_SCHEMA \
    AND A.TABLE_NAME=B.TABLE_NAME \
    AND A.TABLE_SCHEMA=\'{}\' \
    AND B.CHARACTER_SET_NAME NOT IN ('utf8','utf8mb4')".format(in_src_dbname)
    """

    """检查目标表是否有此表"""
    sql_check_tartable = """SELECT TABLE_NAME
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = \'{}\'
    AND TABLE_TYPE = \'BASE TABLE\'
    AND TABLE_NAME = \'{}\';""".format(in_tar_dbname, tb_name)

    if not cur_tar.execute(sql_check_tartable):
        try:
            print ("{}:执行建表{}...".format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),tb_name))
            cur_tar.execute(sql_create_tartable)
            db_tar.commit()
            print ("{}:表{}创建成功...".format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),tb_name))
            res = 1
        except Exception as db_error:
            db_tar.rollback()
            res = -1
            print( "{}:表{}迁移失败，建表语句：\n{}\n错误：\"{}\"".format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())), tb_name, sql_create_tartable, db_error))
        pass
    else:
        print ("{}:目标库表{}已存在，直接进行数据迁移...".format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),tb_name))
    if res == 0 or res == 1:

        """获取表数据行数"""
        #sql_count_table = """SELECT COUNT(1) AS cnt FROM `{}`;""".format(tb_name)
        #cur_src_dic.execute(sql_count_table)
        #tb_cont = cur_src_dic.fetchone().get('cnt')

        """获取表所有列"""
        sql_column_table = """SELECT CONCAT('`',GROUP_CONCAT(COLUMN_NAME SEPARATOR '`,`'),'`') AS src_column
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = \'{}\'
          AND TABLE_NAME = \'{}\';""".format(in_src_dbname, tb_name)

        cur_src_dic.execute(sql_column_table)
        src_column = cur_src_dic.fetchone().get('src_column')

        """清空目标表数据"""
        sql_truncate_tartable = """TRUNCATE TABLE `{}`;""".format(tb_name)
        try:
            cur_tar.execute(sql_truncate_tartable)
            db_tar.commit()
            print ("{}:目标表{}已清空！...".format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),tb_name))
        except Exception as db_error:
            db_tar.rollback()
            print ("{}:清空目标表{}失败！".format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),tb_name))

        """逐行迁移数据"""
        i = 0
        cnt = 0
        is_continue = 1
        #for i in range(0,tb_cont,in_betchnum):
        while is_continue != 0:

            sql_values_srcdata = """SELECT {}
            FROM `{}`
            LIMIT {},{};""".format(src_column,tb_name,i,in_betchnum)
            i = i + in_betchnum
            sql_values_tardata = """INSERT INTO `{}` ({}) VALUES """.format(tb_name,src_column)

            try:
                cur_src_dic.execute(sql_values_srcdata)
                dic_values_srcdata = cur_src_dic.fetchall()
                if dic_values_srcdata:
                    for row_data in dic_values_srcdata:
                        sql_values_tardata += '(' + ','.join('\'' +str(v) +'\'' if isinstance(v,str) or isinstance(v,datetime.datetime) else str(v) for v in row_data.values()) + '),'
                    sql_values_tardata = sql_values_tardata.strip(',')
                    sql_values_tardata += ";"
                    sql_values_tardata = sql_values_tardata.replace('None', 'NULL')
                    #print(sql_values_tardata)
                    b_cnt = cur_tar.execute(sql_values_tardata)
                    db_tar.commit()
                else:
                    is_continue = 0
                cnt += b_cnt
                totalcnt += b_cnt
                t_time = int(time.time() - db_start_time) or 1
                spd = int(totalcnt/t_time)
                print ("{}:表{}已迁移数据{}行,平均速度{}行/秒...".format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),tb_name,cnt,spd))
            except Exception as db_error:
                db_tar.rollback()
                print ("{}:表{}迁移中断！错误：{}".format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),tb_name,db_error))
            continue

        """单表迁移时间"""
        tb_end_time = time.time()
        tb_totaltime = int(tb_end_time - tb_start_time)
        t_time = int(time.time() - db_start_time) or 1
        spd = int(totalcnt/t_time)
        print("{}:表{}数据迁移完成,总计耗时:{}秒,平均速度{}行/秒!...".format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),tb_name,tb_totaltime,spd))
    """关闭游标"""
    cur_src_dic.close()
    cur_tar.close()

"""关闭数据库连接"""
db_end_time = time.time()
db_deltatime = db_end_time - db_start_time
db_totalhour = int(db_start_time_deltatime / 3600)
db_totalminute = int((db_deltatime -db_totalhour * 3600) / 60)
db_totalsecond = int(db_deltatime - db_totalhour * 3600 - db_totalminute * 60)
t_time = int(time.time() - db_start_time) or 1
spd = int(totalcnt/t_time)
print("{}:已将源库{}全部表迁移至{},总计耗时:{}小时{}分{}秒,平均速度{}行/秒!".format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),in_src_dbname,in_tar_dbname,db_totalhour,db_totalminute,db_totalsecond,spd))
db_src.close()
db_tar.close()