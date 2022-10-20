#!/usr/bin/env python3
# Mysql 数据迁移脚本
# Copyright (wangcw)
#-*- coding:utf-8 -*-

import pymysql
import time
import datetime
import logging
import logging.handlers

logger = logging.getLogger('pyetllog')
logger.setLevel(logging.DEBUG)

rf_handler = logging.handlers.TimedRotatingFileHandler('../logs/all.log', when='midnight', interval=1, backupCount=7, atTime=datetime.time(0, 0, 0, 0))
rf_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

f_handler = logging.FileHandler('../logs/error.log')
f_handler.setLevel(logging.ERROR)
f_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(filename)s[:%(lineno)d] - %(message)s"))

logger.addHandler(rf_handler)
logger.addHandler(f_handler)

"""提示信息"""
prompt = """
\033[1;33m迁移注意事项\033[0m
1.如果选择不迁移全部源表，视图可能因表依赖原因迁移失败;
2.作业迁移后状态默认\033[1;31mDISABLE\033[0m，需要自行开启;
"""
print (prompt)

"""入参名称定义"""
in_src_dbname = input("请输入源数据库名称(默认serviceordercenter)>>>：") or 'serviceordercenter'
in_tar_dbname = input("请输入目标数据库名称(默认serviceordercenteretl)>>>：") or 'serviceordercenteretl'
in_betchnum = input("请输入每批次迁移表数据行数(默认1000)") or 1000
is_alltable = input("是否迁移全部表？('Y'自动扫描全部表)/'N'只迁移配置表(tm_ifmovedata)内指定表/输入表名迁移指定表)") or 'N'
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
    logger.error("源库连接失败,错误原因:{}".format(db_error))
try:
    db_tar = pymysql.connect(host='',
                             database=in_tar_dbname,
                             user='',
                             password='',
                             charset='utf8')

except Exception as db_error:
    print ("{}:目标库连接失败：错误原因：{}".format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),db_error))
    logger.error("目标库连接失败,错误原因:{}".format(db_error))
db_start_time = time.time()
totalcnt = 0

"""写入迁移结构测试数据"""

# DROP TABLE IF EXISTS `serviceordercenter`.`tm_ifmovedata`;
# CREATE TABLE `serviceordercenter`.`tm_ifmovedata` (
#   `ID` int(10) unsigned NOT NULL AUTO_INCREMENT COMMENT '自增主键',
#   `TABLE_NAME` varchar(50) DEFAULT NULL COMMENT '表名称',
#   `IF_DATA` tinyint(1) NOT NULL DEFAULT '0' COMMENT '是否迁移数据(0否1是)',
#   `IS_FAIL` smallint(6) NOT NULL DEFAULT '0' COMMENT '迁移是否失败',
#   `INSTER_TIME` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
#   PRIMARY KEY (`ID`),
#   KEY `1` (`TABLE_NAME`)
# ) ENGINE=InnoDB AUTO_INCREMENT=512 DEFAULT CHARSET=utf8 COMMENT='数据迁移是否携带数据(只迁移结构OR迁移结构和数据)'
# ;
# TRUNCATE TABLE `serviceordercenter`.`tm_ifmovedata`;
# INSERT INTO `serviceordercenter`.`tm_ifmovedata`(TABLE_NAME,IF_DATA)
# SELECT `TABLE_NAME`,
#        CASE WHEN `TABLE_ROWS` < 500 THEN 1 ELSE 0 END AS `IF_DATA`
# FROM `INFORMATION_SCHEMA`.`TABLES`
# WHERE `TABLE_SCHEMA`='serviceordercenter';

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
cur_src_dic = db_src.cursor(cursor=pymysql.cursors.DictCursor)
if is_alltable == 'Y':
    """获所有表"""
    sql_get_tables = 'SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=\'{}\' AND TABLE_TYPE=\'BASE TABLE\''.format(in_src_dbname)

    cur_src_dic.execute(sql_get_tables)
    src_table = cur_src_dic.fetchall()

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
elif is_alltable == 'N':
    """获所有表"""
    sql_get_tables = 'SELECT TABLE_NAME FROM tm_ifmovedata WHERE 1=1 ;'

    cur_src_dic.execute(sql_get_tables)
    src_table = cur_src_dic.fetchall()

else:
    src_table = ([{"TABLE_NAME":"{}".format(is_alltable)}])
cur_src_dic.close()

"""循环表"""
for tb_name_dic in src_table:

    tb_start_time = time.time()
    res = 0
    tb_name = tb_name_dic.get('TABLE_NAME')
    """获取表结构"""
    sql_show_table = 'SHOW CREATE TABLE `{}`;'.format(tb_name)

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
    sql_drop_tartable = 'DROP TABLE IF EXISTS `{}`;'.format(tb_name)
    sql_create_tartable = '{};'.format(tb_desc)

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
    sql_check_tartable = 'SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = \'{}\' AND TABLE_TYPE = \'BASE TABLE\' AND TABLE_NAME = \'{}\';'.format(in_tar_dbname,tb_name)

    """检查源库配置表(0只迁表结构,1迁移数据和表结构)"""
    sql_withdata_srctable = 'SELECT `IF_DATA` FROM `tm_ifmovedata` WHERE `TABLE_NAME` = \'{}\' LIMIT 1;'.format(tb_name)

    if not cur_tar.execute(sql_check_tartable):
        try:
            #print ("{}:执行建表{}...".format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),tb_name))
            logger.debug("执行建表{}...".format(tb_name))
            cur_tar.execute('SET FOREIGN_KEY_CHECKS = 0;')
            cur_tar.execute(sql_drop_tartable)
            cur_tar.execute(sql_create_tartable)
            cur_tar.execute('SET FOREIGN_KEY_CHECKS = 1;')
            db_tar.commit()
            #print ("{}:表{}创建成功...".format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),tb_name))
            logger.debug("表{}创建成功...".format(tb_name))
            res = 1
        except Exception as db_error:
            cur_tar.execute('SET FOREIGN_KEY_CHECKS = 1;')
            db_tar.rollback()
            res = -1
            print( "{}:表{}迁移失败，建表语句：\n{}\n错误：\"{}\"".format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())), tb_name, sql_create_tartable, db_error))
            logger.error("表{}迁移失败，建表语句：\n{}\n错误：\"{}\"".format(tb_name,sql_create_tartable,db_error))
        pass
    else:
        #print ("{}:目标库表{}已存在，直接进行数据迁移...".format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),tb_name))
        logger.debug("目标库表{}已存在，直接进行数据迁移...".format(tb_name))
    cur_src_dic.execute(sql_withdata_srctable)
    if_data_dic = cur_src_dic.fetchone()
    if_data = if_data_dic.get('IF_DATA') if if_data_dic is not None else 0

    if (res == 0 or res == 1) and (if_data != 0):

        """获取表数据行数"""
        #sql_count_table = """SELECT COUNT(1) AS cnt FROM `{}`;""".format(tb_name)
        #cur_src_dic.execute(sql_count_table)
        #tb_cont = cur_src_dic.fetchone().get('cnt')

        """获取表所有列"""
        sql_column_table = 'SELECT COLUMN_NAME AS src_column_name,COLUMN_TYPE AS src_column_type FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = \'{}\' AND TABLE_NAME = \'{}\' ORDER BY ORDINAL_POSITION;'.format(in_src_dbname,tb_name)

        cur_src_dic.execute(sql_column_table)
        src_columns = cur_src_dic.fetchall()
        src_columnsss = ''
        for src_column in src_columns:
            src_columnss = src_column.get('src_column_name')
            src_columnsss += '`' + src_columnss + '`,'
        src_column_concat = src_columnsss.strip(',')

        #src_column_concat = ','.join(['`' + str(src_column.get('src_column_name')) + '`' for src_column in src_columns])

        """清空目标表数据"""
        sql_truncate_tartable = 'TRUNCATE TABLE `{}`;'.format(tb_name)
        try:
            cur_tar.execute('SET FOREIGN_KEY_CHECKS = 0;')
            cur_tar.execute(sql_truncate_tartable)
            cur_tar.execute('SET FOREIGN_KEY_CHECKS = 1;')
            db_tar.commit()
            #print ("{}:目标表{}已清空！...".format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),tb_name))
            logger.debug("目标表{}已清空！...".format(tb_name))
        except Exception as db_error:
            cur_tar.execute('SET FOREIGN_KEY_CHECKS = 1;')
            db_tar.rollback()
            print ("{}:清空目标表{}失败！".format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),tb_name))
            logger.error("清空目标表{}失败！".format(tb_name))
        """逐行迁移数据"""
        i = 0
        cnt = 0
        is_continue = 1
        while is_continue != 0:

            sql_values_srcdata = 'SELECT {} FROM `{}` LIMIT {},{};'.format(src_column_concat,tb_name,i,in_betchnum)
            i = i + in_betchnum
            sql_values_tardata = 'INSERT INTO `{}` ({}) VALUES '.format(tb_name,src_column_concat)

            try:
                cur_src_dic.execute(sql_values_srcdata)
                dic_values_srcdata = cur_src_dic.fetchall()
                b_cnt = 0
                if dic_values_srcdata:
                    row_strc = ''
                    for row_data in dic_values_srcdata:
                        row_strs = ''
                        for columnnt in src_columns:
                            cv = row_data.get(columnnt.get('src_column_name'))
                            ct = columnnt.get('src_column_type')
                            if ct.count('char') > 0 or ct.count('text') > 0 or ct.count('json') > 0:
                                if cv is not None:
                                    cv = cv.replace('\\','\\\\').replace('\'','\\\'').replace('\"','\\\"').replace('\\','\\')
                                row_str = '\'' + str(cv) + '\''
                            #elif ct.count('text') > 0:
                            #    if cv is not None:
                            #        cv = cv.replace('\\','\\\\').replace('\'','\\\'').replace('\"','\\\"')
                            #    row_str = '\'' + str(cv) + '\''
                            #elif ct.count('json') > 0 and cv != '' and cv is not None:
                            #    row_str = str('\'' + str(cv.replace('\\','\\\\').replace('\'','\\\'').replace('\"','\\\"')) + '\'')
                            elif ct.count('time') > 0 or ct.count('date') > 0:
                                row_str = '\'' + str(cv) + '\''
                            elif ct.count('tinyint(1)') > 0:
                                row_str = str(int(cv))
                            elif ct.count('bit') > 0:
                                row_str = int.from_bytes(cv,'little')
                            else:
                                row_str = str(cv)
                            row_strs = row_strs + str(row_str) + ','
                        row_strc = row_strc + '(' + row_strs.strip(',') + '),'
                    sql_values_tardata = sql_values_tardata + str(row_strc.replace('None', 'NULL').replace('\'NULL\'', 'NULL').strip(',')) + ";"
                    b_cnt = cur_tar.execute(sql_values_tardata)
                    db_tar.commit()
                else:
                    is_continue = 0
                cnt += b_cnt
                totalcnt += b_cnt
                t_time = int(time.time() - db_start_time) or 1
                spd = int(totalcnt/t_time)
                print ("{}:表{}已迁移数据{}行,平均速度{}行/秒...".format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),tb_name,cnt,spd))
                logger.debug("表{}已迁移数据{}行,平均速度{}行/秒...".format(tb_name,cnt,spd))
            except Exception as db_error:
                db_tar.rollback()
                print ("{}:表{}迁移中断！执行语句:{},错误:{}".format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),tb_name,sql_values_tardata.replace('\\u','\\\\u'),db_error))
                logger.error("表{}迁移中断！执行语句:{},错误:{}".format(tb_name,sql_values_tardata.replace('\\u','\\\\u'),db_error))
            continue

        """单表迁移时间"""
        tb_end_time = time.time()
        tb_totaltime = int(tb_end_time - tb_start_time)
        t_time = int(time.time() - db_start_time) or 1
        spd = int(totalcnt/t_time)
        print("{}:表{}数据迁移完成,总计耗时:{}秒,平均速度{}行/秒!...".format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),tb_name,tb_totaltime,spd))
        logger.debug("表{}数据迁移完成,总计耗时:{}秒,平均速度{}行/秒!...".format(tb_name,tb_totaltime,spd))
    """关闭游标"""
    cur_src_dic.close()
    cur_tar.close()

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
        logger.debug("函数{}创建成功...".format(fun_name))
    except Exception as db_error:
        db_tar.rollback()
        print ("{}:函数{}创建失败，原因：{}".format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),fun_name,db_error))
        logger.debug("函数{}创建失败，原因：{}".format(fun_name,db_error))
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
        logger.debug("视图{}创建成功...".format(view_name))
    except Exception as db_error:
        db_tar.rollback()
        print ("{}:视图{}创建失败，原因：{}".format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),view_name,db_error))
        logger.debug("视图{}创建失败，原因：{}".format(view_name,db_error))
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
        logger.debug("存储过程{}创建成功...".format(proc_name))
    except Exception as db_error:
        db_tar.rollback()
        print ("{}:存储过程{}创建失败，原因：{}".format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),proc_name,db_error))
        logger.debug("存储过程{}创建失败，原因：{}".format(proc_name,db_error))
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
        logger.debug("作业{}创建成功...".format(event_name))
    except Exception as db_error:
        db_tar.rollback()
        print ("{}:作业{}创建失败，原因：{}".format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),event_name,db_error))
        logger.debug("作业{}创建失败，原因：{}".format(event_name,db_error))
    pass
cur_src_dic.close()
cur_tar.close()

"""关闭数据库连接"""
db_end_time = time.time()
db_deltatime = db_end_time - db_start_time
db_totalhour = int(db_deltatime / 3600)
db_totalminute = int((db_deltatime -db_totalhour * 3600) / 60)
db_totalsecond = int(db_deltatime - db_totalhour * 3600 - db_totalminute * 60)
t_time = int(time.time() - db_start_time) or 1
spd = int(totalcnt/t_time)
print("{}:已将源库{}全部表迁移至{},总计耗时:{}小时{}分{}秒,平均速度{}行/秒!".format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),in_src_dbname,in_tar_dbname,db_totalhour,db_totalminute,db_totalsecond,spd))
logger.debug("已将源库{}全部表迁移至{},总计耗时:{}小时{}分{}秒,平均速度{}行/秒!".format(in_src_dbname,in_tar_dbname,db_totalhour,db_totalminute,db_totalsecond,spd))
db_src.close()
db_tar.close()