import time
import pymysql
from dbutils.pooled_db import PooledDB
import threading
import json

# MySQL 连接池
try:
    # mysql_conn = pymysql.connect(host='pc-bp1zip05gl1b1ga3veo.rwlb.rds.aliyuncs.com', user='user_service', password='Lunz2017', database='locationcenter')
    mypool = PooledDB(
        creator=pymysql,  # 使用链接数据库的模块
        maxconnections=5,  # 连接池允许的最大连接数，0和None表示不限制连接数
        mincached=2,  # 初始化时，链接池中至少创建的空闲的链接，0表示不创建
        maxcached=5,  # 链接池中最多闲置的链接，0和None不限制
        maxshared=3,
        # 链接池中最多共享的链接数量，0和None表示全部共享。PS: 无用，因为pymysql和MySQLdb等模块的 threadsafety都为1，所有值无论设置为多少，_maxcached永远为0，所以永远是所有链接都共享。
        blocking=True,  # 连接池中如果没有可用连接后，是否阻塞等待。True，等待；False，不等待然后报错
        maxusage=None,  # 一个链接最多被重复使用的次数，None表示无限制
        setsession=[],  # 开始会话前执行的命令列表。如：["set datestyle to ...", "set time zone ..."]
        ping=1,
        # ping MySQL服务端，检查是否服务可用。
        #  如：0 = None = never,
        # 1 = default = whenever it is requested,
        # 2 = when a cursor is created,
        # 4 = when a query is executed,
        # 7 = always
        host="pc-bp1zip05gl1b1ga3veo.rwlb.rds.aliyuncs.com",
        port=3306,
        user="user_service",
        password="Lunz2017",
        charset="utf8",
        db="locationcenter"
    )
except Exception as e:
    print('mysql连接错误原因：',e)
# TiDB 连接池
try:
    # tidb_conn = pymysql.connect(host='gateway01.us-east-1.prod.aws.tidbcloud.com', port=4000, user='root', password='', database='test', ssl={'ca': '/etc/ssl/cert.pem'} )
    tipool = PooledDB(
        creator=pymysql,  # 使用链接数据库的模块
        maxconnections=5,  # 连接池允许的最大连接数，0和None表示不限制连接数
        mincached=2,  # 初始化时，链接池中至少创建的空闲的链接，0表示不创建
        maxcached=5,  # 链接池中最多闲置的链接，0和None不限制
        maxshared=3,
        # 链接池中最多共享的链接数量，0和None表示全部共享。PS: 无用，因为pymysql和MySQLdb等模块的 threadsafety都为1，所有值无论设置为多少，_maxcached永远为0，所以永远是所有链接都共享。
        blocking=True,  # 连接池中如果没有可用连接后，是否阻塞等待。True，等待；False，不等待然后报错
        maxusage=None,  # 一个链接最多被重复使用的次数，None表示无限制
        setsession=[],  # 开始会话前执行的命令列表。如：["set datestyle to ...", "set time zone ..."]
        ping=1,
        # ping MySQL服务端，检查是否服务可用。
        #  如：0 = None = never,
        # 1 = default = whenever it is requested,
        # 2 = when a cursor is created,
        # 4 = when a query is executed,
        # 7 = always
        host="gateway01.us-east-1.prod.aws.tidbcloud.com",
        port=4000,
        user="root",
        password="",
        charset="utf8",
        db="test",
        ssl={'ca': '/etc/ssl/cert.pem'}
    )
except Exception as e:
    print('tidb连接错误原因：',e)

# 需要迁移的表名
table_name = 'tmp_userlocation'

# 每批次迁移大小
batch_size = 1000

#开始行
offset = 0

# 开启多线程数量
thread_count = 2

# 准备 TiDB 插入语句
insert_stmt = f'INSERT INTO {table_name}(Id,LocationTime,LoginName,UserName,Lat,Lng,Speed,Address,DeviceIMEI,DeviceModel, \
                DeviceType) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'

start_time = time.time()

# 全局迁移行数获取函数
def distribute_data():
    global offset
    offset += batch_size
    return (offset)

# 数据迁移处理
def handle_batch(thread_name):
    batch = 'default'
    while len(batch) > 0:
        batch_start = time.time()
        offsets = distribute_data()
        with mypool.connection() as myconn:
            curmy = myconn.cursor()
            sql = f'SELECT Id,LocationTime,LoginName,UserName,Lat,Lng,Speed,Address,DeviceIMEI,DeviceModel, \
                        DeviceType FROM {table_name} ORDER BY Id LIMIT {offsets}, {batch_size};'
            curmy.execute(sql)
            batch = curmy.fetchall()
            curmy.close()
        with tipool.connection() as ticonn:
            curti = ticonn.cursor()
            try:
                curti.executemany(insert_stmt, tuple(batch))
                ticonn.commit()
            except Exception as e:
                print('sql执行错误：', e)
            curti.close()
            batch_time = time.time()
        print(f'线程 {thread_name}: 迁移进度: {offset} 条，耗时 {(batch_time - batch_start)[:2]} 秒！')

# 多线程开启
threads = []
for i in range(thread_count):
    print(f'线程 {i} 已开启!')
    thread = threading.Thread(target=handle_batch, args=(i,))
    thread.start()
    threads.append(thread)

for thread in threads:
    thread.join()

end_time = time.time()
print(f'迁移完毕，总计耗时: {(end_time - start_time)[:2]} 秒')