import pymysql

try:
    tidb_conn = pymysql.connect(host='gateway01.us-east-1.prod.aws.tidbcloud.com', port=4000, user='root', password='', database='test', ssl={'ca': '/etc/ssl/cert.pem'} )
except Exception as e:
    print('tidb连接错误原因：',e)

sql="SELECT * FROM user WHERE 1=0 LIMIT 2;"
stmt=f"INSERT INTO user_bak(id, name, age) values(%s, %s, %s)"
curtidb = tidb_conn.cursor()
curtidb.execute(sql)
src = curtidb.fetchall()
print(src)
print(len(src))
for data in src:
    curtidb.execute(stmt, tuple(data))
    tidb_conn.commit()
curtidb.close
tidb_conn.close()