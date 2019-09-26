import cx_Oracle

conn = cx_Oracle.connect('app/app@202.102.74.64:1521/app')
c = conn.cursor()
x = c.execute('select sysdate from dual')
print (x.fetchone())
c.close()
conn.close()
