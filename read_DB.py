import mysql.connector

conn = mysql.connector.connect(
    host="192.168.100.35",
    user="root",
    password="password",
    port="32000",
    database="VM_archive_DB"
)

def mysql_read_data(conn):
    curs = conn.cursor()
    # テーブルの内容を表示
    curs.execute("SELECT date_time, VMname, ESXi, hash_value, user, VM_size from VM_ARCHIVE_CHECK")
    rows = curs.fetchall()
    curs.close()
    #print(rows)
    return rows

def mysql_read_after_data(conn):
    curs = conn.cursor()
    # テーブルの内容を表示
    curs.execute("SELECT * from AFTER_ARCHIVE_DATA")
    rows = curs.fetchall()
    curs.close()
    #print(rows)
    return rows
#mysql_read_data(conn)