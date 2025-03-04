import mysql.connector
from read_DB import mysql_read_data


def mysql_insert_data(conn,time,VM_path,ESXi,hash_value,user,VM_size):
    curs = conn.cursor()
    insert_query = """
  INSERT INTO `AFTER_ARCHIVE_DATA` (date_time, VM_path, ESXi, hash_value, user, VM_size)
    VALUES (%s, %s, %s, %s, %s,%s)
    """
    data = (time, VM_path, ESXi, hash_value, user, VM_size)
    curs.execute(insert_query,data)
    conn.commit()  # 変更をコミット
    

#current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#data = (current_datetime,"test","test","test")
#mysql_insert_data(conn,data)