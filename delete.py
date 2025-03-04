# データの削除を行う．
#　テーブルの記録もVM_ARCHIVEにあるデータもstorageにあるデータも消す．
import mysql.connector
from datetime import datetime, timedelta
from read_DB import mysql_read_data
from read_DB import mysql_read_after_data
import os
import shutil
import logging
logging.basicConfig(format='[%(asctime)s]\t%(levelname)s\t%(message)s', datefmt='%d/%b/%Y:%H:%M:%S %z',filename='/var/log/archive.log',level=logging.INFO)
conn = mysql.connector.connect(
    host="192.168.100.35",
    user="root",
    password="password",
    port="32000",
    database="VM_archive_DB"
)


cur = conn.cursor()

def del_data_after_three_month(conn):
    data_rows = mysql_read_after_data(conn)
    cursor = conn.cursor()
    for row in data_rows:
        entry_date = row[0]
        VM_path = row[1]
        esxi = row[2]
        user = row[4]
        del_VM = os.path.join("/home/archive/", VM_path)
        #print(entry_date)
        deletion_date = entry_date + timedelta(days=90)  # Assuming each month has 30 days
        # Check if deletion date has passed
        if datetime.now() > deletion_date:
            logging.info("deletion of records older than 3 months. VM_path: %s, User: %s", VM_path, user)
            current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            #ここに日付を超えたデータを削除するコードを
            query = "DELETE FROM AFTER_ARCHIVE_DATA WHERE date_time = %s"
            his_query = "INSERT INTO HISTORY_OF_HDD (date_time, VM_path, ESXi, user, Processing) VALUES (%s, %s, %s, %s, %s)"
            cur.execute(his_query, (current_datetime, VM_path, esxi,user,'records older than 3 months have been purged'))
            cursor.execute(query, (entry_date,))
            conn.commit()
            try:
                shutil.rmtree(del_VM)
                print(f"Deleted directory at path: {del_VM}")
                logging.info("Deleted directory at VM_path: %s", del_VM)
            except OSError as e:
                print(f"Error deleting directory at VM_path: {del_VM}: {e}")
                print(f"Deleted directory at VM_path: {del_VM}")
                logging.error("Error deleting directory at VM_path: %s: %s", del_VM, e)
                logging.info("Deleted directory at VM_path: %s", del_VM)

    # Close connection

def oldest_VM_path(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT VM_path FROM AFTER_ARCHIVE_DATA ORDER BY date_time ASC LIMIT 1")
    vm_path = cursor.fetchone()[0]  # Fetching the first column of the first row
    cursor.close()

    # Close connection
    
    oldest_vm_path = os.path.join("/home/archive/", vm_path)
    #print(oldest_vm_path)
    return oldest_vm_path


def del_check_data(conn,vm_name,esxi,user,hash_value):# DBのテーブルVM_ARCHIVE_CHECKと元のデータを消す
    logging.info("Starting deletion of data from VM_ARCHIVE_CHECK and associated directory.")
    cursor = conn.cursor()
    query = "DELETE FROM VM_ARCHIVE_CHECK WHERE hash_value = %s"
    cursor.execute(query, (hash_value,))
    conn.commit()

    base_dir = "/mnt/iscsi/target-mini2/VM-archive/"
    vm_dir = os.path.join(base_dir, vm_name)
    try:
        if os.path.exists(vm_dir):
            current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            shutil.rmtree(vm_dir)
            his_del_origin_query = "INSERT INTO HISTORY_OF_HDD (date_time, VM_path, ESXi, user, Processing) VALUES (%s, %s, %s, %s, %s)"
            cursor.execute(his_del_origin_query, (current_datetime, vm_dir, esxi, user, 'Deleted directory'))
            print(f"Deleted directory: {vm_dir}")
            logging.info("Deleted directory: %s", vm_dir)
            conn.commit()
        else:
            print(f"Directory does not exist: {vm_dir}")
            logging.info("Directory does not exist: %s", vm_dir)
    except Exception as e:
        print(f"Error deleting directory {vm_dir}: {e}")
        logging.error("Error deleting directory %s: %s", vm_dir, e)

    
#del_data_after_three_month(conn)
#oldest_VM_path(conn)
