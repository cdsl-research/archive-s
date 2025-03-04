#!/usr/bin/env python3
import mysql.connector
import subprocess
import logging
import os
from preparation import convert_to_bytes
from preparation import output_dict_result
from preparation import output_result_comparison
from hash import calculate_md5
from read_DB import mysql_read_data
from insert_DB import mysql_insert_data
from delete import oldest_VM_path
from delete import del_check_data
from delete import del_data_after_three_month
from datetime import datetime
import shutil 

logging.basicConfig(format='[%(asctime)s]\t%(levelname)s\t%(message)s', datefmt='%d/%b/%Y:%H:%M:%S %z',filename='/var/log/archive.log',level=logging.INFO)
#DBに接続
conn = mysql.connector.connect(
    host="192.168.100.35",
    user="root",
    password="password",
    port="32000",
    database="VM_archive_DB"
    )
rows = mysql_read_data(conn)
#storage_dir = output_result_comparison()

#archive後期限切れVM削除
del_data_after_three_month(conn)


if not rows:
    logging.info("No data found in the database.")
    conn.close()
else:
    cur = conn.cursor()
    def archive():
        # rsync後のstorage内のディレクトリを取得
        # ターゲットディレクトリの取得
        for i,row in enumerate(rows):
            date_time, vmname,esxi,hash_value,user,VM_size = row 
            data,storage_capacity = output_dict_result()
            #print(row)
            conv_size = convert_to_bytes(VM_size)
            retry_count = 0
            max_retries = 2
            #print(esxi,hash_value)
            # rsyncするVMarchiveのディレクトリを取得
            archive_dir = f"/mnt/iscsi/target-mini2/VM-archive/{vmname}"
            # rsync先のHDDを選択
            # print(archive_destination)
            storage_dir = output_result_comparison()
            # ターゲットディレクトリの選択
            archive_destination = os.path.join(storage_dir, vmname)
            logging.info("Archiving process started. Rsync source:%s size: %s Rsync destination:%s", archive_dir, VM_size, archive_destination)

            #　リトライは２回まで
            while retry_count < max_retries:
                try:
                    if conv_size < storage_capacity[storage_dir]:
                        #print(archive_dir)
                        # rsyncコマンドの実行
                        rsync_command = ['sudo','rsync', '-av', archive_dir, storage_dir]
                        subprocess.run(rsync_command, check=True)
                        #print(f"rysnc_data{archive_dir}")
                        current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        hash_VM_archive = hash_value
                        hash_storage = calculate_md5(archive_destination)
                        VM_path = archive_destination.replace("/home/archive/","")
                        rsync_start_query = "INSERT INTO HISTORY_OF_HDD (date_time, VM_path, ESXi, user, Processing) VALUES (%s, %s, %s, %s, %s)"
                        cur.execute(rsync_start_query, (current_datetime, VM_path, esxi, user, 'Archiving process started'))
                        conn.commit()

                        if hash_storage == hash_VM_archive:
                            print("rsync completed successfully.\n")
                            #ここにrsync完了したらDB(INSERT_AFTER_DATA)に書き込む
                            mysql_insert_data(conn,current_datetime,VM_path,esxi,hash_storage,user,VM_size)
                            #rsync完了したら，VM_ARCHIVE_CHECKから該当データを消す
                            logging.info("Rsync completed successfully.    Rsync source: %s",archive_dir)
                            rsync_end_query = "INSERT INTO HISTORY_OF_HDD (date_time, VM_path, ESXi, user, Processing) VALUES (%s, %s, %s, %s, %s)"
                            cur.execute(rsync_end_query, (current_datetime, VM_path, esxi, user, 'Archiving process successfully'))
                            #オリジナルデータ削除
                            del_check_data(conn,vmname,esxi,user,hash_value)
                            conn.commit()
                            break
                        else:
                            print(hash_storage)
                            logging.warning(f"Hash values do not match for {vmname}. Retrying rsync...")
                            print(f"Hash values do not match for {vmname}. Retrying rsync...")
                            retry_count += 1
                    # ここで容量が足りなくなった時の処理を行う．
                    # データ削除
                    else:
                        print("Not enough storage space.")
                        logging.info("Not enough storage space.")
                        old_vm = oldest_VM_path(conn)
                        if os.path.exists(old_vm):
                            shutil.rmtree(old_vm)
                            print("old VM deleted successfully.")
                            logging.info("old VM deleted successfully.")
                        else:
                            print(f"Directory {old_vm} does not exist.")
                            logging.warning("Directory %s does not exist.", old_vm)
                except subprocess.CalledProcessError as e:
                    print(f"Error: rsync command failed with return code {e.returncode}")
                    print(e.output)
                    logging.error("Rsync failed with return code %s", e.returncode)
                    #logging.error(e.output)
                    retry_count += 1
            logging.info("Archiving process finished.")
        conn.close()
    archive()
