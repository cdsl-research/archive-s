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
import requests  # Slackへのリクエスト用

# Slack Webhook URL
SLACK_WEBHOOK_URL = 'https://hooks.slack.com/services/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'

# Slackにメッセージを送信する関数
def send_slack_notification(message):
    try:
        payload = {'text': message}
        response = requests.post(SLACK_WEBHOOK_URL, json=payload)
        if response.status_code != 200:
            logging.error(f"Failed to send message to Slack. Status code: {response.status_code}")
    except Exception as e:
        logging.error(f"Error while sending message to Slack: {e}")

logging.basicConfig(format='[%(asctime)s]\t%(levelname)s\t%(message)s', datefmt='%d/%b/%Y:%H:%M:%S %z', filename='/var/log/archive.log', level=logging.INFO)

# DBに接続
conn = mysql.connector.connect(
    host="192.168.100.35",
    user="root",
    password="password",
    port="32000",
    database="VM_archive_DB"
)
rows = mysql_read_data(conn)

# 保持期間が過ぎたVMの削除と通知
def delete_expired_vms():
    deleted_vms = del_data_after_three_month(conn)
    if deleted_vms:
        for vm in deleted_vms:
            send_slack_notification(f"VM {vm} has been deleted after exceeding the retention period.")
            logging.info("VM %s has been deleted after exceeding the retention period.", vm)

# archive後期限切れVM削除
delete_expired_vms()

if not rows:
    logging.info("No data found in the database.")
    send_slack_notification("No data found in the database.")
    conn.close()
else:
    cur = conn.cursor()

    def archive():
        for i, row in enumerate(rows):
            date_time, vmname, esxi, hash_value, user, VM_size = row
            data, storage_capacity = output_dict_result()
            conv_size = convert_to_bytes(VM_size)
            retry_count = 0
            max_retries = 2
            archive_dir = f"/mnt/iscsi/target-mini2/VM-archive/{vmname}"
            storage_dir = output_result_comparison()
            archive_destination = os.path.join(storage_dir, vmname)
            logging.info("Archiving process started. Rsync source:%s size: %s Rsync destination:%s", archive_dir, VM_size, archive_destination)

            # リトライは2回まで
            while retry_count < max_retries:
                try:
                    if conv_size < storage_capacity[storage_dir]:
                        rsync_command = ['sudo', 'rsync', '-av', archive_dir, storage_dir]
                        subprocess.run(rsync_command, check=True)
                        current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        hash_VM_archive = hash_value
                        hash_storage = calculate_md5(archive_destination)
                        VM_path = archive_destination.replace("/home/archive/", "")
                        rsync_start_query = "INSERT INTO HISTORY_OF_HDD (date_time, VM_path, ESXi, user, Processing) VALUES (%s, %s, %s, %s, %s)", 
                        cur.execute(rsync_start_query, (current_datetime, VM_path, esxi, user, 'Archiving process started'))
                        conn.commit()

                        if hash_storage == hash_VM_archive:
                            print("rsync completed successfully.\n")
                            mysql_insert_data(conn, current_datetime, VM_path, esxi, hash_storage, user, VM_size)
                            logging.info("Rsync completed successfully.    Rsync source: %s", archive_dir)
                            rsync_end_query = "INSERT INTO HISTORY_OF_HDD (date_time, VM_path, ESXi, user, Processing) VALUES (%s, %s, %s, %s, %s)"
                            cur.execute(rsync_end_query, (current_datetime, VM_path, esxi, user, 'Archiving process successfully'))
                            del_check_data(conn, vmname, esxi, user, hash_value)
                            conn.commit()
                            # Slack通知: アーカイブ成功とVM名
                            send_slack_notification(f"Archiving process completed successfully for VM: {vmname} ({VM_size})")
                            break
                        else:
                            logging.warning(f"Hash values do not match for {vmname}. Retrying rsync...")
                            retry_count += 1
                    else:
                        print("Not enough storage space.")
                        logging.info("Not enough storage space.")
                        old_vm = oldest_VM_path(conn)
                        if os.path.exists(old_vm):
                            shutil.rmtree(old_vm)
                            logging.info("Old VM %s deleted successfully.", old_vm)
                            # Slack通知: 削除されたVM
                            send_slack_notification(f"Old VM {old_vm} deleted successfully to free up space.")
                        else:
                            logging.warning("Directory %s does not exist.", old_vm)
                except subprocess.CalledProcessError as e:
                    logging.error("Rsync failed with return code %s", e.returncode)
                    retry_count += 1
                    send_slack_notification(f"Archiving process failed for {vmname}. Retrying... (Attempt {retry_count}/{max_retries})")
            logging.info("Archiving process finished.")
        
        conn.close()

    # 三か月おきにリストア確認の催促メッセージ(消した)
    
    archive()

