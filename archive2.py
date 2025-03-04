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

def main():
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

    # archive後期限切れVM削除
    delete_expired_vms(conn)

    if not rows:
        logging.info("No data found in the database.")
        send_slack_notification("No data found in the database.")
        conn.close()
    else:
        cur = conn.cursor()
        print(cur)

    # 三か月おきにリストア確認の催促メッセージ
    current_month = datetime.now().month
    if current_month % 3 == 0:  # 3の倍数の月に送信
        send_slack_notification("Reminder: Please verify if you can restore archived VMs this month.")

    archive(rows, conn, cur)


# Slackにメッセージを送信する関数
def send_slack_notification(message):
    try:
        payload = {'text': message}
        response = requests.post(SLACK_WEBHOOK_URL, json=payload)
        if response.status_code != 200:
            logging.error(f"Failed to send message to Slack. Status code: {response.status_code}")
    except Exception as e:
        logging.error(f"Error while sending message to Slack: {e}")


# 保持期間が過ぎたVMの削除と通知
def delete_expired_vms(conn):
    deleted_vms = del_data_after_three_month(conn)
    if deleted_vms:
        for vm in deleted_vms:
            send_slack_notification(f"VM {vm} has been deleted after exceeding the retention period.")
            logging.info("VM %s has been deleted after exceeding the retention period.", vm)


def archive(rows, conn, cur):
    for i, row in enumerate(rows):
        date_time, vmname, esxi, hash_value, user, VM_size = row
        data, storage_capacity = output_dict_result()
        conv_size = convert_to_bytes(VM_size)
        retry_count = 0
        max_retries = 2
        archive_dir = f"/mnt/iscsi/target-mini2/VM-archive/{vmname}"
        storage_dir = output_result_comparison()
        archive_destination = os.path.join(storage_dir, vmname)

        # すでに履歴にあるVMはスキップする
        cur.execute("SELECT COUNT(*) FROM HISTORY_OF_HDD WHERE VM_path = %s", (archive_destination,))
        if cur.fetchone()[0] > 0:
            logging.info("VM %s is already archived. Skipping.", vmname)
            continue

        logging.info("Archiving process started for %s. Rsync source: %s size: %s Rsync destination: %s",
                     vmname, archive_dir, VM_size, archive_destination)

        while retry_count < max_retries:
            try:
                if conv_size < storage_capacity[storage_dir]:
                    rsync_command = ['sudo', 'rsync', '-av', archive_dir, storage_dir]
                    subprocess.run(rsync_command, check=True)

                    # アーカイブ成功時のみ履歴を記録
                    current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    hash_storage = calculate_md5(archive_destination)

                    if hash_storage == hash_value:
                        mysql_insert_data(conn, current_datetime, archive_destination, esxi, hash_storage, user, VM_size)
                        logging.info("Rsync completed successfully for VM: %s", vmname)

                        # 履歴に登録
                        cur.execute(
                            "INSERT INTO HISTORY_OF_HDD (date_time, VM_path, ESXi, user, Processing) VALUES (%s, %s, %s, %s, %s)",
                            (current_datetime, archive_destination, esxi, user, 'Archiving process successfully')
                        )
                        conn.commit()

                        del_check_data(conn, vmname, esxi, user, hash_value)

                        # Slack通知: アーカイブ成功
                        send_slack_notification(f"Archiving process completed successfully for VM: {vmname} ({VM_size})")
                        break  # 成功したらループを抜ける
                    else:
                        logging.warning(f"Hash values do not match for {vmname}. Retrying rsync... (Attempt {retry_count+1}/{max_retries})")
                        retry_count += 1
                else:
                    logging.warning("Not enough storage space for VM: %s", vmname)
                    old_vm = oldest_VM_path(conn)
                    if os.path.exists(old_vm):
                        shutil.rmtree(old_vm)
                        logging.info("Old VM %s deleted successfully to free up space.", old_vm)
                        send_slack_notification(f"Old VM {old_vm} deleted successfully to free up space.")
                    else:
                        logging.warning("Directory %s does not exist.", old_vm)
                        break  # ストレージ不足時はループを抜ける

            except subprocess.CalledProcessError as e:
                logging.error("Rsync failed with return code %s for VM: %s", e.returncode, vmname)
                retry_count += 1
                send_slack_notification(f"Archiving process failed for {vmname}. Retrying... (Attempt {retry_count}/{max_retries})")

        if retry_count >= max_retries:
            logging.error("Archiving process failed after maximum retries for VM: %s", vmname)
            send_slack_notification(f"Archiving process failed after maximum retries for VM: {vmname}")

    conn.close()



if __name__ == "__main__":
    main()

