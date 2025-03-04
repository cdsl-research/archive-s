#!/usr/bin/env python3
import subprocess
import random
import os

def convert_to_bytes(size_info):
    if size_info.endswith('K'):
        return float(size_info[:-1]) * 1024
    elif size_info.endswith('M'):
        return float(size_info[:-1]) * (1024 ** 2)
    elif size_info.endswith('G'):
        return float(size_info[:-1]) * (1024 ** 3)
    elif size_info.endswith('T'):
        return float(size_info[:-1]) * (1024 ** 4)
    else:
        return float(size_info)

#test = "28K"
#chtest = int(convert_to_bytes(test))
#print(chtest)

def output_dict_result():
    command1 = ['df', '-h']
    command2 = ['grep', '/dev/sd']

    process1 = subprocess.Popen(command1, stdout=subprocess.PIPE)
    process2 = subprocess.Popen(command2, stdin=process1.stdout, encoding="utf-8", stdout=subprocess.PIPE)

    # コマンド2の実行結果を取得
    output, error = process2.communicate()

    # 空の辞書を作成
    result_dict = {}
    storage_capacity = {}

    # 行ごとに処理
    for line in output.splitlines():
        #print(line)
        # スペースで分割
        parts = line.split()

        # パーティションと使用量の情報を抽出
        partition = parts[5]
        usage_info = parts[2]
        available = parts[3]
        available_bytes = float(available[:-1]) * (1024**4)
        #print(partition)
        #print(usage_info)
        #print(available)
        #print(available_bytes)

        # 辞書に追加
        if usage_info.endswith('K'):
            usage_bytes = float(usage_info[:-1]) * 1024
        elif usage_info.endswith('M'):
            usage_bytes = float(usage_info[:-1]) * (1024**2) 
        elif usage_info.endswith('G'):
            usage_bytes = float(usage_info[:-1]) * (1024**3) 
        elif usage_info.endswith('T'):
            usage_bytes = float(usage_info[:-1]) * (1024**4) 
        else:
            usage_bytes = float(usage_info)

        result_dict[partition] = int(round(usage_bytes))
        storage_capacity[partition] = int(available_bytes - int(round(usage_bytes)))


    # 結果を表示
    #print(result_dict)
    #print(storage_capacity)
    return result_dict ,storage_capacity
#容量の比較
def output_result_comparison():
        #dataには対象ストレージの使用量が入っている
    data, storage = output_dict_result()
    
    min_value = min(int(v) for v in data.values())
    min_keys = [k for k, v in data.items() if int(v) == min_value]

    random_min_key = random.choice(min_keys)
    print(random_min_key)
    return random_min_key


#送る対象を見つける関数
def output_send_target():
    dir_path = "/mnt/iscsi/target-mini2/VM-archive"
    output_send_target = os.listdir(dir_path)
    absolute_paths = [os.path.join(dir_path, item) for item in output_send_target]
    #print(absolute_paths)
    return absolute_paths


#storageに来るVMのパスを生成
def generate_destination_storage(conn, vmname):
    min_storage = output_result_comparison()
    storage_path = os.path.join(min_storage, vmname)
    return storage_path

#output_dict_result()
#output_send_target()
#output_result_comparison()
