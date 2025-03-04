#!/usr/bin/env python3
import hashlib
import os

def calculate_md5(directory):
    files = []
    for root, _, filenames in os.walk(directory):  # os.walkを使用して再帰的にディレクトリを探索
        for f in filenames:
            file_path = os.path.join(root, f)
            if os.path.isfile(file_path):
                files.append(file_path)
    
    files.sort()  # ファイルのリストをアルファベット順にソート
    md5_hash = hashlib.md5()
    for file_path in files:
        with open(file_path, "rb") as f:
            while chunk := f.read(16777216):
                md5_hash.update(chunk)
                print(f"Hashing file: {file_path} ({f.tell()} bytes processed)")
    
    return md5_hash.hexdigest()

def hash_directories_in_path(base_path, output_file):
    with open(output_file, 'w') as out_file:
        entries = os.listdir(base_path)

        
        for entry in entries[1:]:  # 二番目のエントリから始める
            full_path = os.path.join(base_path, entry)
            if os.path.isdir(full_path):
                print(f"Calculating MD5 for directory: {full_path}")
                md5 = calculate_md5(full_path)
                result = f"MD5 for {full_path}: {md5}\n"
                print(result)
                out_file.write(result)

if __name__ == "__main__":
    base_path = "/mnt/iscsi/target-mini2/VM-archive/"
    output_file = "hash_results.txt"
    hash_directories_in_path(base_path, output_file)
