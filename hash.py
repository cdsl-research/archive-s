#!/usr/bin/env python3
import hashlib  # ハッシュ関数を提供するhashlibモジュールをインポート
import os       # オペレーティングシステムとのやり取りを行うosモジュールをインポート

#　ここでは計算するだけ，計算するディレクトリはarchive.pyで選出してそれを受け取って比較する．
def calculate_md5(directory):
    files = []  # ファイルパスを格納する空のリストを初期化
    for f in os.listdir(directory):
        file_path = os.path.join(directory, f)  # ファイル/ディレクトリのパスを作成
        #print(file_path)
        if os.path.isfile(file_path):  # アイテムがファイルであるかを確認
            files.append(file_path)     # ファイルパスをファイルのリストに追加
    #print(file_path)
    files.sort()  # ファイルのリストをアルファベット順にソート
    md5_hash = hashlib.md5()
    # ファイルリスト内の各ファイルについて繰り返す
    for file_path in files:
        # ファイルをバイナリモードで開く
        with open(file_path, "rb") as f:
            # ファイルの内容を読み取り、MD5ハッシュを更新
            while chunk := f.read(16777216):
                md5_hash.update(chunk)
                print(f"Hashing file: {file_path} ({f.tell()} bytes processed)")
    #print(md5_hash.hexdigest())
    # MD5ハッシュの16進数表現を返す
    return md5_hash.hexdigest()