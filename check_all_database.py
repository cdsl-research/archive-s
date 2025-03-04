import mysql.connector
from tabulate import tabulate

# MySQLデータベースに接続するための情報
conn = mysql.connector.connect(
    host="192.168.100.35",
    user="root",
    password="password",
    port="32000",
    database="VM_archive_DB"
)

# カーソルを作成
cursor = conn.cursor()

# テーブルのリストを取得
cursor.execute("SHOW TABLES")
tables = cursor.fetchall()

# 各テーブルの内容を表示
for table in tables:
    table_name = table[0]
    print(f"\nContents of table: {table_name}")

    # テーブルのデータを取得
    cursor.execute(f"SELECT * FROM {table_name}")
    rows = cursor.fetchall()

    # データをタブレット形式で表示
    print(tabulate(rows, headers=[i[0] for i in cursor.description], tablefmt="pipe"))

# 接続を閉じる
cursor.close()
conn.close()