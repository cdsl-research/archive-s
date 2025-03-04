import mysql.connector
from tabulate import tabulate

def display_table_contents(table_name):
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
    try:
        # テーブルのデータを取得
        cursor.execute(f"SELECT * FROM {table_name}")
        rows = cursor.fetchall()
        # データをタブレット形式で表示
        print(f"\nContents of table: {table_name}")
        print(tabulate(rows, headers=[i[0] for i in cursor.description], tablefmt="pipe"))
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        # 接続を閉じる
        cursor.close()
        conn.close()
def get_table_names():
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

    try:
        # テーブルのリストを取得
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        return [table[0] for table in tables]
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return []
    finally:
        # 接続を閉じる
        cursor.close()
        conn.close()

# テーブル名のリストを取得
table_names = get_table_names()

if table_names:
    print("Available tables:")
    for idx, table_name in enumerate(table_names, start=1):
        print(f"{idx}. {table_name}")

    # ユーザーにテーブル名を選択させる
    while True:
        try:
            choice = int(input("Select a table number to display: "))
            if 1 <= choice <= len(table_names):
                selected_table = table_names[choice - 1]
                display_table_contents(selected_table)
                break
            else:
                print("Invalid number. Please select a number from the list.")
        except ValueError:
            print("Invalid input. Please enter a number.")
else:
    print("No tables found in the database.")