# CSVファイルを結合してひとつのCSVファイルにするスクリプト
# テンプレートとして使いまわし
import glob
import pandas as pd

def merge_and_sort_csv(output_file='merged.csv'):
    # 対象となるCSVファイルのパターンを指定（例：BTCUSDT_60_*.csv）
    csv_files = glob.glob("BTCUSDT_60_*.csv")
    
    # 各CSVファイルを読み込み、データフレームのリストに格納
    dfs = []
    for file in csv_files:
        # CSVファイルの読み込み（ヘッダーがない場合はheader=Noneで読み込む）
        df = pd.read_csv(file, header=None)
        dfs.append(df)
    
    # 全てのデータフレームを結合
    merged_df = pd.concat(dfs, ignore_index=True)
    
    # 1列目が日時情報と仮定し、日時型に変換（例：2024.11.01 00:00）
    merged_df[0] = pd.to_datetime(merged_df[0], format='%Y.%m.%d %H:%M')
    
    # 日時でソート
    merged_df = merged_df.sort_values(by=0)
    
    # 日時を元の文字列形式に戻す
    merged_df[0] = merged_df[0].dt.strftime('%Y.%m.%d %H:%M')
    
    # 結合後のデータを新しいCSVファイルに書き出す（元のファイルはそのまま）
    merged_df.to_csv(output_file, header=False, index=False)
    print(f"結合されたCSVファイルを {output_file} として保存しました。")

if __name__ == '__main__':
    merge_and_sort_csv()
