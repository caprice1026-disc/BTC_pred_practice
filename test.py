import pandas as pd

# CSVファイルを読み込み。time列をdatetime型に変換
df = pd.read_csv('merged_dataset.csv', parse_dates=['time'])

# 次の行の終値を取得
df['next_close'] = df['close'].shift(-1)

# 終値のパーセンテージ変化率を計算
# (次の終値 - 現在の終値) / 現在の終値 * 100
df['return_pct'] = (df['next_close'] - df['close']) / df['close'] * 100

# 不要になったnext_close列は削除
df.drop(columns=['next_close'], inplace=True)

# 終値変化率の結果を含むCSVファイルとして出力
output_file = 'merged_dataset_with_return.csv'
df.to_csv(output_file, index=False)
print(f"新しいCSVファイル '{output_file}' に終値のパーセンテージ変化が書き込まれました。")
