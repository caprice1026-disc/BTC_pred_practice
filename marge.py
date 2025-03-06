import pandas as pd
from datetime import datetime

# ファイルパスの設定
funding_file = "funding_rates.csv"
long_short_file = "long_short_ratio.csv"
output_file = "combined.csv"

# --- funding_rates.csv の処理 ---
df_fund = pd.read_csv(funding_file)

# fundingRateTimestamp 列を UNIX ミリ秒から "yyyy/mm/dd/hh" 形式に変換し "time" 列として追加
df_fund["time"] = df_fund["fundingRateTimestamp"].apply(
    lambda x: datetime.fromtimestamp(x / 1000).strftime("%Y/%m/%d/%H")
)
# "time" 列を fundingRateTimestamp 列の前に移動する
cols = df_fund.columns.tolist()
time_index = cols.index("fundingRateTimestamp")
cols.insert(time_index, cols.pop(cols.index("time")))
df_fund = df_fund[cols]

# --- long_short_ratio.csv の処理 ---
df_long = pd.read_csv(long_short_file)

# timestamp 列を UNIX ミリ秒から "yyyy/mm/dd/hh" 形式に変換し "time" 列として追加
df_long["time"] = df_long["timestamp"].apply(
    lambda x: datetime.fromtimestamp(x / 1000).strftime("%Y/%m/%d/%H")
)
# "time" 列を timestamp 列の前に移動する
cols = df_long.columns.tolist()
time_index = cols.index("timestamp")
cols.insert(time_index, cols.pop(cols.index("time")))
df_long = df_long[cols]

# --- 両DataFrameの統合 ---
# それぞれのファイルで列は異なりますが、縦方向に結合するので union された列になる
df_combined = pd.concat([df_fund, df_long], ignore_index=True, sort=False)

# 結果を CSV に出力
df_combined.to_csv(output_file, index=False)

print(f"処理完了。結果は {output_file} に保存されました。")
