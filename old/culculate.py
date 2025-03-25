import pandas as pd
import numpy as np

# @自分　未実行なので実行して確認してね
# CSVファイルの読み込み（ファイル名は適宜変更してください）
df = pd.read_csv('data.csv')

# 時刻の文字列をdatetime型に変換（フォーマットは"YYYY.MM.DD HH:MM"）
df['time'] = pd.to_datetime(df['time'], format='%Y.%m.%d %H:%M')

# 時刻順にソート（必要に応じて）
df.sort_values('time', inplace=True)
df.reset_index(drop=True, inplace=True)

# ----- ATR（Average True Range）の計算 -----
# True Range (TR)の計算
# TR = max( 高値-安値, |高値 - 前期終値|, |安値 - 前期終値| )
df['prev_close'] = df['close'].shift(1)
df['tr1'] = df['high'] - df['low']
df['tr2'] = (df['high'] - df['prev_close']).abs()
df['tr3'] = (df['low'] - df['prev_close']).abs()
df['TR'] = df[['tr1', 'tr2', 'tr3']].max(axis=1)

# ATRは一般に14期間の移動平均を使用
atr_period = 14
df['ATR'] = df['TR'].rolling(window=atr_period, min_periods=atr_period).mean()
# ※ 初期14期間分は十分な過去データがないためNaNとなる

# ----- ボリンジャーバンドの計算 -----
# ここでは期間20、標準偏差の倍率2を採用
bb_period = 20
df['MA20'] = df['close'].rolling(window=bb_period, min_periods=bb_period).mean()
df['std20'] = df['close'].rolling(window=bb_period, min_periods=bb_period).std()
df['BB_upper'] = df['MA20'] + 2 * df['std20']
df['BB_lower'] = df['MA20'] - 2 * df['std20']
# ※ ボリンジャーバンドは20期間分のデータが必要なため、最初の19行はNaN

# ----- 移動平均線（MA）の計算 -----
# MA5
df['MA5'] = df['close'].rolling(window=5, min_periods=5).mean()
# MA10
df['MA10'] = df['close'].rolling(window=10, min_periods=10).mean()
# MA20（再計算。上記BB用のMA20とは別にする場合も同様）
df['MA20'] = df['close'].rolling(window=20, min_periods=20).mean()
# ※ 各移動平均は指定期間分のデータが必要なため、MA5では最初の4行、MA10では最初の9行、MA20では最初の19行はNaN

# ----- RSI（Relative Strength Index）の計算 -----
# ここでは一般に用いられる14期間のRSIを計算
rsi_period = 14
delta = df['close'].diff()

# 上昇幅と下降幅を計算
gain = delta.clip(lower=0)
loss = -delta.clip(upper=0)

# 平均上昇幅・平均下降幅を単純移動平均で算出
avg_gain = gain.rolling(window=rsi_period, min_periods=rsi_period).mean()
avg_loss = loss.rolling(window=rsi_period, min_periods=rsi_period).mean()

# RSおよびRSIの計算
rs = avg_gain / avg_loss
df['RSI'] = 100 - (100 / (1 + rs))
# ※ RSIは14期間分のデータが必要なため、最初の14行はNaN

# ----- EMA（Exponential Moving Average）の計算 -----
# ここでは期間20のEMAを例とする
ema_period = 20
df['EMA'] = df['close'].ewm(span=ema_period, adjust=False).mean()
# ※ EMAは初期値を1行目の値から再帰的に計算するため、全行に値が出るが、初期部分は十分な過去データに基づかないから注意しろ

# ----- 不要な中間計算用の列を削除 -----
df.drop(columns=['prev_close', 'tr1', 'tr2', 'tr3', 'std20', 'TR'], inplace=True)

# ----- 結果をCSVとして出力 -----
df.to_csv('data_with_indicators.csv', index=False)
