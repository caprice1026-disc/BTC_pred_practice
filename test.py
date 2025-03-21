import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time

# -------------------------------
# データ取得用の関数
# -------------------------------
def fetch_klines(symbol="BTCUSDT", category="linear", interval="60",
                 total_days=80, limit=1000,
                 url="https://api.bybit.com/v5/market/kline"):
    """
    total_days は出力の60日に加え、指標計算用に追加する日数（例：80なら60+20日分）を指定します。
    """
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days=total_days)
    end_timestamp = int(end_time.timestamp() * 1000)
    start_timestamp = int(start_time.timestamp() * 1000)

    all_data = []
    current_start = start_timestamp

    while True:
        params = {
            "category": category,
            "symbol": symbol,
            "interval": interval,
            "start": current_start,
            "end": end_timestamp,
            "limit": limit
        }
        response = requests.get(url, params=params)
        result = response.json()
        if result.get("retCode") != 0:
            print("Error:", result.get("retMsg"))
            break
        data_list = result.get("result", {}).get("list", [])
        if not data_list:
            break
        all_data.extend(data_list)
        last_time = int(data_list[-1][0])
        if last_time >= end_timestamp:
            break
        current_start = last_time + 1  # 重複を避けるため+1ms
        time.sleep(0.1)  # レート制限対策

    return all_data

# -------------------------------
# テクニカル指標計算用の関数
# -------------------------------
def calculate_indicators(df):
    # time列の型変換とソートをまとめて実施
    df = (df.assign(time=pd.to_datetime(df["time"].astype(int), unit='ms'))
            .sort_values("time")
            .reset_index(drop=True))
    
    # 数値型に変換する列
    num_cols = ["open", "high", "low", "close", "volume", "turnover"]
    df[num_cols] = df[num_cols].apply(pd.to_numeric)

    # -------------------------------
    # ATR (Average True Range) の計算（14期間）
    # -------------------------------
    df["prev_close"] = df["close"].shift(1)
    tr1 = df["high"] - df["low"]
    tr2 = (df["high"] - df["prev_close"]).abs()
    tr3 = (df["low"] - df["prev_close"]).abs()
    df["TR"] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr_period = 14
    df["ATR"] = df["TR"].rolling(window=atr_period, min_periods=atr_period).mean()

    # -------------------------------
    # ボリンジャーバンド（期間20、標準偏差倍率2）
    # -------------------------------
    bb_period = 20
    df["MA20"] = df["close"].rolling(window=bb_period, min_periods=bb_period).mean()
    df["std20"] = df["close"].rolling(window=bb_period, min_periods=bb_period).std()
    df["BB_upper"] = df["MA20"] + 2 * df["std20"]
    df["BB_lower"] = df["MA20"] - 2 * df["std20"]

    # -------------------------------
    # 移動平均線 (MA)
    # -------------------------------
    df["MA5"] = df["close"].rolling(window=5, min_periods=5).mean()
    df["MA10"] = df["close"].rolling(window=10, min_periods=10).mean()
    # Bollinger用のMA20と同じ計算なのでコピーして利用（計算回数を削減）
    df["MA20_calc"] = df["MA20"]

    # -------------------------------
    # RSI (Relative Strength Index) の計算（14期間）
    # -------------------------------
    rsi_period = 14
    delta = df["close"].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.rolling(window=rsi_period, min_periods=rsi_period).mean()
    avg_loss = loss.rolling(window=rsi_period, min_periods=rsi_period).mean()
    rs = avg_gain / avg_loss
    df["RSI"] = 100 - (100 / (1 + rs))
    df["avg_gain"] = avg_gain
    df["avg_loss"] = avg_loss

    # -------------------------------
    # EMA (Exponential Moving Average) の計算（期間20）
    # -------------------------------
    ema_period = 20
    df["EMA"] = df["close"].ewm(span=ema_period, adjust=False).mean()

    # 中間計算用の列は削除
    df.drop(columns=["prev_close", "std20", "TR"], inplace=True)
    
    return df

# -------------------------------
# メイン処理
# -------------------------------
def main():
    # 最終出力は直近60日分。計算用に追加20日分取得（合計80日）
    extra_days = 20
    output_days = 60
    total_days = output_days + extra_days

    raw_data = fetch_klines(total_days=total_days)
    if not raw_data:
        print("データが取得できませんでした。")
        return

    # APIの各ローソク足は [startTime, open, high, low, close, volume, turnover]
    df = pd.DataFrame(raw_data, columns=["time", "open", "high", "low", "close", "volume", "turnover"])
    df = calculate_indicators(df)

    # 出力用に直近60日分に絞り込み
    final_start_time = datetime.utcnow() - timedelta(days=output_days)
    df_final = df[df["time"] >= final_start_time].copy()

    # CSVファイルとして保存
    df_final.to_csv('data_with_indicators.csv', index=False)
    print("CSVファイル 'data_with_indicators.csv' が生成されました。")

if __name__ == "__main__":
    main()
