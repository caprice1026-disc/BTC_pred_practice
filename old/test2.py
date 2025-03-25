import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
from pybit.unified_trading import HTTP  # ロングショートレシオ取得用

# -------------------------------
# 日足ローソク足データ取得 (Daily Klines)
# -------------------------------
def fetch_daily_klines(symbol="BTCUSDT", category="linear", interval="D",
                         total_days=80, limit=1000,
                         url="https://api.bybit.com/v5/market/kline"):
    """
    指定期間(total_days)分の日足ローソク足データをページング対応で取得します。
    各リクエスト毎にログを出力します。
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
            "interval": interval,  # 日足なので "D"
            "start": current_start,
            "end": end_timestamp,
            "limit": limit
        }
        req_start_dt = datetime.fromtimestamp(current_start / 1000)
        print(f"[DAILY KLINE] リクエスト: {req_start_dt.strftime('%Y-%m-%d %H:%M:%S.%f')} からエンドタイムまで")
        response = requests.get(url, params=params)
        result = response.json()
        if result.get("retCode") != 0:
            print("APIエラー（DAILY KLINE）:", result.get("retMsg"))
            break
        data_list = result.get("result", {}).get("list", [])
        print(f"[DAILY KLINE] {req_start_dt.strftime('%Y-%m-%d %H:%M:%S.%f')}～: {len(data_list)} 件取得")
        if not data_list:
            break
        all_data.extend(data_list)
        last_time = int(data_list[-1][0])
        if last_time >= end_timestamp:
            break
        new_start = last_time + 1
        if new_start <= current_start:
            print(f"[DAILY KLINE] ページング更新できず (current_start={current_start}, new_start={new_start})。ループ終了します。")
            break
        current_start = new_start
        time.sleep(0.1)  # レート制限対策

    return all_data

# -------------------------------
# テクニカル指標計算（※計算内容は日足でも同様）
# -------------------------------
def calculate_indicators(df):
    """
    DataFrameに対してATR、ボリンジャーバンド、移動平均、RSI、EMAを計算し列として追加します。
    ※ 日足データの場合、各計算は日単位となります。
    """
    # 型変換、時刻整形・ソート
    df["time"] = pd.to_datetime(df["time"].astype(int), unit="ms")
    df.sort_values("time", inplace=True)
    df.reset_index(drop=True, inplace=True)

    # 数値型へ変換
    num_cols = ["open", "high", "low", "close", "volume", "turnover"]
    df[num_cols] = df[num_cols].apply(pd.to_numeric)

    # ATR (14日)
    df["prev_close"] = df["close"].shift(1)
    tr1 = df["high"] - df["low"]
    tr2 = (df["high"] - df["prev_close"]).abs()
    tr3 = (df["low"] - df["prev_close"]).abs()
    df["TR"] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr_period = 14
    df["ATR"] = df["TR"].rolling(window=atr_period, min_periods=atr_period).mean()

    # ボリンジャーバンド（20日、標準偏差倍率2）
    bb_period = 20
    df["MA20"] = df["close"].rolling(window=bb_period, min_periods=bb_period).mean()
    df["std20"] = df["close"].rolling(window=bb_period, min_periods=bb_period).std()
    df["BB_upper"] = df["MA20"] + 2 * df["std20"]
    df["BB_lower"] = df["MA20"] - 2 * df["std20"]

    # 移動平均線
    df["MA5"] = df["close"].rolling(window=5, min_periods=5).mean()
    df["MA10"] = df["close"].rolling(window=10, min_periods=10).mean()
    df["MA20_calc"] = df["MA20"]

    # RSI (14日)
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

    # EMA (20日)
    ema_period = 20
    df["EMA"] = df["close"].ewm(span=ema_period, adjust=False).mean()

    # 不要な中間列を削除
    df.drop(columns=["prev_close", "std20", "TR"], inplace=True)
    return df

# -------------------------------
# ロングショートレシオ取得（1日足）
# -------------------------------
def fetch_long_short_ratio(start_ts, end_ts, period="1d", symbol="BTCUSDT",
                           category="linear", limit=500):
    """
    指定期間内のロングショートレシオデータを、1日単位のウィンドウでページング対応で取得します。
    APIリクエスト毎にログを出力します。
    """
    session = HTTP()  # pybitのセッション作成
    records_all = []
    # 1日単位のウィンドウ（24時間）
    window_ms = 24 * 60 * 60 * 1000
    current_start = start_ts

    while current_start < end_ts:
        current_end = min(current_start + window_ms, end_ts)
        cursor = None
        while True:
            params = {
                "category": category,
                "symbol": symbol,
                "period": period,  # 日足なので "1d"
                "startTime": current_start,
                "endTime": current_end,
                "limit": limit
            }
            if cursor:
                params["cursor"] = cursor

            req_start_dt = datetime.fromtimestamp(current_start / 1000)
            req_end_dt = datetime.fromtimestamp(current_end / 1000)
            print(f"[LSR] リクエスト: {req_start_dt.strftime('%Y-%m-%d %H:%M:%S.%f')} ～ {req_end_dt.strftime('%Y-%m-%d %H:%M:%S.%f')}")
            try:
                response = session.get_long_short_ratio(**params)
            except Exception as e:
                print(f"[LSR] API呼び出し例外: {e}")
                break

            if response.get("retCode") != 0:
                print("[LSR] APIエラー:", response.get("retMsg"))
                break

            result = response.get("result", {})
            records = result.get("list", [])
            print(f"[LSR] {req_start_dt.strftime('%Y-%m-%d %H:%M:%S.%f')} ～ {req_end_dt.strftime('%Y-%m-%d %H:%M:%S.%f')} : {len(records)} 件取得")
            records_all.extend(records)

            cursor = result.get("nextPageCursor")
            if not cursor:
                break
            time.sleep(1.1)  # レート制限対策

        current_start = current_end
        time.sleep(1.1)

    return records_all

# -------------------------------
# メイン処理
# -------------------------------
def main():
    # ----- パラメータ設定 -----
    extra_days = 20
    output_days = 60
    total_days = output_days + extra_days

    # ----- Step1: 80日分の日足生データ取得 & CSV保存 -----
    raw_data = fetch_daily_klines(total_days=total_days)
    if not raw_data:
        print("日足ローソク足データが取得できませんでした。")
        return

    # DataFrame作成（各ローソク足は [startTime, open, high, low, close, volume, turnover] の順）
    df_raw = pd.DataFrame(raw_data, columns=["time", "open", "high", "low", "close", "volume", "turnover"])
    df_raw.to_csv("raw_data_daily.csv", index=False)
    print("80日分の日足生データを 'raw_data_daily.csv' として保存しました。")

    # ----- Step2: テクニカル指標の計算 -----
    df_with_indicators = calculate_indicators(df_raw)

    # ----- Step3: 直近60日分のデータ抽出（80日分のうち、計算用に取得した過去データを除外） -----
    final_start_time = datetime.utcnow() - timedelta(days=output_days)
    df_final = df_with_indicators[df_with_indicators["time"] >= final_start_time].copy()

    # ----- Step4: ロングショートレシオの取得（1日足） -----
    final_start_ts = int(final_start_time.timestamp() * 1000)
    final_end_ts = int(datetime.utcnow().timestamp() * 1000)
    lsr_records = fetch_long_short_ratio(start_ts=final_start_ts, end_ts=final_end_ts,
                                           period="1d", symbol="BTCUSDT", category="linear", limit=500)
    if lsr_records:
        df_lsr = pd.DataFrame(lsr_records)
        df_lsr["time"] = pd.to_datetime(df_lsr["timestamp"].astype(int), unit="ms")
        df_lsr = df_lsr[["time", "buyRatio", "sellRatio"]]
    else:
        print("ロングショートレシオデータが取得できませんでした。")
        df_lsr = pd.DataFrame(columns=["time", "buyRatio", "sellRatio"])

    # ----- Step5: テクニカル指標とロングショートレシオのマージ -----
    df_merged = pd.merge(df_final, df_lsr, on="time", how="left")
    df_merged.to_csv("data_with_indicators_daily.csv", index=False)
    print("直近60日分の日足テクニカル指標＋ロングショートレシオ付きデータを 'data_with_indicators_daily.csv' として保存しました。")

if __name__ == "__main__":
    main()
