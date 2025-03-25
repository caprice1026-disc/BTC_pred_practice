import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
from pybit.unified_trading import HTTP  # 資金調達率、オープンインタレスト取得用

# OI追加部分に関しては怪しいので明日レビューし直すこと
# requestsライブラリとPythonのver3.10以上のバージョンは互換性がなかったような気がするので要確認
# 具体的にはUbuntuで実行時に証明書エラーが発生していた気がする

# -------------------------------
# 1. 1時間足ローソク足データ取得 (Klines)
# -------------------------------
def fetch_klines(symbol="BTCUSDT", category="linear", interval="60",
                 total_days=60, limit=1000,
                 url="https://api.bybit.com/v5/market/kline"):
    '''指定期間(total_days)分の1時間足ローソク足データをページング対応で取得する関数'''
    # utcnowが非推奨になった理由と代替メソッドを調べておく
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
        req_start_dt = datetime.fromtimestamp(current_start / 1000)
        print(f"[HOURLY KLINE] リクエスト開始: {req_start_dt.strftime('%Y-%m-%d %H:%M:%S.%f')}")
        response = requests.get(url, params=params)
        result = response.json()
        if result.get("retCode") != 0:
            print("APIエラー（HOURLY KLINE）:", result.get("retMsg"))
            break
        data_list = result.get("result", {}).get("list", [])
        print(f"[HOURLY KLINE] {req_start_dt.strftime('%Y-%m-%d %H:%M:%S.%f')}～: {len(data_list)} 件取得")
        if not data_list:
            break
        all_data.extend(data_list)
        last_time = int(data_list[-1][0])
        if last_time >= end_timestamp:
            break
        new_start = last_time + 1
        if new_start <= current_start:
            print(f"[HOURLY KLINE] ページング更新できず (current_start={current_start}, new_start={new_start})。")
            break
        current_start = new_start
        time.sleep(1)
    return all_data

# -------------------------------
# 2. 日足ローソク足データ取得 (Daily Klines)
# -------------------------------
def fetch_daily_klines(symbol="BTCUSDT", category="linear", interval="D",
                         total_days=60, limit=1000,
                         url="https://api.bybit.com/v5/market/kline"):
    '''指定期間(total_days)分の日足ローソク足データをページング対応で取得する関数'''
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
        print(f"[DAILY KLINE] リクエスト開始: {req_start_dt.strftime('%Y-%m-%d %H:%M:%S.%f')}")
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
            print(f"[DAILY KLINE] ページング更新できず (current_start={current_start}, new_start={new_start})。")
            break
        current_start = new_start
        time.sleep(1)
    return all_data

# -------------------------------
# 3. テクニカル指標計算
# -------------------------------
def calculate_indicators(df):
    '''DataFrameに対して、ATR、ボリンジャーバンド、移動平均、RSI、EMAなどのテクニカル指標を計算し追加する関数'''
    # 型変換、時刻整形・ソート
    df["time"] = pd.to_datetime(df["time"].astype(int), unit="ms")
    df.sort_values("time", inplace=True)
    df.reset_index(drop=True, inplace=True)
    
    # 数値型への変換
    num_cols = ["open", "high", "low", "close", "volume", "turnover"]
    df[num_cols] = df[num_cols].apply(pd.to_numeric)
    
    # ATR (14期間)
    df["prev_close"] = df["close"].shift(1)
    tr1 = df["high"] - df["low"]
    tr2 = (df["high"] - df["prev_close"]).abs()
    tr3 = (df["low"] - df["prev_close"]).abs()
    df["TR"] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr_period = 14
    df["ATR"] = df["TR"].rolling(window=atr_period, min_periods=atr_period).mean()
    
    # ボリンジャーバンド（20期間、標準偏差倍率2）
    bb_period = 20
    df["MA20"] = df["close"].rolling(window=bb_period, min_periods=bb_period).mean()
    df["std20"] = df["close"].rolling(window=bb_period, min_periods=bb_period).std()
    df["BB_upper"] = df["MA20"] + 2 * df["std20"]
    df["BB_lower"] = df["MA20"] - 2 * df["std20"]
    
    # 移動平均線
    df["MA5"] = df["close"].rolling(window=5, min_periods=5).mean()
    df["MA10"] = df["close"].rolling(window=10, min_periods=10).mean()
    df["MA20_calc"] = df["MA20"]
    
    # RSI (14期間)
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
    
    # EMA (20期間)
    ema_period = 20
    df["EMA"] = df["close"].ewm(span=ema_period, adjust=False).mean()
    
    # 不要な中間列を削除
    df.drop(columns=["prev_close", "std20", "TR"], inplace=True)
    return df

# -------------------------------
# 4. 資金調達率データ取得＆線形補間（8時間ごと）
# -------------------------------
def fetch_funding_rate_history_custom(symbol="BTCUSDT", category="linear",
                                      period="8h", total_days=60, limit=200):
    '''指定期間(total_days)分の資金調達率データを、8時間ごとのウィンドウでページング対応で取得する関数。
    取得後、1時間足に合わせるための補間は後続の処理で行う前提。'''
    session = HTTP()  # pybitのセッション作成
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days=total_days)
    end_ts = int(end_time.timestamp() * 1000)
    start_ts = int(start_time.timestamp() * 1000)
    
    records_all = []
    window_ms = 8 * 60 * 60 * 1000  # 8時間分のミリ秒
    current_start = start_ts

    while current_start < end_ts:
        current_end = min(current_start + window_ms, end_ts)
        cursor = None
        while True:
            params = {
                "category": category,
                "symbol": symbol,
                "period": period,
                "startTime": current_start,
                "endTime": current_end,
                "limit": limit
            }
            if cursor:
                params["cursor"] = cursor
            req_start_dt = datetime.fromtimestamp(current_start / 1000)
            req_end_dt = datetime.fromtimestamp(current_end / 1000)
            print(f"[FUNDING] リクエスト: {req_start_dt.strftime('%Y-%m-%d %H:%M:%S')} ～ {req_end_dt.strftime('%Y-%m-%d %H:%M:%S')}")
            try:
                response = session.get_funding_rate_history(**params)
            except Exception as e:
                print(f"[FUNDING] API呼び出し例外: {e}")
                break
            if response.get("retCode") != 0:
                print("[FUNDING] APIエラー:", response.get("retMsg"))
                break
            result = response.get("result", {})
            records = result.get("list", [])
            print(f"[FUNDING] {req_start_dt.strftime('%Y-%m-%d %H:%M:%S')} ～ {req_end_dt.strftime('%Y-%m-%d %H:%M:%S')} : {len(records)} 件取得")
            records_all.extend(records)
            cursor = result.get("nextPageCursor")
            if not cursor:
                break
            time.sleep(1)
        current_start = current_end
        time.sleep(1)
    return records_all

# -------------------------------
# 5. オープンインタレストデータ取得（1時間足）
# -------------------------------
def fetch_open_interest_data(symbol="BTCUSDT", category="linear", interval="1h",
                             total_days=60, limit=200):
    '''指定期間(total_days)分の1時間足のオープンインタレストデータをページング対応で取得する関数'''
    session = HTTP(testnet=False)
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days=total_days)
    end_ts = int(end_time.timestamp() * 1000)
    start_ts = int(start_time.timestamp() * 1000)
    records_all = []
    window_ms = 60 * 60 * 1000  # 1時間分のミリ秒
    current_start = start_ts

    while current_start < end_ts:
        current_end = min(current_start + window_ms, end_ts)
        cursor = None
        while True:
            params = {
                "category": category,
                "symbol": symbol,
                "intervalTime": interval,
                "startTime": current_start,
                "endTime": current_end,
                "limit": limit
            }
            if cursor:
                params["cursor"] = cursor
            req_start_dt = datetime.fromtimestamp(current_start / 1000)
            req_end_dt = datetime.fromtimestamp(current_end / 1000)
            print(f"[OPEN INTEREST] リクエスト: {req_start_dt.strftime('%Y-%m-%d %H:%M:%S')} ～ {req_end_dt.strftime('%Y-%m-%d %H:%M:%S')}")
            try:
                response = session.get_open_interest(**params)
            except Exception as e:
                print(f"[OPEN INTEREST] API呼び出し例外: {e}")
                break
            if response.get("retCode") != 0:
                print("[OPEN INTEREST] APIエラー:", response.get("retMsg"))
                break
            result = response.get("result", {})
            records = result.get("list", [])
            print(f"[OPEN INTEREST] {req_start_dt.strftime('%Y-%m-%d %H:%M:%S')} ～ {req_end_dt.strftime('%Y-%m-%d %H:%M:%S')} : {len(records)} 件取得")
            records_all.extend(records)
            cursor = result.get("nextPageCursor")
            if not cursor:
                break
            time.sleep(1)
        current_start = current_end
        time.sleep(1)
    return records_all

# -------------------------------
# 6. メイン処理：データ統合＆CSV出力
# -------------------------------
def main():
    '''
    1時間足と日足、及び8時間ごとの資金調達率、さらに1時間足のオープンインタレストデータを取得し、
    テクニカル指標計算およびmerge_asofや線形補間で統合し、1時間単位の最終データセットとしてCSVに出力する。
    '''
    total_days = 60  # 60日分のデータ
    symbol = "BTCUSDT"
    
    # Step1: 1時間足データの取得とテクニカル指標計算
    print("1時間足データ取得中...")
    raw_hourly = fetch_klines(symbol=symbol, total_days=total_days)
    if not raw_hourly:
        print("1時間足データが取得できませんでした。")
        return
    df_hourly = pd.DataFrame(raw_hourly, columns=["time", "open", "high", "low", "close", "volume", "turnover"])
    df_hourly = calculate_indicators(df_hourly)
    df_hourly.drop_duplicates(subset=["time"], inplace=True)
    print("1時間足データ取得完了。")
    
    # Step2: 日足データの取得とテクニカル指標計算
    print("日足データ取得中...")
    raw_daily = fetch_daily_klines(symbol=symbol, total_days=total_days)
    if not raw_daily:
        print("日足データが取得できませんでした。")
        return
    df_daily = pd.DataFrame(raw_daily, columns=["time", "open", "high", "low", "close", "volume", "turnover"])
    df_daily = calculate_indicators(df_daily)
    df_daily.drop_duplicates(subset=["time"], inplace=True)
    print("日足データ取得完了。")
    
    # Step3: 1時間足データに日足データをマージ（merge_asof）
    df_hourly["date"] = pd.to_datetime(df_hourly["time"]).dt.floor("D")
    df_daily["date"] = pd.to_datetime(df_daily["time"]).dt.floor("D")
    df_hourly = df_hourly.sort_values("time")
    df_daily = df_daily.sort_values("date")
    print("merge_asofで1時間足と日足データをマージ中...")
    df_merged = pd.merge_asof(df_hourly, df_daily[["date", "MA20", "ATR", "RSI", "EMA"]],
                              on="date", direction="backward", suffixes=("", "_daily"))
    df_merged.drop(columns=["date"], inplace=True)
    print("日足データの拡張完了。")
    
    # Step4: 資金調達率データの取得 & 補間（8時間ごと→1時間足へ）
    print("資金調達率データ取得中...")
    end_ts = int(datetime.utcnow().timestamp() * 1000)
    start_ts = int((datetime.utcnow() - timedelta(days=total_days)).timestamp() * 1000)
    funding_records = fetch_funding_rate_history_custom(symbol=symbol, total_days=total_days)
    if funding_records:
        df_funding = pd.DataFrame(funding_records)
        # 修正：フィールド名は "fundingRateTimestamp" を使用する
        df_funding["time"] = pd.to_datetime(df_funding["fundingRateTimestamp"].astype(int), unit="ms")
        df_funding.drop_duplicates(subset=["time"], inplace=True)
        df_funding.set_index("time", inplace=True)
        # 1時間足にリサンプリングし、線形補間
        df_funding_hourly = df_funding.resample("H").interpolate(method="linear").reset_index()
        print("資金調達率データ取得＆補間完了。")
    else:
        print("資金調達率データが取得できませんでした。")
        df_funding_hourly = pd.DataFrame(columns=["time", "fundingRate"])
    
    # Step5: オープンインタレストデータの取得（1時間足）
    print("オープンインタレストデータ取得中...")
    oi_records = fetch_open_interest_data(symbol=symbol, total_days=total_days)
    if oi_records:
        df_oi = pd.DataFrame(oi_records)
        df_oi["time"] = pd.to_datetime(df_oi["timestamp"].astype(int), unit="ms")
        df_oi.drop_duplicates(subset=["time"], inplace=True)
        df_oi = df_oi[["time", "openInterest"]]
        print("オープンインタレストデータ取得完了。")
    else:
        print("オープンインタレストデータが取得できませんでした。")
        df_oi = pd.DataFrame(columns=["time", "openInterest"])
    
    # Step6: 1時間足＋日足拡張データと資金調達率データをマージ
    df_merged = df_merged.sort_values("time")
    df_funding_hourly = df_funding_hourly.sort_values("time")
    print("merge_asofで資金調達率データをマージ中...")
    df_final = pd.merge_asof(df_merged, df_funding_hourly, on="time", direction="backward")
    
    # Step7: 最終的にオープンインタレストデータもマージ（1時間足を基準）
    df_final = pd.merge_asof(df_final.sort_values("time"), df_oi.sort_values("time"), on="time", direction="backward")
    
    # Step8: 統合データをCSVに出力
    output_file = "merged_dataset.csv"
    df_final.to_csv(output_file, index=False)
    print(f"最終統合データが '{output_file}' に保存されました。")

if __name__ == "__main__":
    main()
