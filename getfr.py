from pybit.unified_trading import HTTP
import csv
import time
from datetime import datetime, timedelta

# 出力内容を他と合わせる必要あり

session = HTTP()

# 設定
CATEGORY = "linear"
SYMBOL = "BTCUSD"  
CSV_FILE = "funding_rates.csv"

# 取得期間の設定（ミリ秒単位）
start_date = datetime(2020, 4, 1)
end_date   = datetime(2024, 12, 1)
start_ts = int(start_date.timestamp() * 1000)
end_ts   = int(end_date.timestamp() * 1000)

# 1回のリクエストで取得する件数（limit の最大値は200）
limit = 200

# 1回のリクエストで問い合わせる期間（例：30日）
window_ms = 30 * 24 * 60 * 60 * 1000  # 30日分のミリ秒

with open(CSV_FILE, mode='w', newline='', encoding='utf-8') as csv_file:
    fieldnames = ["fundingRateTimestamp", "fundingRate"]
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    writer.writeheader()
    
    current_start = start_ts
    while current_start < end_ts:
        current_end = min(current_start + window_ms, end_ts)
        
        try:
            response = session.get_funding_rate_history(
                category=CATEGORY,
                symbol=SYMBOL,
                startTime=current_start,
                endTime=current_end,
                limit=limit
            )
        except Exception as e:
            print("API呼び出し中に例外発生:", e)
            break
        
        if response.get("retCode") != 0:
            print("APIエラー:", response.get("retMsg"))
            break
        
        result = response.get("result", {})
        records = result.get("list", [])
        print(f"{datetime.fromtimestamp(current_start/1000)} ～ {datetime.fromtimestamp(current_end/1000)}: {len(records)} 件取得")
        
        if records:
            for record in records:
                writer.writerow({
                    "fundingRateTimestamp": record.get("fundingRateTimestamp"),
                    "fundingRate": record.get("fundingRate")
                })
            # もし取得件数が limit 件に達していれば、ウィンドウ内に更にデータがある可能性があるので、
            # 最後のレコードのタイムスタンプ+1ms を次の開始時刻とする
            if len(records) == limit:
                last_timestamp = int(records[-1].get("fundingRateTimestamp"))
                next_start = last_timestamp + 1
                # next_start が current_end よりも前なら、ウィンドウを分割して続行
                if next_start < current_end:
                    current_start = next_start
                else:
                    current_start = current_end
            else:
                current_start = current_end
        else:
            # データがなければウィンドウを進める
            current_start = current_end
        
        # レートリミット対策としてウェイト（必要に応じて調整）
        time.sleep(1.1)

print("指定期間内の funding rate データを CSV に保存しました:", CSV_FILE)
