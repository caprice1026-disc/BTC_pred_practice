from pybit.unified_trading import HTTP
import csv
import time
from datetime import datetime

# セッション作成（テストネットの場合は testnet=True を設定）
session = HTTP(testnet=False)

# 設定
CATEGORY = "linear"         # USDT契約の場合。逆指値の場合は "inverse" に変更
SYMBOL = "BTCUSDT"          # 例：BTCUSDT（大文字で指定）
INTERVAL = "1h"             # 1時間ごとのデータ。利用可能な値: 5min,15min,30min,1h,4h,1d
CSV_FILE = "open_interest.csv"

# ※オープンインタレストのデータは比較的新しい期間のみ取得可能な場合があるため、
# ここでは例として 2023-09-30～2023-10-03 の期間を指定しています。
start_date = datetime(2020, 4, 1)
end_date   = datetime(2024, 12, 1)
start_ts = int(start_date.timestamp() * 1000)
end_ts   = int(end_date.timestamp() * 1000)

limit = 200  # 1回のリクエストで取得する件数（最大200件）
# 1回のリクエストで問い合わせる期間：ここでは1日（24時間）単位
window_ms = 24 * 60 * 60 * 1000

with open(CSV_FILE, mode='w', newline='', encoding='utf-8') as csv_file:
    fieldnames = ["timestamp", "openInterest", "symbol"]
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    writer.writeheader()

    current_start = start_ts
    while current_start < end_ts:
        current_end = min(current_start + window_ms, end_ts)
        cursor = None

        while True:
            params = {
                "category": CATEGORY,
                "symbol": SYMBOL,
                "intervalTime": INTERVAL,  # intervalTimeを指定
                "startTime": current_start,  # 整数型で渡す
                "endTime": current_end,      # 整数型で渡す
                "limit": limit
            }
            if cursor:
                params["cursor"] = cursor

            try:
                response = session.get_open_interest(**params)
            except Exception as e:
                print(f"API呼び出し中に例外発生: {e}")
                break

            if response.get("retCode") != 0:
                print("APIエラー:", response.get("retMsg"))
                break

            result = response.get("result", {})
            records = result.get("list", [])
            print(f"{datetime.fromtimestamp(current_start/1000)} ～ {datetime.fromtimestamp(current_end/1000)} : {len(records)} 件取得")

            # 各レコードをCSVへ書き込む
            for record in records:
                writer.writerow({
                    "timestamp": record.get("timestamp"),
                    "openInterest": record.get("openInterest"),
                    "symbol": SYMBOL  # APIにシンボルが含まれない場合も考慮して固定で設定
                })

            # 次ページがあればカーソル更新、なければウィンドウ終了
            cursor = result.get("nextPageCursor")
            if not cursor:
                break

            # レートリミットに配慮してsleep
            time.sleep(1.1)

        current_start = current_end
        # 各ウィンドウ間のsleep
        time.sleep(1.1)

print("指定期間内のオープンインタレストデータを CSV に保存しました:", CSV_FILE)
