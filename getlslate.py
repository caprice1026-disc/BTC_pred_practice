from pybit.unified_trading import HTTP
import csv
import time
from datetime import datetime

# セッション作成
session = HTTP()

# AIに聞いた。＠自分　壊れているので修正してね。

# 設定
CATEGORY = "linear"           # USDT契約の場合。逆指値の場合は "inverse" に変更
SYMBOL = "BTCUSDT"            # 例：BTCUSDT（大文字で指定）
PERIOD = "1h"                 # 1時間ごとのデータ
CSV_FILE = "long_short_ratio.csv"

# 取得期間の設定（ミリ秒）
start_date = datetime(2020, 4, 1)
end_date   = datetime(2024, 12, 1)
start_ts = int(start_date.timestamp() * 1000)
end_ts   = int(end_date.timestamp() * 1000)

limit = 500  # 1回のリクエストで取得する件数（最大500件）

# 1回のリクエストで問い合わせる期間：ここでは1日（24時間）単位
window_ms = 24 * 60 * 60 * 1000

with open(CSV_FILE, mode='w', newline='', encoding='utf-8') as csv_file:
    fieldnames = ["timestamp", "buyRatio", "sellRatio", "symbol"]
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    writer.writeheader()
    csv_file.flush()

    current_start = start_ts
    while current_start < end_ts:
        current_end = min(current_start + window_ms, end_ts)
        cursor = None

        while True:
            params = {
                "category": CATEGORY,
                "symbol": SYMBOL,
                "period": PERIOD,
                "startTime": str(current_start),
                "endTime": str(current_end),
                "limit": limit
            }
            if cursor:
                params["cursor"] = cursor

            try:
                response = session.get_long_short_ratio(**params)
            except Exception as e:
                print(f"API呼び出し中に例外発生: {e}")
                break

            if response.get("retCode") != 0:
                print("APIエラー:", response.get("retMsg"))
                break

            result = response.get("result", {})
            records = result.get("list", [])
            print(f"{datetime.fromtimestamp(current_start/1000)} ～ {datetime.fromtimestamp(current_end/1000)} : {len(records)} 件取得")

            # 取得した各レコードを都度CSVへ書き込む
            for record in records:
                writer.writerow({
                    "timestamp": record.get("timestamp"),
                    "buyRatio": record.get("buyRatio"),
                    "sellRatio": record.get("sellRatio"),
                    "symbol": record.get("symbol")
                })
            csv_file.flush()  # その都度フラッシュしてディスクに保存

            # 次ページがあればカーソル更新、なければウィンドウ終了
            cursor = result.get("nextPageCursor")
            if not cursor:
                break

            # レートリミットに配慮して sleep（必要に応じて調整）
            time.sleep(1.1)

        current_start = current_end
        # 各ウィンドウ間もレートリミットに配慮して sleep
        time.sleep(1.1)

print("指定期間内のロング・ショート比率データを CSV に保存しました:", CSV_FILE)
