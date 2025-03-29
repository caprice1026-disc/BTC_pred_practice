import pandas as pd
from sklearn.model_selection import train_test_split
import lightgbm as lgb
from sklearn.metrics import mean_squared_error, r2_score
import matplotlib.pyplot as plt
import seaborn as sns

# CSVファイルを読み込み、time列は日付型に変換
df = pd.read_csv('merged_dataset_with_return.csv', parse_dates=['time'])

# ターゲット変数(return_pct)がNaNの行を削除
df = df.dropna(subset=['return_pct'])

# 使用する特徴量の選定
feature_cols = ["open", "high", "low", "close", "volume", "ATR", "MA20", "RSI", "EMA", "fundingRate", "openInterest"]

# 入力データとターゲットの設定
X = df[feature_cols]
y = df['return_pct']

# 時系列データなので、先頭80%をトレーニング、残りをテストに分割
split_index = int(0.8 * len(df))
X_train, X_test = X.iloc[:split_index], X.iloc[split_index:]
y_train, y_test = y.iloc[:split_index], y.iloc[split_index:]

# LightGBMの回帰モデルのインスタンス作成
model = lgb.LGBMRegressor(n_estimators=100, learning_rate=0.1)

# モデルの学習
model.fit(X_train, y_train)

# テストデータで予測
y_pred = model.predict(X_test)

# 予測精度の評価：まずMSEを計算し、RMSEを算出
mse = mean_squared_error(y_test, y_pred)
rmse = mse ** 0.5
r2 = r2_score(y_test, y_pred)

print("RMSE:", rmse)
print("R²:", r2)

# 特徴量の重要度の表示＆保存
importances = model.feature_importances_
feature_importances = pd.DataFrame({'Feature': feature_cols, 'Importance': importances})
feature_importances = feature_importances.sort_values(by='Importance', ascending=False)

plt.figure(figsize=(10, 6))
sns.barplot(x='Importance', y='Feature', data=feature_importances)
plt.title('Feature Importances')
plt.xlabel('Importance')
plt.ylabel('Feature')
# グラフをファイルに保存
plt.savefig("feature_importances.png", dpi=300, bbox_inches='tight')
plt.show()

# モデルの保存
model.save_model('lgb_model.txt')
print("モデルは 'lgb_model.txt' に保存されました。")
# 予測結果をCSVファイルとして保存
predictions_df = pd.DataFrame({'time': df['time'].iloc[split_index:], 'actual': y_test, 'predicted': y_pred})
predictions_df.to_csv('predictions.csv', index=False)
print("予測結果は 'predictions.csv' に保存されました。")
# 予測結果の可視化
plt.figure(figsize=(12, 6))
plt.plot(predictions_df['time'], predictions_df['actual'], label='Actual', color='blue')