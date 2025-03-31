import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import math

# CSVファイルのパス（ローカル環境）
input_file = 'merged_dataset_with_return_and_abs.csv'

# CSVファイルを読み込み、time列は日付型に変換
df = pd.read_csv(input_file, parse_dates=['time'])

# 基本統計量と情報を表示
print("=== DataFrame Head ===")
print(df.head())
print("\n=== DataFrame Info ===")
print(df.info())
print("\n=== 基本統計量 ===")
print(df.describe())

# -------------------------------
# 数値変数の分布確認（ヒストグラム）
# -------------------------------
# 数値型の列リストを取得
numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()

# グリッドサイズを自動計算（例：列数4、行数 = ceil(全数値列数/4)）
n = len(numeric_cols)
ncols = 4
nrows = math.ceil(n / ncols)

plt.figure(figsize=(4*ncols, 3*nrows))
for i, col in enumerate(numeric_cols, 1):
    plt.subplot(nrows, ncols, i)
    sns.histplot(df[col], bins=50, kde=True)
    plt.title(col)
    plt.tight_layout()
plt.suptitle("Histograms of Numeric Columns", fontsize=16, y=1.02)
plt.savefig("numeric_histograms.png", dpi=300, bbox_inches='tight')
plt.show()

# -------------------------------
# 相関行列の計算とヒートマップ
# -------------------------------
corr_matrix = df[numeric_cols].corr()
plt.figure(figsize=(14, 12))
sns.heatmap(corr_matrix, annot=True, fmt=".2f", cmap="coolwarm")
plt.title("Correlation Matrix")
plt.tight_layout()
plt.savefig("correlation_matrix.png", dpi=300, bbox_inches='tight')
plt.show()

# -------------------------------
# ターゲット(return_abs)の分布確認
# -------------------------------
plt.figure(figsize=(8,6))
sns.histplot(df['return_abs'], bins=50, kde=True)
plt.title("Distribution of Target (return_abs)")
plt.xlabel("return_abs")
plt.ylabel("Frequency")
plt.tight_layout()
plt.savefig("target_distribution.png", dpi=300, bbox_inches='tight')
plt.show()

# -------------------------------
# 時系列プロット（例：close と return_abs）
# -------------------------------
plt.figure(figsize=(12,6))
plt.plot(df['time'], df['close'], label='Close Price')
plt.xlabel("Time")
plt.ylabel("Price")
plt.title("Time Series of Close Price")
plt.legend()
plt.tight_layout()
plt.savefig("time_series_close.png", dpi=300, bbox_inches='tight')
plt.show()

plt.figure(figsize=(12,6))
plt.plot(df['time'], df['return_abs'], label='Return Absolute', color='orange')
plt.xlabel("Time")
plt.ylabel("Absolute Price Difference")
plt.title("Time Series of Return Absolute")
plt.legend()
plt.tight_layout()
plt.savefig("time_series_return_abs.png", dpi=300, bbox_inches='tight')
plt.show()

# -------------------------------
# 特定の特徴量とターゲット(return_abs)の散布図
# -------------------------------
selected_features = ['volume', 'turnover', 'ATR', 'RSI', 'EMA']
for col in selected_features:
    plt.figure(figsize=(8,6))
    sns.scatterplot(x=df[col], y=df['return_abs'])
    plt.title(f"Scatter Plot: {col} vs return_abs")
    plt.xlabel(col)
    plt.ylabel("return_abs")
    plt.tight_layout()
    plt.savefig(f"scatter_{col}_vs_return_abs.png", dpi=300, bbox_inches='tight')
    plt.show()

print("探索的データ解析の結果を各画像ファイルとして保存しました。")
