import pandas as pd
import numpy as np
import math
import itertools
from sklearn.metrics import mean_squared_error, r2_score
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Dropout
from tensorflow.keras.optimizers import Adam
from tensorflow.keras.callbacks import EarlyStopping


# -------------------------------
# データ読み込みと前処理
# -------------------------------
def load_and_preprocess_data(csv_file):
    """
    CSVファイルからデータを読み込み、'time'列を日付型に変換し、
    ターゲット(return_pct)の欠損値を削除、特徴量とターゲットを抽出して返す関数
    """
    df = pd.read_csv(csv_file, parse_dates=['time'])
    df = df.dropna(subset=['return_pct'])
    feature_cols = ["open", "high", "low", "close", "volume", "ATR", "MA20", "RSI", "EMA", "openInterest"]
    X = df[feature_cols].values
    y = df['return_pct'].values
    return X, y, feature_cols

# -------------------------------
# 学習データの分割（時系列順に80%/20%）
# -------------------------------
def split_data(X, y, split_ratio=0.8):
    """
    データを時系列順にトレーニングとテストに分割する関数
    """
    split_index = int(split_ratio * len(X))
    X_train, X_test = X[:split_index], X[split_index:]
    y_train, y_test = y[:split_index], y[split_index:]
    return X_train, X_test, y_train, y_test

# -------------------------------
# モデル構築
# -------------------------------
def build_model(input_dim, hidden_layers, neurons, dropout_rate):
    """
    入力次元、隠れ層数、各層のユニット数、ドロップアウト率を指定してKerasモデルを構築する関数
    """
    model = Sequential()
    # 入力層＋第1隠れ層
    model.add(Dense(neurons, activation='relu', input_dim=input_dim))
    if dropout_rate > 0:
        model.add(Dropout(dropout_rate))
    # 指定された隠れ層数-1分の隠れ層追加
    for _ in range(hidden_layers - 1):
        model.add(Dense(neurons, activation='relu'))
        if dropout_rate > 0:
            model.add(Dropout(dropout_rate))
    # 出力層（回帰問題なので線形活性化）
    model.add(Dense(1, activation='linear'))
    return model

# -------------------------------
# ハイパーパラメータ探索と評価
# -------------------------------
def hyperparameter_search(X_train, X_test, y_train, y_test, input_dim):
    """
    複数のハイパーパラメータの組み合わせでモデルを学習し、RMSEとR²の結果をリストとして返す関数
    """
    results = []
    param_grid = {
        'hidden_layers': [1, 2],
        'neurons': [32, 64],
        'dropout_rate': [0.0, 0.2],
        'learning_rate': [0.001, 0.01],
        'epochs': [50, 100],
        'batch_size': [32]
    }
    
    # 全組み合わせでループ
    for hidden_layers, neurons, dropout_rate, learning_rate, epochs, batch_size in itertools.product(
            param_grid['hidden_layers'],
            param_grid['neurons'],
            param_grid['dropout_rate'],
            param_grid['learning_rate'],
            param_grid['epochs'],
            param_grid['batch_size']):
        
        print(f"Training DL model with layers={hidden_layers}, neurons={neurons}, dropout={dropout_rate}, lr={learning_rate}, epochs={epochs}, batch_size={batch_size}")
        
        model = build_model(input_dim, hidden_layers, neurons, dropout_rate)
        optimizer = Adam(learning_rate=learning_rate)
        model.compile(optimizer=optimizer, loss='mse')
        
        # EarlyStoppingで過学習対策
        early_stop = EarlyStopping(monitor='val_loss', patience=5, restore_best_weights=True, verbose=0)
        
        history = model.fit(X_train, y_train,
                            validation_split=0.1,
                            epochs=epochs,
                            batch_size=batch_size,
                            verbose=0,
                            callbacks=[early_stop])
        
        y_pred = model.predict(X_test).flatten()
        mse_val = mean_squared_error(y_test, y_pred)
        rmse_val = math.sqrt(mse_val)
        r2_val = r2_score(y_test, y_pred)
        
        print(f"Result: RMSE={rmse_val:.4f}, R²={r2_val:.4f}\n")
        results.append({
            'hidden_layers': hidden_layers,
            'neurons': neurons,
            'dropout_rate': dropout_rate,
            'learning_rate': learning_rate,
            'epochs': epochs,
            'batch_size': batch_size,
            'RMSE': rmse_val,
            'R2': r2_val
        })
    return results

# -------------------------------
# メイン処理
# -------------------------------
def main():
    """
    CSVファイルからデータを読み込み、深層学習モデルを複数のハイパーパラメータ設定で学習し、
    各設定の結果（RMSE、R²）をCSVに書き出す。
    """
    # データ読み込みと前処理
    csv_file = 'merged_dataset_with_return.csv'
    X, y, feature_cols = load_and_preprocess_data(csv_file)
    X_train, X_test, y_train, y_test = split_data(X, y, split_ratio=0.8)
    input_dim = X_train.shape[1]
    
    # ハイパーパラメータ探索と評価
    results = hyperparameter_search(X_train, X_test, y_train, y_test, input_dim)
    results_df = pd.DataFrame(results)
    results_df = results_df.sort_values(by='RMSE')
    output_results = "dl_hyperparameter_results.csv"
    results_df.to_csv(output_results, index=False)
    print(f"ハイパーパラメータの結果は '{output_results}' に保存されました。")

if __name__ == "__main__":
    main()
