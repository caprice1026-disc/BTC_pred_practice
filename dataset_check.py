import pandas as pd

def check_missing_values(df):
    """各列の欠損値件数をチェックする"""
    missing = df.isnull().sum()
    issues = []
    for col, cnt in missing.items():
        if cnt > 0:
            issues.append(f"列 '{col}' に欠損値が {cnt} 件存在します。")
    return issues

def check_negative_values(df, cols):
    """
    指定した数値列について、マイナス値が含まれている場合に問題として報告する。
    例：volume, turnover, ATR, openInterestなど、通常は非負であるべき値。
    """
    issues = []
    for col in cols:
        if col in df.columns:
            negatives = (df[col] < 0).sum()
            if negatives > 0:
                issues.append(f"列 '{col}' に負の値が {negatives} 件あります。")
    return issues

def check_time_duplicates_and_order(df, time_col="time"):
    """時刻列の重複と昇順になっているかをチェックする"""
    issues = []
    if df[time_col].duplicated().any():
        dup_count = df[time_col].duplicated().sum()
        issues.append(f"時刻列 '{time_col}' に重複が {dup_count} 件あります。")
    if not df[time_col].is_monotonic_increasing:
        issues.append(f"時刻列 '{time_col}' が昇順になっていません。")
    return issues

def check_dtypes(df, expected_dtypes):
    """
    expected_dtypes: dict形式で {列名: dtype} を指定
    期待する型と異なる場合に問題として報告する。
    """
    issues = []
    for col, dtype in expected_dtypes.items():
        if col in df.columns:
            if not pd.api.types.is_dtype_equal(df[col].dtype, dtype):
                issues.append(f"列 '{col}' のdtypeが {df[col].dtype} ですが、期待は {dtype} です。")
    return issues

def main():
    # CSVファイル読み込み（time列は日付型として読み込む）
    input_file = "merged_dataset.csv"
    df = pd.read_csv(input_file, parse_dates=["time"])
    
    issues = []
    
    # 1. 欠損値チェック
    issues.extend(check_missing_values(df))
    
    # 2. 数値が非負であるべき列のチェック
    numeric_cols = ["volume", "turnover", "ATR", "openInterest", "fundingRate"]
    # ※ fundingRateは場合によってはゼロや正負がある可能性もあるので、必要に応じて調整
    issues.extend(check_negative_values(df, numeric_cols))
    
    # 3. 時刻の重複と順序チェック
    issues.extend(check_time_duplicates_and_order(df, time_col="time"))
    
    # 4. 各列のdtypeチェック（例として、open, high, low, close, volume, turnover, ATR, fundingRate, openInterestは数値型）
    expected = {
        "open": "float64",
        "high": "float64",
        "low": "float64",
        "close": "float64",
        "volume": "float64",
        "turnover": "float64",
        "ATR": "float64",
        "fundingRate": "float64",
        "openInterest": "float64"
    }
    issues.extend(check_dtypes(df, expected))
    
    # まとめてテキストファイルに出力
    output_file = "preprocessing_issues.txt"
    with open(output_file, "w", encoding="utf-8") as f:
        if issues:
            f.write("前処理上の潜在的な問題点:\n")
            for issue in issues:
                f.write(issue + "\n")
        else:
            f.write("問題は検出されませんでした。")
    
    print(f"検証結果が '{output_file}' に保存されました。")

if __name__ == "__main__":
    main()
