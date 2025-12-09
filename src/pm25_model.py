import json
import pickle
from pathlib import Path
from datetime import timedelta

import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler
from statsmodels.tsa.statespace.sarimax import SARIMAX

from utils_pm25 import build_pm25_daily, build_exog_daily


#   CẤU HÌNH MÔ HÌNH & ĐƯỜNG DẪN

best_order = (1, 1, 2)
best_seasonal = (0, 0, 1, 7)
best_exog_cols = ['PM10',
                'NO2',
                'SO2',
                'pressure',
                'temperature',
                'rain',
                'rain_hours',
                'pressure_lag1',
                'temperature_lag1',
                'PM10_lag1']
TRANSFORM_METHOD = "log"

MODEL_DIRNAME = "models"
MODEL_FILENAME = "sarimax_pm25.pkl"
SCALER_FILENAME = "exog_scaler.pkl"
CONFIG_FILENAME = "config_pm25.json"



#  Quy đổi PM2.5 sang AQI
AQI_BREAKPOINTS = [
    (0.0, 12.0, 0, 50),
    (12.1, 35.4, 51, 100),
    (35.5, 55.4, 101, 150),
    (55.5, 150.4, 151, 200),
    (150.5, 250.4, 201, 300),
    (250.5, 500.0, 301, 500),
]

AQI_LEVELS = [
    {"range": (0, 50), "label": "Tốt", "color": "#66cc66"},
    {"range": (51, 100), "label": "Trung bình", "color": "#eec900"},
    {"range": (101, 150), "label": "Không tốt cho nhóm nhạy cảm", "color": "#FF7F24"},
    {"range": (151, 200), "label": "Xấu", "color": "#CD2626"},
    {"range": (201, 300), "label": "Rất Xấu", "color": "#CD2626"},
    {"range": (301, 500), "label": "Nguy hiểm", "color": "#b03060"},
]



def pm25_to_aqi(pm25: float) -> tuple[int, str, str]:
    """Chuyển PM2.5 sang (AQI, label, color)."""
    value = max(0.0, min(float(pm25), 500.0))

    for c_lo, c_hi, i_lo, i_hi in AQI_BREAKPOINTS:
        if c_lo <= value <= c_hi:
            aqi = int(round((i_hi - i_lo) / (c_hi - c_lo) * (value - c_lo) + i_lo))
            for level in AQI_LEVELS:
                if level["range"][0] <= aqi <= level["range"][1]:
                    return aqi, level["label"], level["color"]
            break

    return 0, "N/A", "#666666"


#   Train và lưu model

def train_and_save_final_model():
    root = Path(__file__).parent.parent
    path_2324 = root / "data" / "data2324.csv"
    path_2025 = root / "data" / "data25.csv"

    df_2324 = pd.read_csv(path_2324)
    try:
        df_2025 = pd.read_csv(path_2025)
        df_all = pd.concat([df_2324, df_2025], ignore_index=True)
    except FileNotFoundError:
        df_all = df_2324.copy()

    df_all["Local Time"] = pd.to_datetime(df_all["Local Time"], format='mixed')
    df_all = df_all.sort_values("Local Time")

    debug = root / "data" / "data_all.csv"
    df_all.to_csv(debug, index=False)
    
    pm25_daily = build_pm25_daily(df_all)
    y_full = pm25_daily.copy()


    exog_daily = build_exog_daily(df_all, y_full.index)
    missing = [c for c in best_exog_cols if c not in exog_daily.columns]
    if missing:
        raise ValueError(f"Thiếu cột: {missing}")
    exog_full = exog_daily[best_exog_cols].copy()

    print(
        f" Dữ liệu: {len(y_full)} ngày từ {y_full.index[0].date()} "
        f"→ {y_full.index[-1].date()}"
    )

    # Chuẩn hóa biến ngoại sinh
    print(" Đang chuẩn hóa biến ngoại sinh")
    scaler = StandardScaler()
    exog_scaled = pd.DataFrame(
        scaler.fit_transform(exog_full),
        index=exog_full.index,
        columns=best_exog_cols,
    )

    # Log dữ liệu
    if TRANSFORM_METHOD == "log":
        y_transformed = np.log(y_full + 1e-6)
    else:
        y_transformed = y_full.copy()

    # Train SARIMAX
    model = SARIMAX(
        y_transformed.astype(float),
        exog=exog_scaled.astype(float),
        order=best_order,
        seasonal_order=best_seasonal,
        enforce_stationarity=False,
        enforce_invertibility=False,
    )

    fitted_model = model.fit(maxiter=200, disp=True)
    print(" Hoàn thành!")

    # Lưu model và scaler
    model_dir = root / MODEL_DIRNAME
    model_dir.mkdir(exist_ok=True)

    with open(model_dir / MODEL_FILENAME, "wb") as f:
        pickle.dump(fitted_model, f)

    with open(model_dir / SCALER_FILENAME, "wb") as f:
        pickle.dump(scaler, f)

    print(" Đang cập nhật config")
    config_path = model_dir / CONFIG_FILENAME
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            cfg = json.load(f)
    except FileNotFoundError:
        cfg = {}

    cfg["best_order"] = list(best_order)
    cfg["best_seasonal"] = list(best_seasonal)
    cfg["best_exog_cols"] = list(best_exog_cols)
    cfg["exog_cols"] = list(best_exog_cols)  # Alias
    cfg["transform"] = TRANSFORM_METHOD
    cfg["lam"] = None
    cfg["last_train_date"] = str(y_full.index[-1].date())
    cfg["n_train_samples"] = len(y_full)

    with open(config_path, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)

    print("\n" + "=" * 60)
    print(" Hoàn thành train model!")
    print(f" Model:  {model_dir / MODEL_FILENAME}")
    print(f" Scaler: {model_dir / SCALER_FILENAME}")
    print(f" Config: {config_path}")
    print(f" Trained on: {len(y_full)} days")
    print("=" * 60)
    return fitted_model, scaler, cfg

#   Load model và config
def load_model_and_config():
    root = Path(__file__).parent.parent
    model_dir = root / MODEL_DIRNAME

    # Load model
    model_path = model_dir / MODEL_FILENAME
    if not model_path.exists():
        raise FileNotFoundError(f"Không tìm thấy model tại {model_path}")
    with open(model_path, "rb") as f:
        model = pickle.load(f)

    # Load scaler
    scaler_path = model_dir / SCALER_FILENAME
    if not scaler_path.exists():
        raise FileNotFoundError(f"Không tìm thấy scaler tại {scaler_path}")
    with open(scaler_path, "rb") as f:
        scaler = pickle.load(f)

    # Load config
    config_path = model_dir / CONFIG_FILENAME
    if not config_path.exists():
        raise FileNotFoundError(f"Không tìm thấy config tại {config_path}")
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    return model, scaler, config


def get_latest_historical_data():
    root = Path(__file__).parent.parent
    path_2324 = root / "data" / "data2324.csv"
    path_2025 = root / "data" / "data25.csv"

    df_2324 = pd.read_csv(path_2324)
    try:
        df_2025 = pd.read_csv(path_2025)
        df_all = pd.concat([df_2324, df_2025], ignore_index=True)
    except FileNotFoundError:
        df_all = df_2324.copy()

    df_all["Local Time"] = pd.to_datetime(df_all["Local Time"], format='mixed')
    df_all = df_all.sort_values("Local Time")

    # Tạo dữ liệu PM2.5
    y_series = build_pm25_daily(df_all)
    # Tạo dữ liệu ngoại sinh
    exog_df = build_exog_daily(df_all, y_series.index)

    return y_series, exog_df, df_all

#   Tạo biến ngoại sinh tương lai
def generate_future_exog(
    start_date: pd.Timestamp,
    horizon_days: int,
    exog_historical: pd.DataFrame,
    df_raw: pd.DataFrame,
    exog_cols: list,
) -> pd.DataFrame:
    future_dates = pd.date_range(start=start_date, periods=horizon_days, freq="D")
    n_lag_days = 3
    historical_tail = exog_historical.tail(n_lag_days).copy()
    exog_future_base = pd.DataFrame(index=future_dates)
    recent_data = exog_historical.tail(30)
    base_cols = [c for c in exog_cols if "_lag" not in c]

    for col in base_cols:
        col_mapping = {
            "PM10": "PM10",
            "NO2": "NO2",
            "SO2": "SO2",
            "pressure": "Pressure",
            "temperature": "Temperature",
            "wind_speed": "Wind Speed",
            "humidity": "Relative Humidity",
            "rain": "Precipitation",
            "rain_hours": "Precipitation",
        }

        if col in col_mapping:
            raw_col = col_mapping[col]

            for future_date in future_dates:
                mask = df_raw["Local Time"].dt.date == future_date.date()
                if mask.any():
                    if col == "rain_hours":
                        rain_data = df_raw.loc[mask, raw_col]
                        exog_future_base.at[future_date, col] = (rain_data > 0).sum()
                    elif col == "rain":
                        exog_future_base.at[future_date, col] = df_raw.loc[mask, raw_col].sum()
                    else:
                        exog_future_base.at[future_date, col] = df_raw.loc[mask, raw_col].mean()

        # Fill missing bằng median
        if col in exog_future_base.columns:
            exog_future_base[col] = exog_future_base[col].fillna(recent_data[col].median())
        else:
            exog_future_base[col] = recent_data[col].median()

    # Kết hợp để tính lag
    combined = pd.concat([historical_tail, exog_future_base])
    
    # Reset index để tránh trùng
    combined = combined.reset_index(drop=False).rename(columns={'index': 'date'}).set_index('date')
    
    # Remove duplicates in index
    combined = combined[~combined.index.duplicated(keep='last')]
    
    for col in exog_cols:
        if "_lag1" in col:
            base_col = col.replace("_lag1", "")
            if base_col in combined.columns:
                combined[col] = combined[base_col].shift(1)
        elif "_lag2" in col:
            base_col = col.replace("_lag2", "")
            if base_col in combined.columns:
                combined[col] = combined[base_col].shift(2)

    # Lấy phần future - sử dụng isin để filter thay vì reindex
    exog_future = combined[combined.index.isin(future_dates)][exog_cols].copy()
    # Reindex để đảm bảo đúng thứ tự
    exog_future = exog_future.reindex(future_dates)

    exog_future = exog_future.bfill().ffill()
    exog_future = exog_future.replace([np.inf, -np.inf], np.nan).fillna(0)

    assert len(exog_future) == horizon_days, f"Expected {horizon_days} rows, got {len(exog_future)}"

    return exog_future

#  Dự báo tương lai
def forecast_pm25_future(start_date=None, horizon_days: int = 3) -> pd.DataFrame:
    # 1. Load model và config
    model, scaler, config = load_model_and_config()
    exog_cols = config.get("exog_cols", config.get("best_exog_cols", []))
    transform_method = config.get("transform", "log")

    # 2. Load dữ liệu lịch sử
    y_hist, exog_hist, df_raw = get_latest_historical_data()

    # 3. Xác định ngày bắt đầu dự báo
    if start_date is None:
        start_date = y_hist.index[-1] + timedelta(days=1)
    else:
        start_date = pd.to_datetime(start_date)

    # 4. Tạo biến ngoại sinh tương lai
    exog_future = generate_future_exog(
        start_date=start_date,
        horizon_days=horizon_days,
        exog_historical=exog_hist,
        df_raw=df_raw,
        exog_cols=exog_cols,
    )

    # 5. Scale biến ngoại sinh
    exog_future_scaled = pd.DataFrame(
        scaler.transform(exog_future),
        index=exog_future.index,
        columns=exog_cols,
    )

    # 6. Dự báo
    fc = model.get_forecast(steps=horizon_days, exog=exog_future_scaled)
    preds_tf = fc.predicted_mean

    # 7. Inverse transform
    if transform_method == "log":
        preds = np.exp(preds_tf) - 1e-6
    else:
        preds = preds_tf
    preds = np.maximum(preds, 0)

    # 8. Chuyển đổi thành AQI và kết hợp kết quả
    results = []
    for date, pm25 in zip(exog_future.index, preds):
        aqi, label, color = pm25_to_aqi(pm25)
        results.append({
            "date": date,
            "PM25_forecast": round(float(pm25), 2),
            "AQI": aqi,
            "AQI_label": label,
            "AQI_color": color,
        })

    return pd.DataFrame(results)


def forecast_with_rolling_window(
    test_date: pd.Timestamp,
    horizon_days: int = 3,
    window_months: int = None  # Không dùng nữa, giữ để backward compatible
) -> pd.DataFrame:
    """
    Train model trên TOÀN BỘ data có sẵn trước test_date.
    Đây là realistic scenario để đạt độ chính xác cao nhất.
    """
    print(f"\n>>> Đánh giá độ chính xác model")
    print(f" Ngày kiểm tra: {test_date.strftime('%Y-%m-%d')}")
    
    root = Path(__file__).parent.parent
    path_2324 = root / "data" / "data2324.csv"
    path_2025 = root / "data" / "data25.csv"

    df_2324 = pd.read_csv(path_2324)
    try:
        df_2025 = pd.read_csv(path_2025)
        df_all = pd.concat([df_2324, df_2025], ignore_index=True)
    except FileNotFoundError:
        df_all = df_2324.copy()

    df_all["Local Time"] = pd.to_datetime(df_all["Local Time"], format='mixed')
    df_all = df_all.sort_values("Local Time")

    # Train trên TẤT CẢ data từ đầu đến trước test_date
    train_data = df_all[df_all["Local Time"] < test_date]
        
    if len(train_data) < 100:
        raise ValueError(f"Không đủ data training ({len(train_data)} records). Cần ít nhất 100 ngày.")
    
    print(f" Training: {len(train_data)} records ({train_data['Local Time'].min().date()} → {train_data['Local Time'].max().date()})")

    pm25_daily = build_pm25_daily(train_data)
    exog_df = build_exog_daily(train_data, pm25_daily.index)
    missing = [c for c in best_exog_cols if c not in exog_df.columns]
    if missing:
        raise ValueError(f"Missing cột : {missing}. Có sẵn: {list(exog_df.columns)}")
    
    # Scale exog variables
    scaler = StandardScaler()
    exog_scaled = pd.DataFrame(
        scaler.fit_transform(exog_df[best_exog_cols]),
        index=exog_df.index,
        columns=best_exog_cols
    )
    
    # Log transform PM2.5
    y_transformed = np.log(pm25_daily + 1e-6)
    
    # Train SARIMAX model
    print(" Training SARIMAX")
    model = SARIMAX(
        y_transformed,
        exog=exog_scaled,
        order=(1, 1, 2),
        seasonal_order=(0, 0, 1, 7),
        enforce_stationarity=False,
        enforce_invertibility=False,
    )
    
    results = model.fit(disp=False)
    print(f" Train thành công (AIC: {results.aic:.1f})")
    
    # Generate future exog
    exog_future = generate_future_exog(
        start_date=test_date,
        horizon_days=horizon_days,
        exog_historical=exog_df,
        df_raw=df_all,
        exog_cols=best_exog_cols
    )
    
    # Scale future exog
    exog_future_scaled = pd.DataFrame(
        scaler.transform(exog_future),
        index=exog_future.index,
        columns=best_exog_cols
    )
    
    # Forecast
    print(f" Dự báo {horizon_days} ngày...")
    forecast_transformed = results.forecast(steps=horizon_days, exog=exog_future_scaled)
    
    # Inverse log transform
    forecast_pm25 = np.exp(forecast_transformed) - 1e-6
    forecast_pm25 = np.maximum(forecast_pm25, 0)
    
    # Build result
    future_dates = pd.date_range(start=test_date, periods=horizon_days, freq="D")
    forecast_results = []
    
    for date, pm25 in zip(future_dates, forecast_pm25):
        aqi, label, color = pm25_to_aqi(pm25)  # Returns (aqi, label, color) not dict
        
        forecast_results.append({
            "date": date,
            "PM25_forecast": round(float(pm25), 2),
            "AQI": aqi,
            "AQI_label": label,
            "AQI_color": color,
        })
    
    df_result = pd.DataFrame(forecast_results)
    print(f" Dự báo hoàn thành: PM2.5  {df_result['PM25_forecast'].min():.1f} - {df_result['PM25_forecast'].max():.1f}")
    
    return df_result
#   CLI ĐƠN GIẢN KHI CHẠY TRỰC TIẾP

if __name__ == "__main__":
    print("Bắt đầu train model: ")
    model, scaler, cfg = train_and_save_final_model()

    print("\n>>> TEST DỰ BÁO 3 NGÀY SAU NGÀY CUỐI CÙNG")
    df = forecast_pm25_future(horizon_days=3)
    print("\n" + "=" * 60)
    print(df.to_string(index=False))
    print("=" * 60)
