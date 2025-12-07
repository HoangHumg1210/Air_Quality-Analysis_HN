import json
from pathlib import Path
from datetime import timedelta

import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_absolute_error, mean_squared_error
from statsmodels.tsa.statespace.sarimax import SARIMAX

from utils_pm25 import build_pm25_daily, build_exog_daily

# =======================
# 1. Thông số mô hình
# =======================
best_order = (1, 1, 2)
best_seasonal = (0, 0, 1, 7)

best_exog_cols = [
    "PM10",
    "NO2",
    "SO2",
    "pressure",
    "temperature",
    "wind_speed",
    "humidity",
    "rain",
    "rain_hours",
    "pressure_lag1",
    "temperature_lag1",
    "humidity_lag1",
    "PM10_lag1",
]


def weekly_walk_forward_eval(
    start_eval_date=None,
    end_eval_date=None,
    horizon_days: int = 7,
    step_days: int = 7,
    min_train_days: int = 180,
):
    """
    Walk-forward evaluation kiểu:
        - Mỗi lần train trên toàn bộ dữ liệu trước ngày forecast_start
        - Dự báo 7 ngày tiếp theo
        - Dịch cửa sổ 7 ngày và lặp lại

    Kết quả:
        - Lưu weekly_eval_results.csv trong thư mục models/
        - Ghi thêm 'weekly_eval' vào config_pm25.json
    """

    # =======================
    # 2. Đọc & gộp dữ liệu
    # =======================
    root = Path(__file__).parent.parent
    path_2324 = root / "data" / "data (2023-2024).csv"
    path_2025 = root / "data" / "data_2025.csv"

    df_2324 = pd.read_csv(path_2324)
    try:
        df_2025 = pd.read_csv(path_2025)
        df_all = pd.concat([df_2324, df_2025], ignore_index=True)
    except FileNotFoundError:
        df_all = df_2324.copy()

    df_all["Local Time"] = pd.to_datetime(df_all["Local Time"])
    df_all = df_all.sort_values("Local Time")

    # =======================
    # 3. Xây y_full & exog_full
    # =======================
    pm25_daily = build_pm25_daily(df_all)
    y_full = pm25_daily.copy()

    exog_daily = build_exog_daily(df_all, y_full.index)
    missing = [c for c in best_exog_cols if c not in exog_daily.columns]
    if missing:
        raise ValueError(f"Thiếu cột exogenous trong exog_daily: {missing}")
    exog_full = exog_daily[best_exog_cols].copy()

    # =======================
    # 4. Xác định khoảng đánh giá
    # =======================
    if start_eval_date is None:
        # Mặc định: bắt đầu eval sau min_train_days
        if len(y_full) <= min_train_days:
            raise ValueError("Chuỗi thời gian quá ngắn cho min_train_days.")
        start_eval_date = y_full.index[min_train_days]
    else:
        start_eval_date = pd.to_datetime(start_eval_date)

    if end_eval_date is None:
        end_eval_date = y_full.index[-1]
    else:
        end_eval_date = pd.to_datetime(end_eval_date)

    if start_eval_date <= y_full.index[0]:
        raise ValueError("start_eval_date phải sau một khoảng train ban đầu.")

    print("Đánh giá từ:", start_eval_date.date(), "→", end_eval_date.date())

    # =======================
    # 5. Vòng lặp walk-forward
    # =======================
    results = []
    current_start = start_eval_date
    segment_id = 0

    while current_start <= end_eval_date:
        forecast_start = current_start

        # train: tất cả ngày trước forecast_start
        train_mask = y_full.index < forecast_start
        if train_mask.sum() < min_train_days:
            print("Train days < min_train_days, dừng vòng lặp.")
            break

        forecast_end = forecast_start + timedelta(days=horizon_days - 1)
        if forecast_end > end_eval_date:
            forecast_end = end_eval_date

        print(
            f"\nTrain đến: {forecast_start.date()-timedelta(days=1)} "
            f"({train_mask.sum()} ngày), dự báo {forecast_start.date()} → {forecast_end.date()}"
        )

        y_train = y_full.loc[train_mask]
        exog_train = exog_full.loc[train_mask]

        y_future_true = y_full.loc[forecast_start:forecast_end]
        exog_future = exog_full.loc[forecast_start:forecast_end]

        # Scale exog theo train
        scaler = StandardScaler()
        scaler.fit(exog_train)
        exog_train_scaled = pd.DataFrame(
            scaler.transform(exog_train),
            index=exog_train.index,
            columns=best_exog_cols,
        )
        exog_future_scaled = pd.DataFrame(
            scaler.transform(exog_future),
            index=exog_future.index,
            columns=best_exog_cols,
        )

        # Log-transform y
        y_train_tf = np.log(y_train + 1e-6)

        # Fit SARIMAX
        model = SARIMAX(
            y_train_tf.astype(float),
            exog=exog_train_scaled.astype(float),
            order=best_order,
            seasonal_order=best_seasonal,
            enforce_stationarity=False,
            enforce_invertibility=False,
        )
        res = model.fit(maxiter=200, disp=False)

        # Forecast
        fc = res.get_forecast(
            steps=len(exog_future_scaled),
            exog=exog_future_scaled,
        )
        preds_tf = fc.predicted_mean
        preds = np.exp(preds_tf) - 1e-6
        preds.index = y_future_true.index

        df_seg = pd.DataFrame(
            {
                "date": y_future_true.index,
                "actual": y_future_true.values,
                "predicted": preds.values,
                "train_end": y_train.index[-1],
                "segment_id": segment_id,
            }
        )
        results.append(df_seg)
        segment_id += 1

        # Sang tuần tiếp theo (non-overlap)
        current_start = forecast_end + timedelta(days=1)

    if not results:
        raise ValueError("Không tạo được bất kỳ đoạn dự báo nào (results rỗng).")

    # =======================
    # 6. Gộp kết quả + metrics
    # =======================
    all_results = pd.concat(results, ignore_index=True)
    all_results.sort_values("date", inplace=True)

    mae = mean_absolute_error(all_results["actual"], all_results["predicted"])
    rmse = np.sqrt(mean_squared_error(all_results["actual"], all_results["predicted"]))
    mape = (
        np.mean(
            np.abs(
                (all_results["actual"] - all_results["predicted"])
                / np.clip(all_results["actual"], 1e-6, None)
            )
        )
        * 100
    )

    metrics = {"mae": float(mae), "rmse": float(rmse), "mape": float(mape)}

    # =======================
    # 7. Lưu csv + update config
    # =======================
    model_dir = root / "models"
    model_dir.mkdir(exist_ok=True)
    all_results.to_csv(model_dir / "weekly_eval_results.csv", index=False)

    config_path = model_dir / "config_pm25.json"
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            cfg = json.load(f)
    except FileNotFoundError:
        cfg = {}

    cfg.setdefault("weekly_eval", {})
    cfg["weekly_eval"]["horizon_days"] = horizon_days
    cfg["weekly_eval"]["step_days"] = step_days
    cfg["weekly_eval"]["start_eval"] = str(start_eval_date.date())
    cfg["weekly_eval"]["end_eval"] = str(end_eval_date.date())
    cfg["weekly_eval"]["metrics"] = metrics

    with open(config_path, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)

    print("\n  Walk-forward weekly eval xong.")
    print(f"MAE  = {mae:.3f}")
    print(f"RMSE = {rmse:.3f}")
    print(f"MAPE = {mape:.2f}%")

    return all_results, metrics


if __name__ == "__main__":
    # Ví dụ: đánh giá từ 2025-02-01 đến hết dữ liệu
    weekly_walk_forward_eval(
        start_eval_date="2025-01-01",
        horizon_days=7,
        step_days=7,
        min_train_days=180,
    )
