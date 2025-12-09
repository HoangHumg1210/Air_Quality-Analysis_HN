
import numpy as np
import pandas as pd
from scipy import stats

# 1. Biến ngày nghỉ / ngày lễ

holiday_periods = {
    # 2023
    'Tết 2023': ('2023-01-20', '2023-01-26'),
    'Lễ 30/4-1/5 2023': ('2023-04-29', '2023-05-03'),
    'Lễ 2/9 2023': ('2023-08-31', '2023-09-04'),

    # 2024
    'Tết Dương 2024': ('2023-12-30', '2024-01-01'),
    'Tết 2024': ('2024-02-08', '2024-02-14'),
    'Lễ 30/4-1/5 2024': ('2024-04-27', '2024-05-01'),
    'Lễ 2/9 2024': ('2024-08-31', '2024-09-03'),

    # 2025
    'Tết Dương 2025': ('2025-01-01', '2025-01-01'),
    'Lễ 30/4-1/5 2025': ('2025-04-27', '2025-05-01'),
    'Lễ 2/9 2025': ('2025-08-30', '2025-09-02'),
}



# 2. Hàm xử lý PM2.5 theo ngày
def build_pm25_daily(df: pd.DataFrame) -> pd.Series:
    df = df.copy()
    df["Local Time"] = pd.to_datetime(df["Local Time"])
    df = df.set_index("Local Time").sort_index()

    pm25 = df["PM25"].resample("D").mean()
    pm25 = pm25.asfreq("D")
    return pm25



# 3. Tìm biến ngoại sinh
def build_exog_daily(df: pd.DataFrame, index_target: pd.DatetimeIndex) -> pd.DataFrame:
    df = df.copy()
    df["Local Time"] = pd.to_datetime(df["Local Time"])
    df = df.set_index("Local Time").sort_index()
    exog_df = pd.DataFrame(index=index_target)

    
    if "PM10" in df.columns:
        exog_df["PM10"] = df["PM10"].resample("D").mean().reindex(index_target)
    if "NO2" in df.columns:
        exog_df["NO2"] = df["NO2"].resample("D").mean().reindex(index_target)
    if "SO2" in df.columns:
        exog_df["SO2"] = df["SO2"].resample("D").mean().reindex(index_target)

    # Thời tiết
    if "Pressure" in df.columns:
        exog_df["pressure"] = df["Pressure"].resample("D").mean().reindex(index_target)

    if "Temperature" in df.columns:
        exog_df["temperature"] = df["Temperature"].resample("D").mean().reindex(index_target)

    if "Wind Speed" in df.columns:
        exog_df["wind_speed"] = df["Wind Speed"].resample("D").mean().reindex(index_target)

    if "Relative Humidity" in df.columns:
        exog_df["humidity"] = df["Relative Humidity"].resample("D").median().reindex(index_target)

    if "Precipitation" in df.columns:
        # Tổng lượng mưa
        exog_df["rain"] = df["Precipitation"].resample("D").sum().reindex(index_target)
        # Giờ mưa
        rain_flag = (df["Precipitation"] > 0).astype(int)
        exog_df["rain_hours"] = rain_flag.resample("D").sum().reindex(index_target)

    # Dịp lễ và cuối tuần 
    exog_df["IsWeekend"] = exog_df.index.dayofweek.isin([5, 6]).astype(int)

    exog_df["IsHoliday"] = 0
    for _, (start, end) in holiday_periods.items():
        start_dt = pd.to_datetime(start)
        end_dt = pd.to_datetime(end)
        mask = (exog_df.index >= start_dt) & (exog_df.index <= end_dt)
        exog_df.loc[mask, "IsHoliday"] = 1

    exog_df["IsHoliday_lag1"] = exog_df["IsHoliday"].shift(1).fillna(0)
    exog_df["IsHoliday_lag2"] = exog_df["IsHoliday"].shift(2).fillna(0)

    # Lấy độ trễ (lag-1)
    lag_base_cols = [
        c for c in exog_df.columns
        if c not in ["IsWeekend", "IsHoliday", "IsHoliday_lag1", "IsHoliday_lag2"]
    ]

    for c in lag_base_cols:
        exog_df[f"{c}_lag1"] = exog_df[c].shift(1)
    exog_df = exog_df.replace([np.inf, -np.inf], np.nan)
    exog_df = exog_df.bfill().ffill()
    exog_df = exog_df.asfreq("D")
    return exog_df

# 4. Chuẩn hóa dữ liệu 

def scale_exog(exog_df: pd.DataFrame, scaler, exog_cols) -> pd.DataFrame:
    exog_sel = exog_df[exog_cols].copy()
    exog_scaled = pd.DataFrame(
        scaler.transform(exog_sel),
        index=exog_sel.index,
        columns=exog_cols,
    )
    return exog_scaled


# 5. Log dữ liệu và chuyển ngược lại 
def transform_series(s: pd.Series, method="log"):
    s = pd.Series(s).astype(float)
    if method == "log":
        return np.log(s + 1e-6), None
    raise ValueError("Transform method không hợp lệ")


def inv_transform(s_t: pd.Series, method="log", lam=None):
    s_t = pd.Series(s_t).astype(float)
    if method == "log":
        return np.exp(s_t) - 1e-6
    raise ValueError("Transform method không hợp lệ")
