# src/utils_pm25.py
import numpy as np
import pandas as pd
from scipy import stats


# ==========================
# 1. Biến ngày nghỉ / ngày lễ
# ==========================
# 👉 HÃY COPY nguyên dict holiday_periods trong notebook của bạn vào đây.
holiday_periods = {
    'Tết 2023': ('2023-01-20', '2023-01-26'),
    'Lễ 30/4-1/5 2023': ('2023-04-29', '2023-05-03'),
    'Lễ 2/9 2023': ('2023-08-31', '2023-09-04'),
    'Tết Dương 2024': ('2023-12-30', '2024-01-01'),
    'Tết 2024': ('2024-02-08', '2024-02-14'),
    'Lễ 30/4-1/5 2024': ('2024-04-27', '2024-05-01'),
    'Lễ 2/9 2024': ('2024-08-31', '2024-09-03'),
}


# ==========================
# 2. Hàm xử lý PM2.5 theo ngày
# ==========================
def build_pm25_daily(df: pd.DataFrame) -> pd.Series:
    """
    Đầu vào: df raw có cột 'Local Time' và 'PM25'
    Đầu ra:  Series PM25 resample theo ngày (mean), freq='D'
    """
    df = df.copy()
    df["Local Time"] = pd.to_datetime(df["Local Time"])
    df = df.set_index("Local Time").sort_index()

    pm25 = df["PM25"].resample("D").mean()
    pm25 = pm25.asfreq("D")
    return pm25


# ==========================
# 3. Hàm build exog daily
# ==========================
def build_exog_daily(df: pd.DataFrame, index_target: pd.DatetimeIndex) -> pd.DataFrame:
    """
    Xây exog theo ngày, align với index_target (index của PM25).
    Tạo các cột:
      - PM10, NO2, SO2 (daily mean)
      - pressure (daily mean)
      - IsWeekend, IsHoliday
      - pressure_lag1, PM10_lag1
      - IsHoliday_lag1, IsHoliday_lag2
    Sau đó bạn sẽ chọn đúng exog_cols từ config.
    """
    df = df.copy()
    df["Local Time"] = pd.to_datetime(df["Local Time"])
    df = df.set_index("Local Time").sort_index()

    exog_df = pd.DataFrame(index=index_target)  # index ngày

    # 3.1 Pollutant: mean theo ngày
    if "PM10" in df.columns:
        exog_df["PM10"] = df["PM10"].resample("D").mean().reindex(exog_df.index)
    if "NO2" in df.columns:
        exog_df["NO2"] = df["NO2"].resample("D").mean().reindex(exog_df.index)
    if "SO2" in df.columns:
        exog_df["SO2"] = df["SO2"].resample("D").mean().reindex(exog_df.index)

    # 3.2 Thời tiết: pressure (nếu cần thêm temperature, wind... thì bổ sung ở đây)
    if "Pressure" in df.columns:
        exog_df["pressure"] = df["Pressure"].resample("D").mean().reindex(exog_df.index)

    # 3.3 Cờ weekend / holiday
    exog_df["IsWeekend"] = exog_df.index.dayofweek.isin([5, 6]).astype(int)

    exog_df["IsHoliday"] = 0
    for _, (start, end) in holiday_periods.items():
        start_dt = pd.to_datetime(start)
        end_dt = pd.to_datetime(end)
        mask = (exog_df.index >= start_dt) & (exog_df.index <= end_dt)
        exog_df.loc[mask, "IsHoliday"] = 1

    # 3.4 Lag cho pressure và PM10
    if "pressure" in exog_df.columns:
        exog_df["pressure_lag1"] = exog_df["pressure"].shift(1)
    if "PM10" in exog_df.columns:
        exog_df["PM10_lag1"] = exog_df["PM10"].shift(1)

    # 3.5 Lag cho holiday
    exog_df["IsHoliday_lag1"] = exog_df["IsHoliday"].shift(1).fillna(0)
    exog_df["IsHoliday_lag2"] = exog_df["IsHoliday"].shift(2).fillna(0)

    # 3.6 Dọn missing
    exog_df = exog_df.replace([np.inf, -np.inf], np.nan)
    exog_df = exog_df.fillna(method="bfill").fillna(method="ffill")

    # đảm bảo freq là D
    exog_df = exog_df.asfreq("D")

    return exog_df


# ==========================
# 4. Scale exog theo scaler đã fit
# ==========================
def scale_exog(exog_df: pd.DataFrame, scaler, exog_cols) -> pd.DataFrame:
    """
    Lấy đúng các cột exog_cols, scale bằng scaler (StandardScaler đã fit khi train).
    """
    exog_sel = exog_df[exog_cols].copy()
    exog_scaled = pd.DataFrame(
        scaler.transform(exog_sel),
        index=exog_sel.index,
        columns=exog_cols,
    )
    return exog_scaled


# ==========================
# 5. Transform / Inverse-transform
# ==========================
def transform_series(s: pd.Series, method="identity"):
    """
    Giống hệt hàm bạn dùng khi train: 'identity' / 'log' / 'boxcox'
    Trả về: series đã transform, cùng index; và lambda (cho boxcox)
    """
    s = pd.Series(s).astype(float)

    if method == "identity":
        return s.copy(), None

    if method == "log":
        return np.log(s + 1e-6), None

    if method == "boxcox":
        y = s + 1e-6
        y_bc, lam = stats.boxcox(y)
        return pd.Series(y_bc, index=s.index), lam

    raise ValueError("Transform method không hợp lệ")


def inv_transform(s_t: pd.Series, method="identity", lam=None):
    """
    Inverse transform ngược lại về thang gốc.
    """
    s_t = pd.Series(s_t).astype(float)

    if method == "identity":
        return s_t

    if method == "log":
        return np.exp(s_t) - 1e-6

    if method == "boxcox":
        # xử lý đặc biệt khi lam=0
        if lam is None:
            raise ValueError("Cần lam để inverse BoxCox")
        if lam == 0:
            return np.exp(s_t) - 1e-6
        return np.power(s_t * lam + 1, 1 / lam) - 1e-6

    raise ValueError("Transform method không hợp lệ")
