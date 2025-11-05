import warnings
warnings.simplefilter('ignore')

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from statsmodels.tools.sm_exceptions import ConvergenceWarning
from statsmodels.tsa.stattools import adfuller
from statsmodels.tsa.seasonal import STL, seasonal_decompose
from statsmodels.tsa.statespace.sarimax import SARIMAX
from statsmodels.stats.diagnostic import acorr_ljungbox
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf
warnings.simplefilter('ignore', ConvergenceWarning)

# StatsForecast (AutoARIMA)
from statsforecast import StatsForecast
from statsforecast.models import AutoARIMA
from statsforecast.arima import arima_string

# ======================
# 0) LOAD & PREPARE DATA
# ======================
def load_pm25_data(file_path, train_ratio=0.8):
    """Tải, tiền xử lý và chia dữ liệu PM2.5."""
    df = pd.read_csv(file_path)
    df['Local Time'] = pd.to_datetime(df['Local Time'])
    df = df.set_index('Local Time').sort_index()

    df_daily = df.resample('D').mean(numeric_only=True).asfreq('D').interpolate()

    y = df_daily['PM25']

    exog_vars = ['NO2', 'SO2', 'Pressure', 'Temperature', 'Wind Speed', 'Precipitation']
    X = df_daily[exog_vars].copy()

    result = seasonal_decompose(y, model='additive', period=7)
    result.plot()
    plt.suptitle('Phân rã chuỗi thời gian PM2.5 (mùa vụ tuần)')
    plt.show()

    cutoff = int(len(y) * train_ratio)
    y_train, y_test = y.iloc[:cutoff], y.iloc[cutoff:]
    X_train, X_test = X.iloc[:cutoff], X.iloc[cutoff:]

    print(f"Dữ liệu được chia: Train={len(y_train)} mẫu, Test={len(y_test)} mẫu")
    return df_daily, y_train, y_test, X_train, X_test


def get_forecast_kpis(test_series, forecast_series):
    """Tính toán các chỉ số KPI cho dự báo."""
    actual = test_series.astype(float)
    forecast = forecast_series.astype(float)

    mean_actual = actual.mean()
    bias = np.mean(forecast - actual)
    mae = np.mean(np.abs(forecast - actual))
    rmse = np.sqrt(np.mean((forecast - actual) ** 2))

    kpis = {
        'RMSE': rmse,
        'MAE': mae,
        'Bias%': (bias / mean_actual * 100) if mean_actual != 0 else np.nan,
    }
    return pd.DataFrame([kpis]).round(3)


def get_differencing_order_d(train_series):
    """Xác định bậc sai phân không mùa vụ (d) bằng kiểm định ADF."""
    for d in range(5):
        adf_p = adfuller(train_series.diff(d).dropna(), autolag='AIC')[1]
        if adf_p < 0.05:
            return d
    return 2  # Mặc định nếu không tìm thấy


def get_differencing_order_D(train_series, season_len=7):
    """Xác định bậc sai phân mùa vụ (D) bằng phân rã STL."""
    if len(train_series) < 2 * season_len:
        return 0
    strength = STL(train_series, period=season_len).fit().seasonal.std() / STL(train_series,
                                                                               period=season_len).fit().resid.std()
    return 1 if strength > 0.8 else 0


# ---- HÀM RIÊNG CHO TỪNG MÔ HÌNH ----

# 1. Hàm cho SARIMA (đơn biến)
def sarima_grid_search(train_series, d, D, season_len):
    """Tìm kiếm tham số tốt nhất cho mô hình SARIMA."""
    best_aicc, best_fit = np.inf, None
    best_order, best_seasonal_order = None, None

    for p in range(3):
        for q in range(3):
            for P in range(2):
                for Q in range(2):
                    try:
                        model = SARIMAX(
                            train_series,
                            order=(p, d, q),
                            seasonal_order=(P, D, Q, season_len)
                        ).fit(disp=False)
                        if model.aicc < best_aicc:
                            best_aicc = model.aicc
                            best_fit = model
                            best_order = (p, d, q)
                            best_seasonal_order = (P, D, Q, season_len)
                    except:
                        continue
    return best_fit, best_order, best_seasonal_order


# 2. Hàm cho SARIMAX (đa biến)
def sarimax_grid_search(train_series, d, D, season_len, exog_train):
    """Tìm kiếm tham số tốt nhất cho mô hình SARIMAX."""
    best_aicc, best_fit = np.inf, None
    best_order, best_seasonal_order = None, None

    for p in range(3):
        for q in range(3):
            for P in range(2):
                for Q in range(2):
                    try:
                        model = SARIMAX(
                            train_series,
                            exog=exog_train,  # Thêm biến ngoại sinh
                            order=(p, d, q),
                            seasonal_order=(P, D, Q, season_len)
                        ).fit(disp=False)
                        if model.aicc < best_aicc:
                            best_aicc = model.aicc
                            best_fit = model
                            best_order = (p, d, q)
                            best_seasonal_order = (P, D, Q, season_len)
                    except:
                        continue
    return best_fit, best_order, best_seasonal_order


def plot_forecast_with_ci(train_series, test_series, forecast_obj, model_name):
    """Vẽ biểu đồ dự báo với khoảng tin cậy."""
    forecast_df = forecast_obj.summary_frame()

    plt.figure(figsize=(15, 6))
    plt.plot(train_series.index, train_series.values, label='Train', color='blue', linewidth=1.5)
    plt.plot(test_series.index, test_series.values, label='Actual (Test)', color='green', linewidth=2)
    plt.plot(forecast_df.index, forecast_df['mean'], label='Forecast', color='red', linewidth=2, linestyle='--')

    # Vẽ khoảng tin cậy 95%
    plt.fill_between(forecast_df.index,
                     forecast_df['mean_ci_lower'],
                     forecast_df['mean_ci_upper'],
                     alpha=0.3, color='red', label='95% Confidence Interval')

    plt.axvline(x=train_series.index[-1], color='gray', linestyle='--', linewidth=1, alpha=0.7)
    plt.title(f'{model_name} - Dự báo PM2.5 với khoảng tin cậy 95%', fontsize=14, fontweight='bold')
    plt.xlabel('Thời gian', fontsize=12)
    plt.ylabel('PM2.5', fontsize=12)
    plt.legend(loc='best', fontsize=10)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.show()


def plot_all_forecasts_comparison(train_series, test_series, forecasts_dict):
    """Vẽ biểu đồ so sánh tất cả các mô hình dự báo."""
    plt.figure(figsize=(16, 8))

    # Vẽ dữ liệu train
    plt.plot(train_series.index, train_series.values,
             label='Train', color='blue', linewidth=1.5, alpha=0.7)

    # Vẽ dữ liệu test (actual)
    plt.plot(test_series.index, test_series.values,
             label='Actual (Test)', color='black', linewidth=2.5, marker='o', markersize=4)

    # Vẽ các dự báo
    colors = ['red', 'green', 'purple', 'orange']
    styles = ['--', '-.', ':', '--']

    for i, (model_name, forecast_values) in enumerate(forecasts_dict.items()):
        plt.plot(test_series.index, forecast_values,
                label=f'{model_name}',
                color=colors[i % len(colors)],
                linewidth=2,
                linestyle=styles[i % len(styles)],
                marker='s', markersize=3)

    plt.axvline(x=train_series.index[-1], color='gray', linestyle='--', linewidth=2, alpha=0.5)
    plt.title('So sánh các mô hình dự báo PM2.5', fontsize=16, fontweight='bold')
    plt.xlabel('Thời gian', fontsize=12)
    plt.ylabel('PM2.5', fontsize=12)
    plt.legend(loc='best', fontsize=11)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.show()


# Tải và chia dữ liệu
file_path = 'E:\Document\PROJECT_1\data\hanoi-aqi-weather-data.csv' # Thay đổi đường dẫn đến file của bạn
df_daily, y_train, y_test, X_train, X_test = load_pm25_data(file_path, train_ratio=0.8)

# Xác định các tham số cơ bản
season_len = 7
d = get_differencing_order_d(y_train)
D = get_differencing_order_D(y_train, season_len=season_len)

print(f"\nBậc sai phân được xác định: d={d}, D={D}, Chu kỳ mùa vụ={season_len}")


# --- MÔ HÌNH 1: SARIMA ---
print("\n--- Bắt đầu xây dựng mô hình SARIMA (đơn biến) ---")

# Tìm mô hình tốt nhất bằng Grid Search
sarima_fit, sarima_order, sarima_seasonal_order = sarima_grid_search(y_train, d, D, season_len)
print(f"SARIMA tốt nhất: order={sarima_order}, seasonal_order={sarima_seasonal_order}")

# Lấy dự báo cho tập test
sarima_forecast = sarima_fit.get_forecast(steps=len(y_test)).predicted_mean

# --- MÔ HÌNH 2: SARIMAX ---
print("\n--- Bắt đầu xây dựng mô hình SARIMAX (có biến ngoại sinh) ---")

# Tìm mô hình tốt nhất bằng Grid Search
sarimax_fit, sarimax_order, sarimax_seasonal_order = sarimax_grid_search(y_train, d, D, season_len, exog_train=X_train)
print(f"SARIMAX tốt nhất: order={sarimax_order}, seasonal_order={sarimax_seasonal_order}")

# Lấy dự báo cho tập test (phải cung cấp các biến ngoại sinh của tập test)
sarimax_forecast = sarimax_fit.get_forecast(steps=len(y_test), exog=X_test).predicted_mean

# --- MÔ HÌNH 3: AutoARIMAX ---
print("\n--- Bắt đầu xây dựng mô hình AutoARIMAX (tự động) ---")

# Chuẩn bị DataFrame cho StatsForecast
train_df_sf = y_train.reset_index().rename(columns={"Local Time": "ds", "PM25": "y"})
train_df_sf['unique_id'] = 'pm25_series'
train_df_sf = pd.merge(train_df_sf, X_train.reset_index().rename(columns={"Local Time": "ds"}), on='ds')

X_test_sf = X_test.reset_index().rename(columns={"Local Time": "ds"})
X_test_sf['unique_id'] = 'pm25_series'

# Huấn luyện và dự báo
exog_vars = list(X_train.columns)
sf_model = StatsForecast(models=[AutoARIMA(season_length=season_len)], freq='D')
sf_model.fit(train_df_sf, X_df=train_df_sf[exog_vars])
autoarimax_forecast_sf = sf_model.predict(h=len(y_test), X_df=X_test_sf[exog_vars])

# Chuyển kết quả về dạng Series để so sánh
autoarimax_forecast = pd.Series(autoarimax_forecast_sf['AutoARIMA'].values, index=y_test.index)

print(f"AutoARIMAX đã chọn mô hình: {sf_model.models[0]}")


# --- SO SÁNH KẾT QUẢ ---
print("\n" + "="*60)
print("--- SO SÁNH KẾT QUẢ CÁC MÔ HÌNH ---")
print("="*60)

# 1. Vẽ biểu đồ so sánh tất cả các mô hình
forecasts_dict = {
    'SARIMA': sarima_forecast.values,
    'SARIMAX': sarimax_forecast.values,
    'AutoARIMAX': autoarimax_forecast.values
}
plot_all_forecasts_comparison(y_train, y_test, forecasts_dict)

# 2. Tạo DataFrame tổng hợp
results_df = pd.DataFrame({
    'Thực tế': y_test,
    'SARIMA': sarima_forecast,
    'SARIMAX': sarimax_forecast,
    'AutoARIMAX': autoarimax_forecast
})

# 3. Vẽ biểu đồ so sánh dạng bảng
results_df.plot(figsize=(15, 8), style=['-', '--', '--', '-.'],
                title='So sánh kết quả dự báo PM2.5 của các mô hình',
                linewidth=1.5)
plt.legend()
plt.grid(True, alpha=0.4)
plt.show()

# 4. Tính toán và in bảng KPI
kpi_sarima = get_forecast_kpis(y_test, sarima_forecast)
kpi_sarimax = get_forecast_kpis(y_test, sarimax_forecast)
kpi_autoarimax = get_forecast_kpis(y_test, autoarimax_forecast)

kpi_sarima['Model'] = 'SARIMA (đơn biến)'
kpi_sarimax['Model'] = 'SARIMAX (đa biến)'
kpi_autoarimax['Model'] = 'AutoARIMAX (tự động)'

kpi_summary = pd.concat([kpi_sarima, kpi_sarimax, kpi_autoarimax]).set_index('Model')

print("\n--- Bảng so sánh KPI của các mô hình ---")
print(kpi_summary[['RMSE', 'MAE', 'Bias%']])

# 5. Kết luận
best_model = kpi_summary['RMSE'].idxmin()
print(f"\n{'='*60}")
print(f"=> KẾT LUẬN: Mô hình '{best_model}' cho kết quả tốt nhất")
print(f"   RMSE: {kpi_summary.loc[best_model, 'RMSE']:.3f}")
print(f"   MAE: {kpi_summary.loc[best_model, 'MAE']:.3f}")
print(f"   Bias%: {kpi_summary.loc[best_model, 'Bias%']:.3f}%")
print(f"{'='*60}")
