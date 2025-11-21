# src/app.py
import json
from pathlib import Path

import joblib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import streamlit as st

from utils_pm25 import (
    build_pm25_daily,
    build_exog_daily,
    scale_exog,
    inv_transform,
)

# ==========================
# 0. Hàm chuyển PM2.5 -> AQI + mức (nội suy)
# ==========================
def pm25_to_aqi(pm: float):
    # (C_lo, C_hi, I_lo, I_hi, category)
    BREAKPOINTS_PM25 = [
        (0.0,    9.0,    0,   50,  "Tốt"),
        (9.1,    35.4,   51,  100, "Trung bình"),
        (35.5,   55.4,   101, 150, "Không tốt cho nhóm nhạy cảm"),
        (55.5,   125.4,  151, 200, "Kém"),
        (125.5,  225.4,  201, 300, "Rất kém"),
        (225.5,  500.0,  301, 500, "Nguy hại"),
    ]

    C = float(pm)

    # Kẹp C trong khoảng cho phép
    if C < BREAKPOINTS_PM25[0][0]:
        C = BREAKPOINTS_PM25[0][0]
    if C > BREAKPOINTS_PM25[-1][1]:
        C = BREAKPOINTS_PM25[-1][1]

    for C_lo, C_hi, I_lo, I_hi, cat in BREAKPOINTS_PM25:
        if C_lo <= C <= C_hi:
            aqi = (I_hi - I_lo) / (C_hi - C_lo) * (C - C_lo) + I_lo
            return cat, int(round(aqi))

    # Fallback nếu có gì đó lạ
    return "Không xác định", 0


# ==========================
# 1. Load model + scaler + config
# ==========================
@st.cache_resource
def load_model_and_cfg():
    base_dir = Path(__file__).resolve().parent  # src/
    model_dir = base_dir.parent / "models"      # ../models

    model = joblib.load(model_dir / "sarimax_pm25.pkl")
    scaler = joblib.load(model_dir / "exog_scaler.pkl")

    with open(model_dir / "config_pm25.json", "r", encoding="utf-8") as f:
        cfg = json.load(f)

    return model, scaler, cfg


# ==========================
# 1.b Dữ liệu CSV mặc định
# ==========================
@st.cache_data
def load_default_csv():
    """
    Đọc file CSV mặc định để dùng khi user không upload gì.
    Đặt file ở: ../data/data_test.csv (so với src/app.py)
    """
    base_dir = Path(__file__).resolve().parent  # src/
    data_dir = base_dir.parent / "data"         # ../data
    df = pd.read_csv(data_dir / "data_test.csv")
    return df


# ==========================
# 2. Cấu hình page
# ==========================
st.set_page_config(
    page_title="PM2.5 Forecast - SARIMAX",
    layout="wide"
)

model, scaler, cfg = load_model_and_cfg()

# ==========================
# SIDEBAR - chọn nguồn dữ liệu + info model
# ==========================
with st.sidebar:
    st.subheader("Nguồn dữ liệu")

    input_mode = st.radio(
        "Chọn nguồn dữ liệu",
        ["Dữ liệu mẫu có sẵn", "Upload file CSV"],
    )

    uploaded = None
    if input_mode == "Upload file CSV":
        uploaded = st.file_uploader(
            "Upload dữ liệu (CSV)",
            type=["csv"],
            help="File có schema giống dữ liệu train (có cột Local Time, PM25, ...)"
        )

    with st.expander("Thông tin mô hình"):
        st.write(f"**ARIMA order**: `{cfg['order']}`")
        st.write(f"**Seasonal order**: `{cfg['seasonal_order']}`")
        st.write("**Biến exog sử dụng:**")
        st.code(", ".join(cfg["exog_cols"]), language="markdown")

st.title("Dự báo chất lượng không khí bằng mô hình SARIMAX")

# ==========================
# 3. Lấy dữ liệu đầu vào (default hoặc upload)
# ==========================
if input_mode == "Upload file CSV":
    if uploaded is None:
        st.warning("Hãy upload file CSV hoặc chọn 'Dữ liệu mẫu có sẵn'.")
        st.stop()
    df_new = pd.read_csv(uploaded)
    data_source = f"File upload: {uploaded.name}"
else:
    df_new = load_default_csv()
    data_source = "Dữ liệu mẫu có sẵn (data_test.csv)"

st.caption(f"Đang dùng {data_source}")

# ==========================
# 4. Xử lý dữ liệu theo ngày
# ==========================
has_pm25 = "PM25" in df_new.columns
if has_pm25:
    pm25_daily = build_pm25_daily(df_new)
else:
    df_new["Local Time"] = pd.to_datetime(df_new["Local Time"])
    idx_daily = (
        df_new
        .set_index("Local Time")
        .sort_index()
        .resample("D")
        .mean()
        .index
    )
    pm25_daily = pd.Series(index=idx_daily, dtype=float)

# 4.1 Xây exog theo ngày, align với index PM25
exog_daily = build_exog_daily(df_new, pm25_daily.index)

# 4.2 Lấy đúng các cột exog_cols từ config
exog_cols = cfg["exog_cols"]
missing_cols = [c for c in exog_cols if c not in exog_daily.columns]
if missing_cols:
    st.error(f"Thiếu các cột exog trong exog_daily: {missing_cols}")
    st.stop()
else:
    exog_scaled = scale_exog(exog_daily, scaler, exog_cols)

# ==========================
# 5. Forecast với SARIMAX (dự báo tương lai)
# ==========================
train_idx = pd.to_datetime(model.model.data.row_labels)
last_train_date = train_idx[-1]
st.write("Model được train tới ngày:", last_train_date.date())

# Chỉ lấy exog các ngày sau khi train xong
exog_future = exog_scaled[exog_scaled.index > last_train_date]

if exog_future.empty:
    st.error(
        "Dữ liệu không có ngày nào sau thời điểm model được train, "
        "nên không có gì để dự báo."
    )
    st.stop()
else:
    max_horizon = min(90, len(exog_future))
    default_H = min(30, max_horizon)

    # Slider trong sidebar: thiết lập forecast horizon
    with st.sidebar:
        st.subheader("Thiết lập dự báo")
        H = st.slider(
            "Số ngày dự báo",
            min_value=1,
            max_value=max_horizon,
            value=default_H,
        )

    exog_future = exog_future.iloc[:H]

    fc = model.get_forecast(steps=len(exog_future), exog=exog_future)

    preds_tf = fc.predicted_mean
    ci = fc.conf_int(alpha=0.05)
    lo_tf = ci.iloc[:, 0]
    hi_tf = ci.iloc[:, 1]

    # Inverse transform
    preds = inv_transform(
        preds_tf,
        method=cfg["transform"],
        lam=cfg["lam"]
    )
    lo = inv_transform(
        lo_tf,
        method=cfg["transform"],
        lam=cfg["lam"]
    )
    hi = inv_transform(
        hi_tf,
        method=cfg["transform"],
        lam=cfg["lam"]
    )

    preds.index = exog_future.index
    lo.index = exog_future.index
    hi.index = exog_future.index

# ==========================
# 6. Chuẩn bị đánh giá + AQI
# ==========================
mae = None
rmse = None
y_true = None
y_pred = None

if has_pm25:
    from sklearn.metrics import mean_absolute_error, mean_squared_error

    y_true = pm25_daily.reindex(exog_future.index)
    y_pred = preds.reindex(exog_future.index)
    mask = ~(y_true.isna() | y_pred.isna())
    if mask.sum() > 0:
        mae = mean_absolute_error(y_true[mask], y_pred[mask])
        rmse = np.sqrt(mean_squared_error(y_true[mask], y_pred[mask]))

df_forecast = pd.DataFrame({"PM25_pred": preds})
cats = []
aqis = []
for v in df_forecast["PM25_pred"]:
    cat, aqi = pm25_to_aqi(float(v))
    cats.append(cat)
    aqis.append(aqi)
df_forecast["AQI"] = aqis
df_forecast["Mức"] = cats

# Chọn 1 ngày bất kỳ để xem chi tiết
with st.sidebar:
    st.subheader("Xem dự báo theo ngày")
    selected_date = st.selectbox(
        "Chọn ngày dự báo",
        options=list(preds.index),
        format_func=lambda d: d.strftime("%Y-%m-%d")
    )

pm_selected = preds.loc[selected_date]
lo_selected = lo.loc[selected_date]
hi_selected = hi.loc[selected_date]
cat_sel, aqi_sel = pm25_to_aqi(float(pm_selected))

# Ngày đầu tiên dãy dự báo (giữ lại cho phần summary)
ngay_dau = preds.index[0]
pm_dau = preds.iloc[0]
cat_dau, aqi_dau = pm25_to_aqi(float(pm_dau))

# ==========================
# 7. Sidebar (phần sau forecast): MAE/RMSE + menu
# ==========================
with st.sidebar:
    # Hiển thị MAE/RMSE nếu có
    if mae is not None and rmse is not None:
        st.subheader("Đánh giá mô hình")
        c1, c2 = st.columns(2)
        c1.metric("MAE", f"{mae:.3f}")
        c2.metric("RMSE", f"{rmse:.3f}")

    # Menu chế độ hiển thị
    modes = ["Tổng quan"]
    if has_pm25:
        modes.append("Đánh giá mô hình")

    view_mode = st.radio("Chế độ hiển thị", modes)

# ==========================
# 8. Các hàm vẽ biểu đồ
# ==========================
def plot_forecast_main():
    fig, ax = plt.subplots(figsize=(12, 4))

    ax.plot(preds.index, preds, label="Dự báo SARIMAX")
    ax.fill_between(
        preds.index,
        lo,
        hi,
        alpha=0.15,
        label="95% CI",
    )

    ax.set_xlabel("Ngày")
    ax.set_ylabel("Nồng độ PM2.5 (µg/m³)")
    ax.set_title("Chỉ số PM2.5 dự báo")
    ax.legend()
    st.pyplot(fig)


def plot_compare():
    if has_pm25 and (y_true is not None):
        fig, ax = plt.subplots(figsize=(12, 4))
        ax.plot(y_true.index, y_true, label="Thực tế")
        ax.plot(preds.index, preds, label="Dự báo", linestyle="--")
        ax.set_xlabel("Ngày")
        ax.set_ylabel("PM2.5 (µg/m³)")
        ax.set_title("So sánh PM2.5 thực tế vs dự báo")
        ax.legend()
        st.pyplot(fig)
    else:
        st.info(
            "Không có cột PM25 thực tế trong dữ liệu đầu vào "
            "nên không thể vẽ biểu đồ so sánh."
        )


def plot_aqi_time():
    fig, ax = plt.subplots(figsize=(12, 4))
    ax.plot(df_forecast.index, df_forecast["AQI"])
    ax.set_xlabel("Ngày")
    ax.set_ylabel("AQI")
    ax.set_title("AQI dự báo theo thời gian")
    st.pyplot(fig)


# ==========================
# 9. Nội dung MAIN theo menu sidebar
# ==========================
if view_mode == "Tổng quan":
    st.subheader("Tổng quan dự báo")
    plot_forecast_main()
    plot_aqi_time()

    # Thông tin tổng quan + ngày được chọn
    st.markdown(
        f"- Model đang dự báo **{len(preds)} ngày**.\n"
        f"- Ngày đầu tiên dự báo: **{ngay_dau.date()}**, mức chất lượng không khí **{cat_dau}** (AQI ≈ {aqi_dau})."
    )

    st.subheader(f"Dự báo chi tiết cho ngày {selected_date.date()}")
    c1, c2, c3 = st.columns(3)
    c1.metric("PM2.5", f"{pm_selected:.2f} µg/m³")
    c2.metric("CI thấp", f"{lo_selected:.2f}")
    c3.metric("CI cao", f"{hi_selected:.2f}")
    st.metric("AQI", f"{aqi_sel} ({cat_sel})")

    st.dataframe(df_forecast)

elif view_mode == "Đánh giá mô hình":
    st.subheader("Đánh giá mô hình trên khoảng dự báo")
    if (mae is not None) and (rmse is not None):
        c1, c2 = st.columns(2)
        with c1:
            st.metric("MAE", f"{mae:.3f}")
        with c2:
            st.metric("RMSE", f"{rmse:.3f}")
    plot_compare()
