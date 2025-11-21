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


model, scaler, cfg = load_model_and_cfg()

st.set_page_config(page_title="PM2.5 Forecast - SARIMAX", layout="wide")
st.title("Dự báo PM2.5 bằng mô hình SARIMAX")

st.write("Mô hình đã chọn:", cfg["order"], "x", cfg["seasonal_order"])
st.write("Biến exog sử dụng:", ", ".join(cfg["exog_cols"]))


# ==========================
# 2. Upload dữ liệu test
# ==========================
uploaded = st.file_uploader("Upload file dữ liệu (CSV, giống schema train)", type=["csv"])

if uploaded is not None:
    df_new = pd.read_csv(uploaded)
    st.subheader("5 dòng đầu của dữ liệu upload")
    st.dataframe(df_new.head())

    # 2.1 PM2.5 daily (nếu có cột PM25)
    has_pm25 = "PM25" in df_new.columns
    if has_pm25:
        pm25_daily = build_pm25_daily(df_new)
        st.write("Khoảng thời gian PM2.5:", pm25_daily.index.min(), "→", pm25_daily.index.max())
    else:
        # Nếu không có PM25, vẫn cần index ngày (dùng Local Time)
        df_new["Local Time"] = pd.to_datetime(df_new["Local Time"])
        idx_daily = df_new.set_index("Local Time").sort_index().resample("D").mean().index
        pm25_daily = pd.Series(index=idx_daily, dtype=float)

    # 2.2 Xây exog theo ngày, align với index PM25
    exog_daily = build_exog_daily(df_new, pm25_daily.index)

    # 2.3 Lấy đúng các cột exog_cols từ config
    exog_cols = cfg["exog_cols"]
    missing_cols = [c for c in exog_cols if c not in exog_daily.columns]
    if missing_cols:
        st.error(f"Thiếu các cột exog trong exog_daily: {missing_cols}")
    else:
        exog_scaled = scale_exog(exog_daily, scaler, exog_cols)

        # ==========================
        # 3. Forecast với SARIMAX
        # ==========================
        steps = len(exog_scaled)
        fc = model.get_forecast(steps=steps, exog=exog_scaled)

        preds_tf = fc.predicted_mean
        ci = fc.conf_int(alpha=0.05)
        lo_tf = ci.iloc[:, 0]
        hi_tf = ci.iloc[:, 1]

        # 3.1 Inverse transform về thang PM2.5 gốc
        preds = inv_transform(preds_tf, method=cfg["transform"], lam=cfg["lam"])
        lo = inv_transform(lo_tf, method=cfg["transform"], lam=cfg["lam"])
        hi = inv_transform(hi_tf, method=cfg["transform"], lam=cfg["lam"])

        # ==========================
        # 4. Vẽ biểu đồ
        # ==========================
        st.subheader("Biểu đồ dự báo PM2.5 trên dữ liệu upload")

        fig, ax = plt.subplots(figsize=(12, 4))
        if has_pm25:
            ax.plot(pm25_daily.index, pm25_daily, label="PM2.5 thực tế", alpha=0.6)
        ax.plot(preds.index, preds, label="Dự báo SARIMAX", color="C3")
        ax.fill_between(preds.index, lo, hi, color="C3", alpha=0.15, label="95% CI")
        ax.set_xlabel("Ngày")
        ax.set_ylabel("PM2.5 (µg/m³)")
        ax.legend()
        ax.set_title("Dự báo PM2.5")
        st.pyplot(fig)

        # ==========================
        # 5. Tính MAE / RMSE nếu có PM25 thật
        # ==========================
        if has_pm25:
            from sklearn.metrics import mean_absolute_error, mean_squared_error

            y_true = pm25_daily
            y_pred = preds.reindex(y_true.index)

            mask = ~(y_true.isna() | y_pred.isna())
            mae = mean_absolute_error(y_true[mask], y_pred[mask])
            rmse = np.sqrt(mean_squared_error(y_true[mask], y_pred[mask]))

            st.subheader("Đánh giá trên dữ liệu này")
            st.write(f"MAE  : {mae:.3f}")
            st.write(f"RMSE : {rmse:.3f}")
