
import json
from pathlib import Path
import zipfile 

import joblib
import numpy as np
import pandas as pd
import streamlit as st
import plotly.graph_objects as go


from utils_pm25 import (
    build_pm25_daily,
    build_exog_daily,
    scale_exog,
    inv_transform,
)


def pm25_to_aqi(pm: float):
    BREAKPOINTS_PM25 = [
        (0.0,    9.0,    0,   50,  "Tốt"),
        (9.1,    35.4,   51,  100, "Trung bình"),
        (35.5,   55.4,   101, 150, "Không tốt cho nhóm nhạy cảm"),
        (55.5,   125.4,  151, 200, "Kém"),
        (125.5,  225.4,  201, 300, "Rất kém"),
        (225.5,  500.0,  301, 500, "Nguy hại"),
    ]

    C = float(pm)

    if C < BREAKPOINTS_PM25[0][0]:
        C = BREAKPOINTS_PM25[0][0]
    if C > BREAKPOINTS_PM25[-1][1]:
        C = BREAKPOINTS_PM25[-1][1]

    for C_lo, C_hi, I_lo, I_hi, cat in BREAKPOINTS_PM25:
        if C_lo <= C <= C_hi:
            aqi = (I_hi - I_lo) / (C_hi - C_lo) * (C - C_lo) + I_lo
            return cat, int(round(aqi))

    return "Không xác định", 0





# 1. Load model + scaler + config
@st.cache_resource
def load_model_and_cfg():
    base_dir = Path(__file__).resolve().parent
    model_dir = base_dir.parent / "models"

    
    scaler = joblib.load(model_dir / "exog_scaler.pkl")
    
    zipped = model_dir / "sarimax_pm25.zip"
    pkl_file = model_dir / "sarimax_pm25.pkl"

    # Nếu file .pkl chưa được giải nén -> giải nén nó
    if zipped.exists() and not pkl_file.exists():
        with zipfile.ZipFile(zipped, 'r') as zip_ref:
            zip_ref.extractall(model_dir)

    model = joblib.load(pkl_file)
    scaler = joblib.load(model_dir / "exog_scaler.pkl")

    with open(model_dir / "config_pm25.json", "r", encoding="utf-8") as f:
        cfg = json.load(f)

    return model, scaler, cfg



# 1.b Dữ liệu CSV mặc định

@st.cache_data
def load_default_csv():
    base_dir = Path(__file__).resolve().parent  # src/
    data_dir = base_dir.parent / "data"         # ../data
    df = pd.read_csv(data_dir / "data_2025.csv")
    return df


# 2. Cấu hình page

st.set_page_config(
    page_title="PM2.5 Forecast - SARIMAX",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# Ẩn header/menu/footer Streamlit cho giao diện “dashboard” sạch hơn
show_streamlit_style = """
<style>
#MainMenu {visibility: visible;}
footer {visibility: visible;}
header {visibility: visible;}
</style>
"""
st.markdown(show_streamlit_style, unsafe_allow_html=True)

model, scaler, cfg = load_model_and_cfg()


# SIDEBAR - chọn nguồn dữ liệu + info model

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
            help="File có schema giống dữ liệu train (có cột Local Time, PM25, ...)",
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
    data_source = "Dữ liệu mẫu có sẵn (data_2025.csv)"


# ==========================
# 4. Xử lý dữ liệu theo ngày
# ==========================
has_pm25 = "PM25" in df_new.columns

# Nếu không có PM25 thì vẫn cần Local Time để tạo index ngày
if not has_pm25 and "Local Time" not in df_new.columns:
    st.error(
        "File CSV cần có cột 'PM25' hoặc ít nhất cột 'Local Time' "
        "để có thể xây dựng chuỗi thời gian theo ngày."
    )
    st.stop()

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


# 5. Forecast với SARIMAX (dự báo tương lai)

train_idx = pd.to_datetime(model.model.data.row_labels)
last_train_date = train_idx[-1]

# Chỉ lấy exog các ngày sau khi train xong
exog_future = exog_scaled[exog_scaled.index > last_train_date]

if exog_future.empty:
    st.error(
        "Dữ liệu không có ngày nào sau thời điểm model được train, "
        "nên không có gì để dự báo."
    )
    st.stop()
else:
    max_horizon = min(200, len(exog_future))
    default_H = min(10, max_horizon)

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


    preds = inv_transform(
        preds_tf,
        method=cfg["transform"],
        lam=cfg["lam"],
    )
    lo = inv_transform(
        lo_tf,
        method=cfg["transform"],
        lam=cfg["lam"],
    )
    hi = inv_transform(
        hi_tf,
        method=cfg["transform"],
        lam=cfg["lam"],
    )

    preds.index = exog_future.index
    lo.index = exog_future.index
    hi.index = exog_future.index


# 6. Chuẩn bị đánh giá + AQI

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

# Chỉ số AQI tổng quan trên cả dải dự báo
aqi_series = df_forecast["AQI"]
aqi_mean = aqi_series.mean()
aqi_max = aqi_series.max()
cat_max, _ = pm25_to_aqi(aqi_max)


# 8. Biểu đồ
def plot_forecast_interactive(preds, lo, hi):
    fig = go.Figure()

    # CI trên
    fig.add_trace(go.Scatter(
        x=preds.index,
        y=hi,
        mode="lines",
        line=dict(width=0),
        showlegend=False,
        hoverinfo="skip",
        name="CI trên",
    ))

    # CI dưới + fill
    fig.add_trace(go.Scatter(
        x=preds.index,
        y=lo,
        mode="lines",
        line=dict(width=0),
        fill="tonexty",
        fillcolor="rgba(0, 100, 255, 0.1)",
        name="Khoảng tin cậy 95%",
        hoverinfo="skip",
    ))

    # Đường dự báo
    fig.add_trace(go.Scatter(
        x=preds.index,
        y=preds,
        mode="lines+markers",
        name="Dự báo PM2.5",
        line=dict(color="#1f77b4", width=3),
        marker=dict(size=5),
    ))

    # Vạch ngưỡng nhạy cảm
    fig.add_hline(
        y=35.4,
        line_dash="dot",
        line_color="orange",
        annotation_text="Ngưỡng nhạy cảm 35.4",
    )

    fig.update_layout(
        title="Dự báo nồng độ PM2.5 theo thời gian",
        xaxis_title="Ngày",
        yaxis_title="Nồng độ PM2.5 (µg/m³)",
        hovermode="x unified",
        template="plotly_white",
        height=420,
    )
    st.plotly_chart(fig, use_container_width=True)


def plot_compare_interactive(y_true, y_pred):
    if (y_true is None) or (y_pred is None):
        st.info("Không có dữ liệu PM2.5 thực tế để so sánh.")
        return

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=y_true.index,
        y=y_true,
        mode="lines",
        name="Thực tế",
        line=dict(color="green"),
    ))

    fig.add_trace(go.Scatter(
        x=y_pred.index,
        y=y_pred,
        mode="lines",
        name="Dự báo",
        line=dict(color="orange", dash="dash"),
    ))

    fig.update_layout(
        title="So sánh PM2.5: Thực tế vs Dự báo",
        xaxis_title="Ngày",
        yaxis_title="PM2.5 (µg/m³)",
        hovermode="x unified",
        template="plotly_white",
        height=420,
    )
    st.plotly_chart(fig, use_container_width=True)


def plot_aqi_gauge(aqi_val, cat_val):

    fig = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=aqi_val,
        domain={"x": [0, 1], "y": [0, 1]},
        title={"text": f"<b>AQI: {cat_val}</b>"},
        delta={
            "reference": 50,
            "increasing": {"color": "red"},
            "decreasing": {"color": "green"},
        },
        gauge={
            "axis": {"range": [0, 500]},
            "bar": {"color": "#333333"},
            "steps": [
                {"range": [0, 50], "color": "#7DD6C2"},
                {"range": [50, 100], "color": "#FFE98A"},
                {"range": [100, 150], "color": "#FFB870"},
                {"range": [150, 200], "color": "#FF6B6B"},
                {"range": [200, 300], "color": "#B67CDA"},
                {"range": [300, 500], "color": "#5A0E27"},
            ],
        },
    ))

    fig.update_layout(height=260, margin=dict(l=20, r=20, t=50, b=20))
    st.plotly_chart(fig, use_container_width=True)



# 9. Nội dung MAIN – dùng Tabs
tab_overview, tab_detail, tab_eval = st.tabs(
    ["Tổng quan", "Chi tiết theo ngày", "Đánh giá mô hình"]
)

# --- TAB 1: TỔNG QUAN ---
with tab_overview:
    st.subheader("Tổng quan và tìm ngày có chất lượng không khí tốt")

    with st.expander("Tìm kiếm theo mức AQI"):
        level_choices = [
            "Tốt",
            "Trung bình",
            "Không tốt cho nhóm nhạy cảm",
            "Kém",
            "Rất kém",
            "Nguy hại",
        ]

        level = st.multiselect(
            "Chọn mức AQI:",
            options=level_choices,
            default=["Tốt", "Trung bình"],
        )

        df_good = df_forecast[df_forecast["Mức"].isin(level)]
        if df_good.empty:
            st.warning("Không có ngày nào thỏa điều kiện lọc.")
        else:
            df_good_display = df_good.copy()
            df_good_display["Ngày"] = df_good_display.index.date
            df_good_display = df_good_display[["Ngày", "PM25_pred", "AQI", "Mức"]]

            st.write(f"Có {len(df_good_display)} ngày thỏa điều kiện.")
            st.dataframe(df_good_display, use_container_width=True)

    cols = st.columns(4)
    with cols[0]:
        st.metric("AQI trung bình", f"{aqi_mean:.0f}")
    with cols[1]:
        delta_max = aqi_max - aqi_mean
        st.metric(
            "AQI cao nhất",
            f"{aqi_max:.0f}",
            delta=f"{delta_max:+.0f}",
            help=f"Mức: {cat_max}",
        )
    with cols[2]:
        st.metric("PM2.5 trung bình", f"{preds.mean():.2f} µg/m³")

    plot_forecast_interactive(preds, lo, hi)

    with st.expander("Xem bảng dữ liệu dự báo"):
        st.dataframe(df_forecast, use_container_width=True)


# --- TAB 2: CHI TIẾT NGÀY ---
with tab_detail:
    st.subheader("Chi tiết theo ngày")

    c_sel1, c_sel2 = st.columns([1, 2])

    with c_sel1:
        selected_date = st.selectbox(
            "Chọn ngày dự báo",
            options=list(preds.index),
            format_func=lambda d: d.strftime("%Y-%m-%d"),
        )

        # Tính AQI cho ngày đã chọn
        pm_selected = preds.loc[selected_date]
        lo_selected = lo.loc[selected_date]
        hi_selected = hi.loc[selected_date]
        cat_sel, aqi_sel = pm25_to_aqi(float(pm_selected))

        plot_aqi_gauge(aqi_sel, cat_sel)

    with c_sel2:
        st.subheader(f"Dự báo chi tiết: {selected_date.strftime('%d/%m/%Y')}")
        tile1, tile2, tile3 = st.columns(3)
        tile1.metric("PM2.5 dự báo", f"{pm_selected:.2f}", help="µg/m³")
        tile2.metric("Cận dưới (95%)", f"{lo_selected:.2f}")
        tile3.metric("Cận trên (95%)", f"{hi_selected:.2f}")

        st.metric("AQI", f"{aqi_sel} ({cat_sel})")

        st.markdown("**Khuyến nghị:**")
        if cat_sel == "Tốt":
            st.success("Không khí trong lành. Bạn có thể thoải mái hoạt động ngoài trời.")
        elif cat_sel == "Trung bình":
            st.warning(
                "Chất lượng không khí chấp nhận được. Nhóm nhạy cảm nên hạn chế vận động mạnh ngoài trời."
            )
        elif cat_sel == "Không tốt cho nhóm nhạy cảm":
            st.warning(
                "Nhóm nhạy cảm (người già, trẻ nhỏ, người có bệnh hô hấp) nên hạn chế ra ngoài."
            )
        elif cat_sel == "Kém":
            st.error(
                "Không khí kém. Nên đeo khẩu trang đạt chuẩn và hạn chế ở ngoài trời quá lâu."
            )
        else:
            st.error(
                "Không khí rất kém/nguy hại. Nên ở trong nhà, đóng cửa và sử dụng lọc không khí nếu có."
            )


# --- TAB 3: ĐÁNH GIÁ MÔ HÌNH ---
with tab_eval:
    if has_pm25 and (mae is not None) and (rmse is not None):
        st.subheader("Đánh giá độ chính xác mô hình")
        m1, m2 = st.columns(2)
        m1.metric("MAE", f"{mae:.3f}")
        m2.metric("RMSE", f"{rmse:.3f}")
        plot_compare_interactive(y_true, y_pred)
    elif has_pm25:
        st.warning(
            "Khoảng thời gian dự báo chưa có dữ liệu PM2.5 thực tế để so sánh."
        )
    else:
        st.info(
            "Dữ liệu đầu vào không có cột PM25 thực tế nên không thể đánh giá sai số."
        )



