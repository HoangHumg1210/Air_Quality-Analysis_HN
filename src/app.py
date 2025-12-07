from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import streamlit as st
from sklearn.metrics import mean_absolute_error, mean_squared_error

# ==================== PAGE CONFIG ====================

st.set_page_config(
    page_title="Dashboard Chất lượng Không khí - Hà Nội",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ==================== CONSTANTS ====================

WHO_PM25_THRESHOLD = 35.4  # µg/m³

AQI_LEVELS = [
    {"range": (0, 50), "label": "Tốt", "color": "#66cc66", "bg": "#d4edda", 
     "advice": "Chất lượng không khí tốt. Hoạt động ngoài trời bình thường."},
    {"range": (51, 100), "label": "Trung bình", "color": "#eec900", "bg": "#fff8e1",
     "advice": "Chất lượng chấp nhận được. Nhóm nhạy cảm nên hạn chế hoạt động ngoài trời kéo dài."},
    {"range": (101, 150), "label": "Không tốt cho nhóm nhạy cảm", "color": "#FF7F24", "bg": "#ffe5d0",
     "advice": "Trẻ em, người già, người có bệnh hô hấp nên hạn chế ra ngoài."},
    {"range": (151, 200), "label": "Kém", "color": "#CD2626", "bg": "#f8d7da",
     "advice": "Mọi người có thể bị ảnh hưởng. Hạn chế hoạt động ngoài trời."},
    {"range": (201, 300), "label": "Rất kém", "color": "#CD2626", "bg": "#f8d7da",
     "advice": "Cảnh báo sức khỏe. Tránh hoạt động ngoài trời, đeo khẩu trang."},
    {"range": (301, 500), "label": "Nguy hiểm", "color": "#b03060", "bg": "#d4a5a5",
     "advice": "Nguy hiểm! Ở trong nhà, đóng cửa sổ, sử dụng máy lọc không khí."},
]

AQI_BREAKPOINTS = [
    (0.0, 12.0, 0, 50),
    (12.1, 35.4, 51, 100),
    (35.5, 55.4, 101, 150),
    (55.5, 150.4, 151, 200),
    (150.5, 250.4, 201, 300),
    (250.5, 500.0, 301, 500),
]


# ==================== HELPER FUNCTIONS ====================

def pm25_to_aqi(pm25: float) -> tuple[int, str, dict]:
    """Chuyển đổi PM2.5 sang (AQI, label, level_info)."""
    value = max(0.0, min(float(pm25), 500.0))
    
    for c_lo, c_hi, i_lo, i_hi in AQI_BREAKPOINTS:
        if c_lo <= value <= c_hi:
            aqi = int(round((i_hi - i_lo) / (c_hi - c_lo) * (value - c_lo) + i_lo))
            for level in AQI_LEVELS:
                if level["range"][0] <= aqi <= level["range"][1]:
                    return aqi, level["label"], level
            break
    
    return 0, "N/A", AQI_LEVELS[0]


def compute_metrics(df: pd.DataFrame) -> dict:
    """Tính các chỉ số đánh giá mô hình."""
    y_true, y_pred = df["actual"], df["predicted"]
    
    mae = mean_absolute_error(y_true, y_pred)
    rmse = np.sqrt(mean_squared_error(y_true, y_pred))
    
    ss_res = np.sum((y_true - y_pred) ** 2)
    ss_tot = np.sum((y_true - y_true.mean()) ** 2)
    r2 = 1 - (ss_res / ss_tot) if ss_tot != 0 else np.nan
    
    mape = np.mean(np.abs((y_true - y_pred) / np.clip(y_true, 1e-6, None))) * 100
    
    return {
        "mae": mae, "rmse": rmse, "r2": r2, "mape": mape,
        "avg_actual": y_true.mean(), "avg_pred": y_pred.mean(),
        "max": y_true.max(), "min": y_true.min(),
        "count": len(df),
    }


def add_aqi_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Thêm cột AQI vào DataFrame."""
    df = df.copy()
    df["AQI_actual"] = df["actual"].apply(lambda v: pm25_to_aqi(v)[0])
    df["AQI_pred"] = df["predicted"].apply(lambda v: pm25_to_aqi(v)[0])
    return df


# ==================== DATA LOADING ====================

@st.cache_data
def load_data() -> pd.DataFrame | None:
    """Load kết quả rolling forecast."""
    path = Path(__file__).resolve().parent.parent / "models" / "weekly_eval_results.csv"
    
    if not path.exists():
        return None
    
    df = pd.read_csv(path)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    if "train_end" in df.columns:
        df["train_end"] = pd.to_datetime(df["train_end"], errors="coerce")
    
    return df.sort_values("date")


# ==================== CHART COMPONENTS ====================

def create_aqi_gauge(pm25_value: float, title: str) -> go.Figure:
    """Tạo gauge chart AQI."""
    aqi, label, level = pm25_to_aqi(pm25_value)
    
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=aqi,
        title={"text": f"<b>{title}</b><br><span style='font-size:16px;color:{level['color']}'>{label}</span>"},
        gauge={
            "axis": {"range": [0, 500], "tickwidth": 2, "tickcolor": "gray"},
            "bar": {"color": level["color"], "thickness": 0.3},
            "bgcolor": "white",
            "borderwidth": 2,
            "bordercolor": "gray",
            "steps": [
                {"range": [0, 50], "color": "#66cc66"},
                {"range": [50, 100], "color": "#eec900"},
                {"range": [100, 150], "color": "#FF7F24"},
                {"range": [150, 200], "color": "#f8d7da"},
                {"range": [200, 300], "color": "#CD2626"},
                {"range": [300, 500], "color": "#b03060"}, 
            ],
            "threshold": {
                "line": {"color": level["color"], "width": 6},
                "thickness": 0.8,
                "value": aqi,
            },
        },
    ))
    
    fig.update_layout(
        height=280,
        margin=dict(l=30, r=30, t=80, b=30),
        paper_bgcolor="rgba(0,0,0,0)",
    )
    return fig


def create_pm25_chart(df: pd.DataFrame, title: str) -> go.Figure:
    """Biểu đồ PM2.5 theo thời gian."""
    fig = go.Figure()
    
    # Area fill for actual
    fig.add_trace(go.Scatter(
        x=df["date"], y=df["actual"],
        mode="lines",
        name="Thực tế",
        line=dict(color="#2e7d32", width=2),
        fill="tozeroy",
        fillcolor="rgba(46, 125, 50, 0.1)",
    ))
    
    # Predicted line
    fig.add_trace(go.Scatter(
        x=df["date"], y=df["predicted"],
        mode="lines+markers",
        name="Dự báo SARIMAX",
        line=dict(color="#ef6c00", width=2, dash="dot"),
        marker=dict(size=4),
    ))
    
    # WHO threshold
    fig.add_hline(y=WHO_PM25_THRESHOLD, line_dash="dash", line_color="red",
                  annotation_text="Ngưỡng WHO (35.4)", annotation_position="top left")
    
    fig.update_layout(
        title=dict(text=title, font=dict(size=16)),
        xaxis_title="Ngày",
        yaxis_title="PM2.5 (µg/m³)",
        hovermode="x unified",
        template="plotly_white",
        height=400,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0.5, xanchor="center"),
    )
    return fig


def create_aqi_chart(df: pd.DataFrame) -> go.Figure:
    """Biểu đồ AQI theo thời gian với vùng màu."""
    df_aqi = add_aqi_columns(df)
    
    fig = go.Figure()
    
    # Background zones
    for level in AQI_LEVELS[:4]:  # Only show first 4 levels
        fig.add_hrect(
            y0=level["range"][0], y1=level["range"][1],
            fillcolor=level["color"], opacity=0.1, line_width=0,
        )
    
    fig.add_trace(go.Scatter(
        x=df_aqi["date"], y=df_aqi["AQI_actual"],
        mode="lines+markers",
        name="AQI Thực tế",
        line=dict(color="#2e7d32", width=2),
        marker=dict(size=4),
    ))
    
    fig.add_trace(go.Scatter(
        x=df_aqi["date"], y=df_aqi["AQI_pred"],
        mode="lines+markers",
        name="AQI Dự báo",
        line=dict(color="#ef6c00", width=2, dash="dot"),
        marker=dict(size=4),
    ))
    
    fig.update_layout(
        title="Chỉ số AQI theo thời gian",
        xaxis_title="Ngày",
        yaxis_title="AQI",
        yaxis=dict(range=[0, max(200, df_aqi["AQI_actual"].max() + 20)]),
        hovermode="x unified",
        template="plotly_white",
        height=400,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0.5, xanchor="center"),
    )
    return fig



def create_error_histogram(df: pd.DataFrame) -> go.Figure:
    """Histogram phân phối sai số."""
    error = df["predicted"] - df["actual"]
    
    fig = go.Figure()
    fig.add_trace(go.Histogram(
        x=error,
        nbinsx=30,
        marker_color="#5c6bc0",
        opacity=0.8,
    ))
    
    fig.add_vline(x=0, line_dash="dash", line_color="red", line_width=2)
    fig.add_vline(x=error.mean(), line_dash="dot", line_color="green",
                  annotation_text=f"Mean: {error.mean():.1f}")
    
    fig.update_layout(
        title="Phân phối sai số dự báo",
        xaxis_title="Sai số (µg/m³)",
        yaxis_title="Tần suất",
        template="plotly_white",
        height=350,
    )
    return fig




# ==================== MAIN APP ====================

def main():
    # Header
    st.markdown("""
    <h1 style='text-align: center; color: #1e3a5f;'>
         Dự báo chất lượng không khí
    </h1>
    <p style='text-align: center; color: #666; margin-bottom: 30px;'>
        Hà Nội | Mô hình SARIMAX với Rolling Forecast
    </p>
    """, unsafe_allow_html=True)
    
    # Load data
    df_all = load_data()
    if df_all is None or df_all.empty:
        st.error(" Chưa có dữ liệu. Chạy: `python train_sarimax.py`")
        st.stop()
    
    # ========== SIDEBAR ==========
    with st.sidebar:
        st.header(" Chọn ngày")
        
        min_date = df_all["date"].min().date()
        max_date = df_all["date"].max().date()
        
        selected_date = st.date_input(
            "Chọn ngày",
            value=max_date,
            min_value=min_date,
            max_value=max_date,
        )
        
        # Date range for charts
        st.markdown("**Khoảng thời gian hiển thị biểu đồ:**")
        days_range = st.slider(
            "Số ngày trước/sau ngày được chọn",
            min_value=0,
            max_value=30,
            value=7,
            help="Chọn 0 để chỉ xem toàn bộ dữ liệu"
        )
        
        
        
        st.divider()
        st.subheader(" Chọn chế độ xem")
        view_option = st.radio(
            "Hiển thị:",
            ["PM2.5 theo thời gian", "AQI theo thời gian", "Hiệu suất mô hình", "Bảng dữ liệu"],
            index=0
        )
        
        st.divider()
        
        # Collapsible model info
        with st.expander(" Thông tin", expanded=False):
            st.markdown("""
            - **Mô hình:** SARIMAX
            - **Order:** (1, 1, 2)
            - **Seasonal:** (0, 0, 1, 7)
            - **Train:** 2023-2024
            - **Test:** 2025
            """)
        # Collapsible AQI scale
        with st.expander(" Thang AQI", expanded=False):
            for level in AQI_LEVELS:
                st.markdown(
                    f"<div style='background:{level['bg']}; padding:5px 10px; border-radius:5px; margin:3px 0;'>"
                    f"<span style='color:{level['color']}'>●</span> "
                    f"<b>{level['range'][0]}-{level['range'][1]}</b>: {level['label']}</div>",
                    unsafe_allow_html=True
                )
    
    # Filter data for selected date
    df_selected = df_all[df_all["date"].dt.date == selected_date]
    
    if df_selected.empty:
        st.warning(f"Không có dữ liệu cho ngày {selected_date}")
        st.stop()
    
    # Get data for selected date
    row = df_selected.iloc[0]
    pm25_actual = row["actual"]
    pm25_pred = row["predicted"]
    
    aqi_actual, label_actual, level_actual = pm25_to_aqi(pm25_actual)
    aqi_pred, label_pred, level_pred = pm25_to_aqi(pm25_pred)
    
    # Filter data for charts based on date range
    if days_range > 0:
        from datetime import timedelta
        start_date = selected_date - timedelta(days=days_range)
        end_date = selected_date + timedelta(days=days_range)
        mask = (df_all["date"].dt.date >= start_date) & (df_all["date"].dt.date <= end_date)
        df = df_all[mask].copy()
        date_range_text = f"{start_date.strftime('%d/%m/%Y')} - {end_date.strftime('%d/%m/%Y')}"
    else:
        df = df_all.copy()
        date_range_text = "Toàn bộ dữ liệu"
    
    # Compute metrics for filtered data
    metrics = compute_metrics(df)
    
    # ========== ROW 1: AQI GAUGE + HEALTH ADVICE ==========
    st.subheader(" Tổng quan Chất lượng Không khí")
    
    col1, col2 = st.columns([1, 1.5])
    
    with col1:
        st.plotly_chart(create_aqi_gauge(pm25_pred, f"AQI Dự báo - {selected_date.strftime('%d/%m/%Y')}"), use_container_width=True)
    
    
    with col2:
        st.markdown(f"""
        <div style='background:{level_pred["bg"]}; padding: 20px; border-radius: 10px; 
                    border-left: 5px solid {level_pred["color"]}; height: 100%;'>
            <h4 style='margin-top:0; color:{level_pred["color"]}'> Khuyến nghị</h4>
            <p style='font-size: 15px; margin-bottom: 15px;'>{level_pred["advice"]}</p>
            <hr>
            <div style='display:flex; justify-content:space-between;'>
                <div><b>PM2.5 Thực tế:</b> {pm25_actual:.1f} µg/m³</div>
                <div><b>PM2.5 Dự báo:</b> {pm25_pred:.1f} µg/m³</div>
                <div><b>Sai số:</b> {abs(pm25_pred - pm25_actual):.1f} µg/m³</div>
            </div>
            <div style='margin-top:10px;'>
                <b>AQI Thực tế:</b> {aqi_actual} ({label_actual}) | <b>AQI Dự báo:</b> {aqi_pred} ({label_pred})
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    st.divider()
    
    # ========== DISPLAY SELECTED VIEW ==========
    
    if view_option == "PM2.5 theo thời gian":
        st.plotly_chart(create_pm25_chart(df, f"PM2.5 theo thời gian ({date_range_text})"), use_container_width=True)
    
    elif view_option == "AQI theo thời gian":
        st.plotly_chart(create_aqi_chart(df), use_container_width=True)
    
    elif view_option == "Hiệu suất mô hình":
        st.markdown("### Các chỉ số đánh giá mô hình")
        
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("MAE", f"{metrics['mae']:.2f} µg/m³", 
                    help="Mean Absolute Error - Sai số tuyệt đối trung bình")
        col2.metric("RMSE", f"{metrics['rmse']:.2f} µg/m³",
                    help="Root Mean Square Error - Căn bậc hai sai số bình phương trung bình")
        col3.metric("R²", f"{metrics['r2']:.3f}" if not np.isnan(metrics['r2']) else "N/A",
                    help="Hệ số xác định - càng gần 1 càng tốt")
        col4.metric("MAPE", f"{metrics['mape']:.1f}%",
                    help="Mean Absolute Percentage Error - Sai số phần trăm trung bình")
    
    else:  # Bảng dữ liệu
        df_display = add_aqi_columns(df)
        df_display["error"] = (df_display["predicted"] - df_display["actual"]).round(2)
        df_display["date_str"] = df_display["date"].dt.strftime("%Y-%m-%d")
        
        st.dataframe(
            df_display[["date_str", "actual", "predicted", "error", "AQI_actual", "AQI_pred"]].rename(columns={
                "date_str": "Ngày",
                "actual": "PM2.5 Thực tế",
                "predicted": "PM2.5 Dự báo",
                "error": "Sai số",
                "AQI_actual": "AQI Thực tế",
                "AQI_pred": "AQI Dự báo",
            }),
            use_container_width=True,
            height=400,
        )
    
    st.divider()
    st.caption(f" Dữ liệu từ {min_date} đến {max_date} | Cập nhật: {datetime.now().strftime('%d/%m/%Y %H:%M')}")


if __name__ == "__main__":
    main()
