from pathlib import Path
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from sklearn.metrics import mean_absolute_error, mean_squared_error
from pm25_model import forecast_pm25_future, forecast_with_rolling_window, pm25_to_aqi


st.set_page_config(
    page_title="Dự báo chất lượng không khí ",
    layout="wide",
    initial_sidebar_state="expanded",
)

NATIONAL_PM25_THRESHOLD = 25  

AQI_LEVELS = [
    {"range": (0, 50), "label": "Tốt", "color": "#66cc66", "bg": "#d4edda",
     "advice": "Chất lượng không khí tốt. Hoạt động ngoài trời bình thường."},
    {"range": (51, 100), "label": "Trung bình", "color": "#eec900", "bg": "#fff8e1",
     "advice": "Chất lượng chấp nhận được. Nhóm nhạy cảm nên hạn chế hoạt động ngoài trời kéo dài."},
    {"range": (101, 150), "label": "Không tốt cho nhóm nhạy cảm", "color": "#FF7F24", "bg": "#ffe5d0",
     "advice": "Trẻ em, người già, người có bệnh hô hấp nên hạn chế ra ngoài."},
    {"range": (151, 200), "label": "Xấu", "color": "#CD2626", "bg": "#f8d7da",
     "advice": "Mọi người có thể bị ảnh hưởng. Hạn chế hoạt động ngoài trời."},
    {"range": (201, 300), "label": "Rất Xấu", "color": "#CD2626", "bg": "#f8d7da",
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


def pm25_to_aqi(pm25: float) -> tuple[int, str, dict]:
    value = max(0.0, min(float(pm25), 500.0))
    for c_lo, c_hi, i_lo, i_hi in AQI_BREAKPOINTS:
        if c_lo <= value <= c_hi:
            aqi = int(round((i_hi - i_lo) / (c_hi - c_lo) * (value - c_lo) + i_lo))
            for level in AQI_LEVELS:
                if level["range"][0] <= aqi <= level["range"][1]:
                    return aqi, level["label"], level
            break
    return 0, "N/A", AQI_LEVELS[0]

# 
def compute_metrics(df: pd.DataFrame) -> dict:
    y_true, y_pred = df["actual"], df["predicted"]
    mae = mean_absolute_error(y_true, y_pred)
    rmse = np.sqrt(mean_squared_error(y_true, y_pred))
    ss_res = np.sum((y_true - y_pred) ** 2)
    ss_tot = np.sum((y_true - y_true.mean()) ** 2)
    mape = np.mean(np.abs((y_true - y_pred) / np.clip(y_true, 1e-6, None))) * 100
    return {
        "mae": mae,
        "rmse": rmse,
        "mape": mape,
        "avg_actual": y_true.mean(),
        "avg_pred": y_pred.mean(),
        "max": y_true.max(),
        "min": y_true.min(),
        "count": len(df),
    }

def add_aqi_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["AQI_actual"] = df["actual"].apply(lambda v: pm25_to_aqi(v)[0])
    df["AQI_pred"] = df["predicted"].apply(lambda v: pm25_to_aqi(v)[0])
    return df

@st.cache_data
def load_actual_pm25_data() -> pd.DataFrame:
    root = Path(__file__).resolve().parent.parent
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
    

    df_daily = df_all.groupby(df_all["Local Time"].dt.date).agg({"PM25": "mean"}).reset_index()
    df_daily.columns = ["date", "actual_pm25"]
    df_daily["date"] = pd.to_datetime(df_daily["date"])
    
    return df_daily

@st.cache_data
def load_data() -> pd.DataFrame | None:
    try:
        df_actual = load_actual_pm25_data()
        df = pd.DataFrame({
            'date': df_actual['date'],
            'actual': df_actual['actual_pm25']
        })
        return df.sort_values("date")
    except Exception as e:
        st.error(f"Lỗi khi load dữ liệu: {e}")
        return None

# Biểu đồ

# Thanh gauge
def create_aqi_gauge(pm25_value: float, title: str) -> go.Figure:
    aqi, label, level = pm25_to_aqi(pm25_value)
    fig = go.Figure(
        go.Indicator(
            mode="gauge+number",
            value=aqi,
            title={"text": f"<b>{title}</b><br><span style='font-size:16px;color:{level['color']}'>{label}</span>"},
            gauge={
                "axis": {"range": [0, 500], "tickwidth": 3, "tickcolor": "gray","tickvals": [0, 50, 100, 150, 200, 300, 500], "ticktext": ["0", "50", "100", "150", "200", "300", "500"]},
                "bar": {"color": level["color"], "thickness": 0.3},
                "bgcolor": "white",
                "borderwidth": 2,
                "bordercolor": "gray",
                "steps": [
                    {"range": [0, 50], "color": "#00cc00"},
                    {"range": [50, 100], "color": "#ffff00"},
                    {"range": [100, 150], "color": "#ff6600"},
                    {"range": [150, 200], "color": "#ff0000"},
                    {"range": [200, 300], "color": "#990099"},
                    {"range": [300, 500], "color": "#660000"},
                ],
                "threshold": {
                    "line": {"color": '#222222', "width": 6},
                    "thickness": 0.8,
                    "value": aqi,
                },
            },
        )
    )
    fig.update_layout(height=280, margin=dict(l=30, r=30, t=80, b=30), paper_bgcolor="rgba(0,0,0,0)")
    return fig

def create_future_forecast_chart(df_forecast: pd.DataFrame, start_date_str: str) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=df_forecast["date"],
            y=df_forecast["AQI"],
            mode="lines+markers",
            name="AQI Dự báo",
            line=dict(color="#1976d2", width=3),
            marker=dict(size=10, symbol="diamond"),
            fill="tozeroy",
            fillcolor="rgba(25, 118, 210, 0.1)",
        )
    )
    
    # Ngưỡng chuẩn quốc gia PM25 = 25 (AQI = 78)
    fig.add_hline(
        y=78,
        line_dash="dash",
        line_color="orange",
        annotation_text="Ngưỡng AQI 78",
        annotation_position="top left",
    )
    
    for level in AQI_LEVELS:
        y0, y1 = level["range"]
        fig.add_hrect(
            y0=y0, 
            y1=y1, 
            fillcolor=level["color"], 
            opacity=0.08, 
            line_width=0
        )
    
    fig.update_layout(
        title=dict(text=f" Dự báo AQI cho 3 ngày tới (từ {start_date_str})", font=dict(size=16)),
        xaxis_title="Ngày",
        yaxis_title="Chỉ số AQI",
        yaxis=dict(range=[0, min(500, df_forecast["AQI"].max() * 1.2)]),
        hovermode="x unified",
        template="plotly_white",
        height=400,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0.5, xanchor="center"),
    )
    return fig

def create_comparison_chart(df_comparison: pd.DataFrame) -> go.Figure:

    fig = go.Figure()
    
    # Thực tế
    fig.add_trace(go.Scatter(
        x=df_comparison["date"],
        y=df_comparison["actual_pm25"],
        mode="lines+markers",
        name="Thực tế",
        line=dict(color="#2ecc71", width=3),
        marker=dict(size=10, symbol="circle"),
    ))
    
    # Dự báo
    fig.add_trace(go.Scatter(
        x=df_comparison["date"],
        y=df_comparison["PM25_forecast"],
        mode="lines+markers",
        name="Dự báo",
        line=dict(color="#3498db", width=3, dash="dash"),
        marker=dict(size=10, symbol="diamond"),
    ))
    
    # Ngưỡng PM2.5 chuẩn quốc gia
    fig.add_hline(
        y=NATIONAL_PM25_THRESHOLD,
        line_dash="dot",
        line_color="orange",
        annotation_text=f"Ngưỡng PM2.5 = {NATIONAL_PM25_THRESHOLD}",
        annotation_position="right",
    )
    
    fig.update_layout(
        title=dict(text=" So sánh Dự báo vs Thực tế", font=dict(size=16)),
        xaxis_title="Ngày",
        yaxis_title="PM2.5 (µg/m³)",
        hovermode="x unified",
        template="plotly_white",
        height=450,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="center",
            x=0.5,
            bgcolor="rgba(255,255,255,0.8)",
            bordercolor="gray",
            borderwidth=1
        ),
    )
    return fig

def display_accuracy_metrics(df_valid: pd.DataFrame):
    """Calculate and display accuracy metrics."""
    actual = df_valid["actual_pm25"]
    forecast = df_valid["PM25_forecast"]
    
    mae = np.mean(np.abs(actual - forecast))
    rmse = np.sqrt(np.mean((actual - forecast) ** 2))
    mape = np.mean(np.abs((actual - forecast) / actual)) * 100
    

    ss_res = np.sum((actual - forecast) ** 2)
    ss_tot = np.sum((actual - actual.mean()) ** 2)
    
    st.markdown("###  Độ chính xác mô hình")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "MAE (Sai số TB)",
            f"{mae:.2f} µg/m³",
            delta=None,
            help="Mean Absolute Error - Sai số tuyệt đối trung bình"
        )
    with col2:
        st.metric(
            "RMSE",
            f"{rmse:.2f} µg/m³",
            delta=None,
            help="Root Mean Square Error - Căn bậc hai của sai số bình phương trung bình"
        )
    with col3:
        st.metric(
            "MAPE",
            f"{mape:.1f}%",
            delta=None,
            help="Mean Absolute Percentage Error - Sai số phần trăm trung bình"
        )


def format_comparison_table(df_valid: pd.DataFrame) -> pd.DataFrame:
    """Format comparison table with error columns."""
    df_display = df_valid[["date", "actual_pm25", "PM25_forecast", "AQI", "AQI_label"]].copy()
    df_display["Sai số"] = df_display["PM25_forecast"] - df_display["actual_pm25"]
    df_display["Sai số %"] = (df_display["Sai số"] / df_display["actual_pm25"]) * 100
    df_display["date"] = pd.to_datetime(df_display["date"]).dt.strftime("%d/%m/%Y")
    
    # Round values
    df_display["actual_pm25"] = df_display["actual_pm25"].round(2)
    df_display["PM25_forecast"] = df_display["PM25_forecast"].round(2)
    df_display["Sai số"] = df_display["Sai số"].round(2)
    df_display["Sai số %"] = df_display["Sai số %"].round(1)
    
    df_display.columns = ["Ngày", "Thực tế (µg/m³)", "Dự báo (µg/m³)", "AQI", "Mức độ", "Sai số", "Sai số %"]
    return df_display


# ==================== MAIN APP ====================

def main():
    st.markdown("""
    <h1 style='text-align: center; color: #1e3a5f;'>
        Dự báo chất lượng không khí
    </h1>
    <p style='text-align: center; color: #666; margin-bottom: 30px;'>
        Hà Nội | Mô hình SARIMAX
    </p>
    """, unsafe_allow_html=True)

    # Load historical data for date range
    df_all = load_data()
    if df_all is None or df_all.empty:
        st.error("⚠️ Chưa có dữ liệu lịch sử. Vui lòng chạy `python train_sarimax.py`.")
        st.stop()

    # Sidebar: mode selection and controls
    with st.sidebar:
        st.markdown("### Chế độ hiển thị")
        forecast_mode = st.radio(
            "Chọn chế độ:",
            ["Dự báo tương lai", "Đánh giá độ chính xác"],
            index=0,
        )
        
        st.divider()
        
        min_date = df_all["date"].min().date()
        max_date = df_all["date"].max().date()
        
        if forecast_mode == "Dự báo tương lai":
            # Mode 1: Dự báo tương lai
            st.markdown("####  Cài đặt dự báo")
            horizon_days = st.selectbox(
                "Số ngày dự báo:",
                [5, 7],
                index=0,
            )
            forecast_start = max_date + timedelta(days=1)
            st.success(f" Dự báo từ **{forecast_start.strftime('%d/%m/%Y')}**")
            selected_date = None
        else:
            # Mode 2: Đánh giá độ chính xác
            st.markdown("#### Chọn ngày kiểm tra")
            
            # Restrict to November 2025 only (best model performance)
            min_date_2025 = datetime(2024, 1, 1).date()
            max_date_2025 = datetime(2025, 11, 30).date()
            max_selectable = max_date_2025 - timedelta(days=0)
            default_date = datetime(2025, 11, 15).date()  # Mid-November
            
            selected_date = st.date_input(
                "Ngày kiểm tra:",
                value=default_date,
                min_value=min_date_2025,
                max_value=max_selectable,
            )
            forecast_start = selected_date + timedelta(days=1)
            horizon_days = 5
            
            # Show actual training info
            train_start_date = datetime(2023, 1, 1).date()
            st.caption(f" Dự báo từ **{forecast_start.strftime('%d/%m/%Y')}**")
        
        st.divider()
        
        # Thông tin mô hình
        with st.expander("Thông tin Mô hình", expanded=False):
            st.markdown("#### Tham số SARIMAX")
            st.markdown("""
            - **Order (p,d,q)**: (1, 1, 2)
            - **Seasonal (P,D,Q,s)**: (0, 0, 1, 7)
            - **Transform**: Log
            """)
            
            st.markdown("#### Biến ngoại sinh (Exogenous)")
            st.markdown("""
            **Chất lượng không khí:**
            - PM10, NO₂, SO₂
            
            **Khí tượng:**
            - Áp suất, Nhiệt độ, Độ ẩm
            - Tốc độ gió, Lượng mưa
            
            **Biến trễ (Lag-1):**
            - PM10, Áp suất, Nhiệt độ, Độ ẩm
            """)
        
        st.divider()
        
        with st.expander(" Thang đo AQI", expanded=False):
            st.markdown("#### Chỉ số chất lượng không khí")
            for level in AQI_LEVELS:
                st.markdown(f"""
                <div style='background-color: {level['bg']}; padding: 10px; margin: 5px 0; border-radius: 5px; border-left: 4px solid {level['color']}'>
                    <strong style='color: {level['color']}'>{level['range'][0]}-{level['range'][1]}: {level['label']}</strong><br>
                    <small>{level['advice']}</small>
                </div>
                """, unsafe_allow_html=True)



    if forecast_mode == "Đánh giá độ chính xác":
        df_forecast = forecast_with_rolling_window(
            test_date=pd.Timestamp(forecast_start),
            horizon_days=horizon_days,
            window_months=6
    )
    else:
        df_forecast = forecast_pm25_future(start_date=forecast_start, horizon_days=horizon_days)
    
    if forecast_mode == "Dự báo tương lai":
        st.markdown(f"##  Dự báo {horizon_days} ngày tới")
        
        high_aqi_days = df_forecast[df_forecast["AQI"] > 100]
        max_aqi = df_forecast["AQI"].max()
        max_aqi_idx = df_forecast["AQI"].idxmax()
        max_aqi_date = df_forecast.loc[max_aqi_idx, "date"]
        max_pm25 = df_forecast.loc[max_aqi_idx, "PM25_forecast"]
        
        max_aqi_level = None
        for level in AQI_LEVELS:
            if level["range"][0] <= max_aqi <= level["range"][1]:
                max_aqi_level = level
                break
        if max_aqi_level is None:
            max_aqi_level = AQI_LEVELS[0]
        
        if not high_aqi_days.empty:

            col_gauge, col_text = st.columns([1, 2])
            
            with col_gauge:
                date_str = pd.to_datetime(max_aqi_date).strftime("%d/%m/%Y")
                fig_warning = create_aqi_gauge(max_pm25, f"AQI Dự báo - {date_str}")
                st.plotly_chart(fig_warning, use_container_width=True, key="gauge_warning")
                st.caption(f"⚠️ **Ngày tệ nhất:** {date_str}")
            
            with col_text:
                st.markdown(f"""
                <div style='background: linear-gradient(135deg, {max_aqi_level['color']}15 0%, {max_aqi_level['color']}05 100%); 
                            border-left: 6px solid {max_aqi_level['color']}; 
                            padding: 25px; 
                            border-radius: 10px; 
                            margin: 20px 0;
                            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
                            height: 100%;'>
                    <div style='display: flex; align-items: center; margin-bottom: 15px;'>
                        <span style='font-size: 48px; margin-right: 20px;'>⚠️</span>
                        <div>
                            <h2 style='color: {max_aqi_level['color']}; margin: 0; font-size: 28px;'>CẢNH BÁO CHẤT LƯỢNG KHÔNG KHÍ</h2>
                            <p style='margin: 5px 0 0 0; font-size: 18px; color: #555;'>
                                Có <strong style='color: {max_aqi_level['color']}; font-size: 22px;'>{len(high_aqi_days)} ngày</strong> có AQI > 100
                            </p>
                        </div>
                    </div>
                    <div style='background-color: white; padding: 15px; border-radius: 8px; margin-top: 15px;'>
                        <p style='margin: 0; font-size: 16px; line-height: 1.6;'>
                            <strong style='color: {max_aqi_level['color']}; font-size: 18px;'> Khuyến nghị:</strong><br>
                            {max_aqi_level['advice']}
                        </p>
                        <p style='margin: 10px 0 0 0; font-size: 15px; color: #666;'>
                            <strong> Biện pháp bảo vệ:</strong> Đeo khẩu trang khi ra ngoài, sử dụng máy lọc không khí trong nhà, 
                            hạn chế mở cửa sổ vào giờ cao điểm.
                        </p>
                    </div>
                </div>
                """, unsafe_allow_html=True)
        else:
            
            col_gauge, col_text = st.columns([1, 2])
            
            with col_gauge:
                first_pm25 = float(df_forecast.iloc[0]["PM25_forecast"])
                date_str = pd.to_datetime(forecast_start).strftime("%d/%m/%Y")
                fig_success = create_aqi_gauge(first_pm25, f"AQI Dự báo - {date_str}")
                st.plotly_chart(fig_success, use_container_width=True, key="gauge_success")
            
    
        st.divider()
        
        # Forecast Chart
        st.plotly_chart(
            create_future_forecast_chart(df_forecast, forecast_start.strftime('%d/%m/%Y')),
            use_container_width=True,
            key="forecast_chart"
        )
        
        # Forecast Table
        st.markdown("###  Chi tiết dự báo")
        df_display = df_forecast.copy()
        df_display["date"] = pd.to_datetime(df_display["date"]).dt.strftime("%d/%m/%Y")
        df_display = df_display[["date", "PM25_forecast", "AQI", "AQI_label"]]
        df_display.columns = ["Ngày", "PM2.5 (µg/m³)", "AQI", "Mức độ"]
        
        st.dataframe(
            df_display,
            use_container_width=True,
            hide_index=True,
        )
        
    # ==================== MODE 2: BACKTEST MODE ====================
    else:
        st.markdown(f"##  So sánh Dự báo vs Thực tế")
        st.caption(f"Dự báo từ ngày {forecast_start.strftime('%d/%m/%Y')}")
    

        df_actual = load_actual_pm25_data()

        df_comparison = df_forecast.merge(
            df_actual[["date", "actual_pm25"]], 
            on="date", 
            how="left"
        )
        
    
        df_valid = df_comparison[df_comparison["actual_pm25"].notna()].copy()
        
        if df_valid.empty:
            st.warning(" Không có dữ liệu thực tế để so sánh cho khoảng thời gian này. Vui lòng chọn ngày xa hơn trong quá khứ.")
            st.stop()
        
        # Display Metrics
        display_accuracy_metrics(df_valid)
        
        st.divider()
        
 
        st.plotly_chart(
            create_comparison_chart(df_valid),
            use_container_width=True,
            key="comparison_chart"
        )
        
        # Comparison Table
        st.markdown("###  Bảng so sánh chi tiết")
        df_table = format_comparison_table(df_valid)
        
        st.dataframe(
            df_table,
            use_container_width=True,
            hide_index=True,
        )
    

    st.divider()
    min_date = df_all["date"].min().date()
    max_date = df_all["date"].max().date()
    st.caption(f" Dữ liệu từ {min_date} đến {max_date} | Cập nhật: {datetime.now().strftime('%d/%m/%Y %H:%M')} ")

if __name__ == "__main__":
    main()
