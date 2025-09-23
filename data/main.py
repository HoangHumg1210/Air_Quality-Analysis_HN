import requests
import pandas as pd
import os
from dotenv import load_dotenv
from datetime import  datetime, timedelta



# load biến môi trường
load_dotenv('.env')

API_KEY = os.getenv('API_KEY')
lat = float(os.getenv('lat'))
lon = float(os.getenv('lon'))

start = int((datetime.now() - timedelta(days = 2*365+1)).timestamp())
end = int(datetime.now().timestamp())

url = "http://api.openweathermap.org/data/2.5/air_pollution/history"
data_list = []

print("Bắt đầu crawl Data Pollution...")
while start < end:
    next_end_time = min(start + 24*3600, end)
    params = {
        'lat': lat,
        'lon': lon,
        'start': start,
        'end': next_end_time,
        'appid': API_KEY
    }

    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            for item in data['list']:
                timestamp = datetime.fromtimestamp(item['dt']).strftime('%Y-%m-%d %H:%M:%S')
                aqi = item['main']['aqi']
                components = item['components']
                row = {
                    'Time': timestamp,
                    'AQI': aqi, #Trong đó 1 = Tốt, 2 = Trung bình, 3 = Trung bình, 4 = Kém, 5 = Rất kém.
                    'PM2.5': components['pm2_5'],
                    'PM10': components['pm10'],
                    'CO': components['co'],
                    'NO2': components['no2'],
                    'O3': components['o3'],
                    'SO2': components['so2'],
                    'NH3': components['nh3']
                }
                data_list.append(row)
            print(f"Xong ngày {datetime.fromtimestamp(start).date()}")
        else:
            print(f'Lỗi: Không thể lấy dữ liệu. Cụ thể {response.status_code} ngay {datetime.fromtimestamp(start).date()}')
            break
    except Exception as e:
        print(f'Lỗi: {e}')
        break
    start = next_end_time + 1


df = pd.DataFrame(data_list)
print("Đã crawl xong!")
print(f'Tổng số dòng crawl đào đc: {len(df)}')
# --- ĐỔI TÊN AQI 1–5 CỦA OWM ---
df.rename(columns={'AQI': 'AQI_cat_owm'}, inplace=True)

# --- HÀM TÍNH AQI TỪ BREAKPOINT ---
def aqi_from_breakpoints(c, table):
    try:
        x = float(c)
    except (TypeError, ValueError):
        return None
    for C_lo, C_hi, I_lo, I_hi in table:
        if C_lo <= x <= C_hi:
            return (I_hi - I_lo) / (C_hi - C_lo) * (x - C_lo) + I_lo
    return None

# --- BẢNG BREAKPOINT EPA ---
PM25_BP = [
    (0.0, 12.0, 0, 50),
    (12.1, 35.4, 51, 100),
    (35.5, 55.4, 101, 150),
    (55.5, 150.4, 151, 200),
    (150.5, 250.4, 201, 300),
    (250.5, 350.4, 301, 400),
    (350.5, 500.4, 401, 500),
]
PM10_BP = [
    (0, 54, 0, 50),
    (55, 154, 51, 100),
    (155, 254, 101, 150),
    (255, 354, 151, 200),
    (355, 424, 201, 300),
    (425, 504, 301, 400),
    (505, 604, 401, 500),
]

# --- TÍNH AQI ---
df['AQI_PM25'] = df['PM2.5'].apply(lambda v: aqi_from_breakpoints(v, PM25_BP))
df['AQI_PM10'] = df['PM10'].apply(lambda v: aqi_from_breakpoints(v, PM10_BP))
df['AQI_calc'] = df[['AQI_PM25', 'AQI_PM10']].max(axis=1)

# --- NHÃN MỨC AQI ---
def aqi_label(aqi):
    if aqi is None: return None
    if aqi <= 50: return 'Good'
    if aqi <= 100: return 'Moderate'
    if aqi <= 150: return 'Unhealthy for SG'
    if aqi <= 200: return 'Unhealthy'
    if aqi <= 300: return 'Very Unhealthy'
    return 'Hazardous'

df['AQI_level'] = df['AQI_calc'].apply(aqi_label)

print("Đã tính xong AQI chuẩn. Ví dụ:")
print(df[['Time', 'PM2.5', 'PM10', 'AQI_PM25', 'AQI_PM10', 'AQI_calc', 'AQI_level']].head())
print("Bắt đầu crawl Data Weather...")

url_weather = f"https://power.larc.nasa.gov/api/temporal/daily/point?parameters=T2M,WS2M,RH2M,PRECTOTCORR&community=AG&longitude={lon}&latitude={lat}&start=2018&end=2023&format=JSON"

r = requests.get(url_weather)
weather_list = []

if r.status_code == 200:
    data = r.json()
    parameters = data['properties']['parameter']
    dates = parameters['T2M'].keys()

    for date in dates:
        weather_list.append({
            'Date': pd.to_datetime(date),
            'Temp': parameters['T2M'][date], # nhiệt độ
            'Humidity': parameters['RH2M'][date], # độ ẩm
            'Wind_speed': parameters['WS2M'][date], # tốc độ gió
            'Precipitation': parameters['PRECTOTCORR'][date]  # lượng mưa
        })
    print(f"Xong , tổng {len(weather_list)} ngày.")
else:
    print("Lỗi weather: ", r.status_code)

df_weather = pd.DataFrame(weather_list)
print("Đã crawl xong weather NASA POWER.")

print("Đang gộp dữ liệu...")


if not df.empty and 'Time' in df.columns:
    df['Time'] = pd.to_datetime(df['Time'])
    df['Date'] = df['Time'].dt.date
    df_weather['Date'] = df_weather['Date'].dt.date

    df_merge = pd.merge(df, df_weather, how='left', on='Date')
    output = 'data_crawl.csv'
    df_merge.to_csv(output, index=False)

    print("Gộp xong. File đã lưu:", output)
    print(df_merge.head())
else:
    print("DataFrame AQI rỗng hoặc không có cột 'Time'. Không thể gộp.")

