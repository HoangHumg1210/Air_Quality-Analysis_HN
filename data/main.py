import os, time, requests, pandas as pd
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from dotenv import load_dotenv, find_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ====== LOAD API KEY ======
dotenv_path = find_dotenv()
load_dotenv(dotenv_path, override=True)
API_KEY = os.getenv("API_KEY")
if not API_KEY:
    raise RuntimeError("Không tìm thấy API_KEY trong .env")

# ====== CONFIG ======
CITY = "Hanoi"
COUNTRY = "VN"
TZ_NAME = "Asia/Ho_Chi_Minh"
TZ = ZoneInfo(TZ_NAME)

# Tuỳ ý mở rộng thêm quận
DISTRICTS = {
    "Ba Dinh": (21.0338, 105.8142),
    "Hoan Kiem": (21.0285, 105.8542),
    "Tay Ho": (21.0680, 105.8220),
    "Cau Giay": (21.0362, 105.7906),
    "Dong Da": (21.0185, 105.8290),
    "Hai Ba Trung": (21.0064, 105.8602),
    "Hoang Mai": (20.9711, 105.8580),
    "Thanh Xuan": (20.9945, 105.8120),
    "Long Bien": (21.0500, 105.8890),
    "Bac Tu Liem": (21.0601, 105.7495),
    "Nam Tu Liem": (21.0106, 105.7646),
    "Ha Dong": (20.9593, 105.7655),
}

AIR_HIST_URL = "https://api.openweathermap.org/data/2.5/air_pollution/history"
OPEN_METEO_ARCHIVE = "https://archive-api.open-meteo.com/v1/era5"

# ====== HTTP SESSION with RETRY ======
def make_session():
    retry = Retry(
        total=5,                 # thử tối đa 5 lần
        backoff_factor=0.8,      # 0.8s, 1.6s, 2.4s, ...
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    s = requests.Session()
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://",  HTTPAdapter(max_retries=retry))
    return s

SESSION = make_session()

def call(url, params, timeout=20):
    r = SESSION.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()

# ====== AQI US/EPA BREAKPOINTS ======
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

def aqi_from_bp(c, table):
    if c is None:
        return None
    x = float(c)
    for C_lo, C_hi, I_lo, I_hi in table:
        if C_lo <= x <= C_hi:
            return (I_hi - I_lo) / (C_hi - C_lo) * (x - C_lo) + I_lo
    return None

def aqi_us_from_pm(pm25, pm10):
    aqi_pm25 = aqi_from_bp(pm25, PM25_BP)
    aqi_pm10 = aqi_from_bp(pm10, PM10_BP)
    vals = [v for v in (aqi_pm25, aqi_pm10) if v is not None]
    return max(vals) if vals else None

# ====== UNIT HELPERS ======
def co_ugm3_to_mgm3(x):
    return None if x is None else x / 1000.0  # µg/m3 -> mg/m3

# ====== OPENWEATHER: HOURLY AIR (THEO NGÀY) ======
def get_air_history(lat, lon, day_utc):
    start = int(datetime(day_utc.year, day_utc.month, day_utc.day, tzinfo=timezone.utc).timestamp())
    end   = start + 24*3600 - 1
    return call(AIR_HIST_URL, {"lat": lat, "lon": lon, "start": start, "end": end, "appid": API_KEY})

# ====== OPEN-METEO ARCHIVE (ERA5): HOURLY WEATHER ======
def get_weather_hourly_map(lat, lon, start_dt, end_dt):
    """
    Trả về dict: unix_ts(UTC) -> {Temperature, RH, Clouds, Pressure, Precipitation,  Wind Speed}
    Chia theo từng năm để tránh response quá lớn.
    """
    weather_map = {}
    year_start = start_dt.year
    year_end   = end_dt.year
    for year in range(year_start, year_end + 1):
        s = datetime(year, 1, 1, tzinfo=timezone.utc)
        e = datetime(year, 12, 31, tzinfo=timezone.utc)
        if s < start_dt: s = start_dt
        if e > end_dt:   e = end_dt

        params = {
            "latitude": lat,
            "longitude": lon,
            "hourly": ",".join([
                "temperature_2m",
                "relative_humidity_2m",
                "cloudcover",
                "pressure_msl",
                "precipitation",
                "uv_index",
                "wind_speed_10m",
            ]),
            "timezone": "UTC",  # để khớp dt_unix từ OWM (UTC)
            "start_date": s.strftime("%Y-%m-%d"),
            "end_date":   e.strftime("%Y-%m-%d"),
        }

        h = call(OPEN_METEO_ARCHIVE, params, timeout=60)
        hourly = h.get("hourly", {})
        times = hourly.get("time", [])
        for i, t in enumerate(times):
            dt_utc = datetime.fromisoformat(t).replace(tzinfo=timezone.utc)
            ts = int(dt_utc.timestamp())
            weather_map[ts] = {
                "Temperature":        (hourly.get("temperature_2m") or [None])[i],       # °C
                "Relative Humidity":  (hourly.get("relative_humidity_2m") or [None])[i], # %
                "Clouds":             (hourly.get("cloudcover") or [None])[i],          # %
                "Pressure":           (hourly.get("pressure_msl") or [None])[i],        # hPa
                "Precipitation":      (hourly.get("precipitation") or [None])[i],       # mm

                "Wind Speed":         (hourly.get("wind_speed_10m") or [None])[i],      # m/s
            }
        # Giãn nhịp xíu để lịch sự với server
        time.sleep(0.3)
    return weather_map

# ====== MAIN RUNNER (with checkpoint & graceful stop) ======
def run_one_district(name, lat, lon, start_utc, end_utc):
    print(f"\n=== {name} ({lat},{lon}) ===")
    all_rows = []

    # lấy weather hourly (UTC) cho toàn giai đoạn
    wx_map = get_weather_hourly_map(lat, lon, start_utc, end_utc)

    cur_day = start_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    end_day = end_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    total_days = (end_day - cur_day).days

    chkpt_path = f"__chkpt_{name.replace(' ', '_')}.csv"

    try:
        for d in range(total_days):
            day_utc = cur_day + timedelta(days=d)
            air = get_air_history(lat, lon, day_utc)
            cnt = 0

            for item in air.get("list", []):
                dt_unix   = int(item.get("dt", 0))                    # UTC seconds
                utc_time  = datetime.fromtimestamp(dt_unix, tz=timezone.utc)
                local_time = utc_time.astimezone(TZ)

                comp  = item.get("components", {})
                pm25  = comp.get("pm2_5")      # µg/m3
                pm10  = comp.get("pm10")       # µg/m3
                co_mg = co_ugm3_to_mgm3(comp.get("co"))  # mg/m3
                aqi_us = aqi_us_from_pm(pm25, pm10)

                w = wx_map.get(dt_unix, {})  # weather theo giờ matching timestamp

                row = {
                    "District": name,
                    "Local Time": local_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "UTC Time": utc_time.isoformat(),
                    "City": CITY,
                    "Country Code": COUNTRY,
                    "Timezone": TZ_NAME,

                    # Air Quality (units)
                    "AQI": aqi_us,                          # US/EPA 0–500
                    "CO": co_mg,                            # mg/m3
                    "NO2": comp.get("no2"),                 # µg/m3
                    "O3":  comp.get("o3"),                  # µg/m3
                    "PM10": pm10,                           # µg/m3
                    "PM25": pm25,                           # µg/m3
                    "SO2": comp.get("so2"),                 # µg/m3

                    # Weather hourly (Open-Meteo ERA5)
                    "Clouds":            w.get("Clouds"),            # %
                    "Precipitation":     w.get("Precipitation"),     # mm
                    "Pressure":          w.get("Pressure"),          # hPa
                    "Relative Humidity": w.get("Relative Humidity"), # %
                    "Temperature":       w.get("Temperature"),       # °C
                    "Wind Speed":        w.get("Wind Speed"),        # m/s
                }
                all_rows.append(row)
                cnt += 1

            print(f"{day_utc.date()} -> {cnt} giờ")
            # checkpoint mỗi 7 ngày
            if (d + 1) % 7 == 0:
                pd.DataFrame(all_rows).to_csv(chkpt_path, index=False, encoding="utf-8")
                print(f"[CHKPT] Saved {chkpt_path} ({d+1}/{total_days} ngày)")
            # giãn nhịp giữa các ngày để tránh rate limit
            time.sleep(0.25)

    except KeyboardInterrupt:
        ck = f"__interrupted_{name.replace(' ', '_')}.csv"
        pd.DataFrame(all_rows).to_csv(ck, index=False, encoding="utf-8")
        print(f"\n[BREAK] Bạn dừng bằng Ctrl+C. Đã lưu tạm vào {ck}")
        raise
    except Exception as e:
        print(f"{day_utc.date()} lỗi: {e}")

    return pd.DataFrame(all_rows)

if __name__ == "__main__":
    # Khoảng thời gian: 00:00 01/01/2021 → 00:00 01/01/2024 (UTC)
    start_utc = datetime(2022, 1, 1, 0, 0, tzinfo=timezone.utc)
    end_utc   = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)

    outputs = []
    for dname, (lat, lon) in DISTRICTS.items():
        df = run_one_district(dname, lat, lon, start_utc, end_utc)
        df.to_csv(f"{dname.replace(' ', '_')}.csv", index=False, encoding="utf-8")
        outputs.append(df)

    final_df = pd.concat(outputs, ignore_index=True)
    final_df.to_csv("air_quality_all_districts.csv", index=False, encoding="utf-8")
    print("\nHoàn tất. File đã lưu: air_quality_all_districts.csv")

