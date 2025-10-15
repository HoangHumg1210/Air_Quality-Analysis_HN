import os, time, requests, pandas as pd
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo


API_KEY = os.getenv("API_KEY")
if not API_KEY:
    raise RuntimeError("Thiếu API_KEY")

CITY = "Hanoi"
COUNTRY = "VN"
TZ_NAME = "Asia/Ho_Chi_Minh"
TZ = ZoneInfo(TZ_NAME)

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



# ================== FETCH HELPERS ==================

def http_get(url, params, timeout=30):
    r = requests.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()

# OpenWeather: hourly air per day (UTC timestamps)

def get_air_history(lat, lon, day_utc):
    start = int(datetime(day_utc.year, day_utc.month, day_utc.day, tzinfo=timezone.utc).timestamp())
    end = start + 24 * 3600 - 1
    return http_get(AIR_HIST_URL, {"lat": lat, "lon": lon, "start": start, "end": end, "appid": API_KEY})

# Open‑Meteo ERA5: hourly weather (request by year to keep response small)

def get_weather_hourly_map(lat, lon, start_dt, end_dt):
    weather_map = {}
    for year in range(start_dt.year, end_dt.year + 1):
        s = datetime(year, 1, 1, tzinfo=timezone.utc)
        e = datetime(year, 12, 31, tzinfo=timezone.utc)
        s = max(s, start_dt)
        e = min(e, end_dt)
        params = {
            "latitude": lat,
            "longitude": lon,
            "hourly": ",".join([
                "temperature_2m",
                "relative_humidity_2m",
                "cloudcover",
                "pressure_msl",
                "precipitation",
                "wind_speed_10m",
            ]),
            "timezone": "UTC",
            "start_date": s.strftime("%Y-%m-%d"),
            "end_date": e.strftime("%Y-%m-%d"),
        }
        data = http_get(OPEN_METEO_ARCHIVE, params)
        hourly = data.get("hourly", {})
        times = hourly.get("time", [])
        for i, t in enumerate(times):
            dt_utc = datetime.fromisoformat(t).replace(tzinfo=timezone.utc)
            ts = int(dt_utc.timestamp())
            weather_map[ts] = {
                "Temperature": (hourly.get("temperature_2m") or [None])[i],
                "Relative Humidity": (hourly.get("relative_humidity_2m") or [None])[i],
                "Clouds": (hourly.get("cloudcover") or [None])[i],
                "Pressure": (hourly.get("pressure_msl") or [None])[i],
                "Precipitation": (hourly.get("precipitation") or [None])[i],
                "Wind Speed": (hourly.get("wind_speed_10m") or [None])[i],
            }
        time.sleep(0.2)
    return weather_map


def run_one_district(name, lat, lon, start_utc, end_utc):
    rows = []
    wx_map = get_weather_hourly_map(lat, lon, start_utc, end_utc)
    cur_day = start_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    end_day = end_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    for d in range((end_day - cur_day).days):
        day_utc = cur_day + timedelta(days=d)
        air = get_air_history(lat, lon, day_utc)
        for item in air.get("list", []):
            ts = int(item.get("dt", 0))
            utc_time = datetime.fromtimestamp(ts, tz=timezone.utc)
            local_time = utc_time.astimezone(TZ)
            comp = item.get("components", {})
            pm25 = comp.get("pm2_5")
            pm10 = comp.get("pm10")
            row = {
                "District": name,
                "Local Time": local_time.strftime("%Y-%m-%d %H:%M:%S"),
                "UTC Time": utc_time.isoformat(),
                "City": CITY,
                "Country Code": COUNTRY,
                "Timezone": TZ_NAME,
                "CO": (comp.get("co") / 1000.0) if comp.get("co") is not None else None,  # µg/m3 -> mg/m3
                "NO2": comp.get("no2"), "O3": comp.get("o3"), "PM10": pm10, "PM25": pm25, "SO2": comp.get("so2"),
            }
            # merge hourly weather by exact timestamp
            w = wx_map.get(ts, {})
            row.update({
                "Clouds": w.get("Clouds"),
                "Precipitation": w.get("Precipitation"),
                "Pressure": w.get("Pressure"),
                "Relative Humidity": w.get("Relative Humidity"),
                "Temperature": w.get("Temperature"),
                "Wind Speed": w.get("Wind Speed"),
            })
            rows.append(row)
        time.sleep(0.1)
    return pd.DataFrame(rows)

if __name__ == "__main__":

    start_utc = datetime(2022, 1, 1, 0, 0, tzinfo=timezone.utc)
    end_utc = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)

    outputs = []
    for dname, (lat, lon) in DISTRICTS.items():
        print(f"Fetching {dname}…")
        df = run_one_district(dname, lat, lon, start_utc, end_utc)
        df.to_csv(f"{dname.replace(' ', '_')}.csv", index=False, encoding="utf-8")
        outputs.append(df)

    final_df = pd.concat(outputs, ignore_index=True)
    final_df.to_csv("datacsv", index=False, encoding="utf-8")
    print("Done -> datacsv")
