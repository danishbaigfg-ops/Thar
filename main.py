from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from threading import Lock
from zoneinfo import ZoneInfo
import json

from fastapi import FastAPI, HTTPException
import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry
from apscheduler.schedulers.background import BackgroundScheduler


# Coordinates: 24°47'31.7"N 70°23'26.2"E
LATITUDE = 24.7921389
LONGITUDE = 70.3906111

# Local timezone
APP_TIMEZONE = ZoneInfo("Asia/Karachi")

CACHE_FILE = Path("weather_cache.json")
cache_lock = Lock()

weather_cache = {
    "last_updated": None,
    "data": None
}


RAIN_RELATED_WEATHER_CODES = {
    51, 53, 55,      # Drizzle
    56, 57,          # Freezing drizzle
    61, 63, 65,      # Rain
    66, 67,          # Freezing rain
    71, 73, 75,      # Snow fall
    77,              # Snow grains
    80, 81, 82,      # Rain showers
    85, 86,          # Snow showers
    95,              # Thunderstorm
    96, 99           # Thunderstorm with hail
}


# Setup Open-Meteo API client
cache_session = requests_cache.CachedSession(".cache", expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)


def save_cache_to_file(data: dict):
    with CACHE_FILE.open("w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=2)


def load_cache_from_file():
    if not CACHE_FILE.exists():
        return

    with CACHE_FILE.open("r", encoding="utf-8") as file:
        saved_data = json.load(file)

    with cache_lock:
        weather_cache["last_updated"] = saved_data.get("last_updated")
        weather_cache["data"] = saved_data.get("data")


def fetch_openmeteo_weather() -> dict:
    """
    This is the only function that calls Open-Meteo.
    """

    url = "https://api.open-meteo.com/v1/forecast"

    params = {
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "hourly": [
            "temperature_2m",
            "weather_code",
            "relative_humidity_2m",
            "pressure_msl",
            "wind_speed_10m",
            "precipitation",
            "wind_gusts_10m"
        ],
        "models": "best_match",
        "timezone": "auto",
    }

    responses = openmeteo.weather_api(url, params=params)
    response = responses[0]

    hourly = response.Hourly()

    hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
    hourly_weather_code = hourly.Variables(1).ValuesAsNumpy()
    hourly_relative_humidity_2m = hourly.Variables(2).ValuesAsNumpy()
    hourly_pressure_msl = hourly.Variables(3).ValuesAsNumpy()
    hourly_wind_speed_10m = hourly.Variables(4).ValuesAsNumpy()
    hourly_precipitation = hourly.Variables(5).ValuesAsNumpy()
    hourly_wind_gusts_10m = hourly.Variables(6).ValuesAsNumpy()

    timezone = response.Timezone().decode()
    timezone_abbreviation = response.TimezoneAbbreviation().decode()

    hourly_data = {
        "date": pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left"
        ).tz_convert(timezone),

        "temperature_2m": hourly_temperature_2m,
        "weather_code": hourly_weather_code,
        "relative_humidity_2m": hourly_relative_humidity_2m,
        "pressure_msl": hourly_pressure_msl,
        "wind_speed_10m": hourly_wind_speed_10m,
        "precipitation": hourly_precipitation,
        "wind_gusts_10m": hourly_wind_gusts_10m
    }

    hourly_dataframe = pd.DataFrame(data=hourly_data)

    hourly_dataframe["date"] = hourly_dataframe["date"].astype(str)
    hourly_dataframe["weather_code"] = hourly_dataframe["weather_code"].astype(int)

    hourly_dataframe["rain_code"] = hourly_dataframe["weather_code"].apply(
        lambda code: int(code) if int(code) in RAIN_RELATED_WEATHER_CODES else 0
    )

    return {
        "location": {
            "latitude": response.Latitude(),
            "longitude": response.Longitude(),
            "elevation": response.Elevation(),
            "timezone": timezone,
            "timezone_abbreviation": timezone_abbreviation,
            "utc_offset_seconds": response.UtcOffsetSeconds()
        },
        "hourly": hourly_dataframe.to_dict(orient="records")
    }


def refresh_weather_cache():
    """
    Updates weather cache by calling Open-Meteo.
    This runs:
    - First time if no cache exists
    - Daily at 12:30 AM
    - Daily at 12:30 PM
    """

    try:
        data = fetch_openmeteo_weather()

        updated_cache = {
            "last_updated": datetime.now(APP_TIMEZONE).isoformat(),
            "data": data
        }

        with cache_lock:
            weather_cache["last_updated"] = updated_cache["last_updated"]
            weather_cache["data"] = updated_cache["data"]

        save_cache_to_file(updated_cache)

        print(f"Weather cache updated at {updated_cache['last_updated']}")

    except Exception as e:
        print(f"Weather cache update failed: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Load saved cache when app starts
    load_cache_from_file()

    # First-time fetch only if no cached data exists
    with cache_lock:
        has_cached_data = weather_cache["data"] is not None

    if not has_cached_data:
        print("No weather cache found. Fetching first-time weather data...")
        refresh_weather_cache()
    else:
        print("Weather cache found. Skipping first-time Open-Meteo fetch.")

    scheduler = BackgroundScheduler(timezone=APP_TIMEZONE)

    # Run every day at 12:30 AM
    scheduler.add_job(
        refresh_weather_cache,
        trigger="cron",
        hour=0,
        minute=30,
        id="weather_update_0030",
        replace_existing=True,
        max_instances=1,
        coalesce=True
    )

    # Run every day at 12:30 PM
    scheduler.add_job(
        refresh_weather_cache,
        trigger="cron",
        hour=12,
        minute=30,
        id="weather_update_1230",
        replace_existing=True,
        max_instances=1,
        coalesce=True
    )

    scheduler.start()

    yield

    scheduler.shutdown(wait=False)


app = FastAPI(
    title="Open-Meteo Weather API",
    description="FastAPI wrapper for Open-Meteo forecast data",
    version="1.0.0",
    lifespan=lifespan
)


@app.get("/")
def root():
    return {
        "message": "Open-Meteo FastAPI service is running",
        "coordinates": {
            "latitude": LATITUDE,
            "longitude": LONGITUDE
        },
        "update_schedule": {
            "timezone": "Asia/Karachi",
            "times": ["12:30 AM", "12:30 PM"]
        },
        "last_updated": weather_cache["last_updated"]
    }


@app.get("/weather/hourly")
def get_hourly_weather():
    """
    This endpoint does not call Open-Meteo.
    It only returns cached data.
    """

    with cache_lock:
        cached_data = weather_cache["data"]
        last_updated = weather_cache["last_updated"]

    if cached_data is None:
        raise HTTPException(
            status_code=503,
            detail="Weather data is not available yet."
        )

    return {
        "last_updated": last_updated,
        "source": "cached",
        "data": cached_data
    }


@app.get("/weather/status")
def weather_status():
    return {
        "last_updated": weather_cache["last_updated"],
        "has_cached_data": weather_cache["data"] is not None,
        "openmeteo_hits_per_day": 2,
        "first_time_fetch": "enabled_if_no_cache_exists",
        "scheduled_times": ["12:30 AM", "12:30 PM"],
        "timezone": "Asia/Karachi"
    }