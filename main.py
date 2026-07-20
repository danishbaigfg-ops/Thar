import asyncio
import json
from datetime import datetime
from zoneinfo import ZoneInfo

import httpx
from fastapi import FastAPI, HTTPException, Request
from workers import WorkerEntrypoint


# Coordinates: 24°47'31.7"N 70°23'26.2"E
LATITUDE = 24.7921389
LONGITUDE = 70.3906111

# Local timezone
APP_TIMEZONE = ZoneInfo("Asia/Karachi")

WEATHER_CACHE_KEY = "weather_cache"

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


async def fetch_openmeteo_weather(client: httpx.AsyncClient) -> dict:
    """
    This is the only function that calls Open-Meteo.
    """

    url = "https://api.open-meteo.com/v1/forecast"

    params = {
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "hourly": ",".join([
            "temperature_2m",
            "weather_code",
            "relative_humidity_2m",
            "pressure_msl",
            "wind_speed_10m",
            "precipitation",
            "wind_gusts_10m",
        ]),
        "models": "best_match",
        "timezone": "auto",
    }

    response = None
    last_error = None
    for attempt in range(5):
        try:
            response = await client.get(url, params=params, timeout=15)
            response.raise_for_status()
            break
        except httpx.HTTPError as exc:
            last_error = exc
            await asyncio.sleep(0.2 * (2 ** attempt))
    if response is None:
        raise last_error

    payload = response.json()
    hourly = payload["hourly"]
    times = hourly["time"]

    records = []
    for i, time_str in enumerate(times):
        weather_code = int(hourly["weather_code"][i])
        records.append({
            "date": time_str,
            "temperature_2m": hourly["temperature_2m"][i],
            "weather_code": weather_code,
            "relative_humidity_2m": hourly["relative_humidity_2m"][i],
            "pressure_msl": hourly["pressure_msl"][i],
            "wind_speed_10m": hourly["wind_speed_10m"][i],
            "precipitation": hourly["precipitation"][i],
            "wind_gusts_10m": hourly["wind_gusts_10m"][i],
            "rain_code": weather_code if weather_code in RAIN_RELATED_WEATHER_CODES else 0,
        })

    return {
        "location": {
            "latitude": payload["latitude"],
            "longitude": payload["longitude"],
            "elevation": payload["elevation"],
            "timezone": payload["timezone"],
            "timezone_abbreviation": payload["timezone_abbreviation"],
            "utc_offset_seconds": payload["utc_offset_seconds"],
        },
        "hourly": records,
    }


async def refresh_weather_cache(env) -> dict:
    async with httpx.AsyncClient() as client:
        data = await fetch_openmeteo_weather(client)

    cache_payload = {
        "last_updated": datetime.now(APP_TIMEZONE).isoformat(),
        "data": data,
    }

    await env.WEATHER_KV.put(WEATHER_CACHE_KEY, json.dumps(cache_payload))

    return cache_payload


async def get_cache(env) -> dict | None:
    raw = await env.WEATHER_KV.get(WEATHER_CACHE_KEY)
    if raw is None:
        return None
    return json.loads(raw)


app = FastAPI(
    title="Open-Meteo Weather API",
    description="FastAPI wrapper for Open-Meteo forecast data",
    version="1.0.0",
)


@app.get("/")
async def root(request: Request):
    env = request.scope["env"]
    cache = await get_cache(env)

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
        "last_updated": cache["last_updated"] if cache else None
    }


@app.get("/weather/hourly")
async def get_hourly_weather(request: Request):
    """
    This endpoint does not call Open-Meteo.
    It only returns cached data.
    """

    env = request.scope["env"]
    cache = await get_cache(env)

    if cache is None:
        raise HTTPException(
            status_code=503,
            detail="Weather data is not available yet."
        )

    return {
        "last_updated": cache["last_updated"],
        "source": "cached",
        "data": cache["data"]
    }


@app.get("/weather/status")
async def weather_status(request: Request):
    env = request.scope["env"]
    cache = await get_cache(env)

    return {
        "last_updated": cache["last_updated"] if cache else None,
        "has_cached_data": cache is not None,
        "openmeteo_hits_per_day": 2,
        "first_time_fetch": "manual_via_admin_refresh",
        "scheduled_times": ["12:30 AM", "12:30 PM"],
        "timezone": "Asia/Karachi"
    }


@app.post("/admin/refresh")
async def trigger_refresh(request: Request, token: str = ""):
    """
    Manually bootstraps/refreshes the cache. Workers have no startup hook,
    so this must be called once after the first deploy (and can be used
    to recover if a scheduled run was missed).
    Requires the ADMIN_TOKEN secret to be set via `wrangler secret put ADMIN_TOKEN`.
    """

    env = request.scope["env"]
    expected_token = getattr(env, "ADMIN_TOKEN", None)

    if not expected_token or token != expected_token:
        raise HTTPException(status_code=403, detail="Forbidden")

    cache_payload = await refresh_weather_cache(env)

    return {"status": "ok", "last_updated": cache_payload["last_updated"]}


class Default(WorkerEntrypoint):
    async def fetch(self, request):
        import asgi
        return await asgi.fetch(app, request.js_object, self.env)

    async def scheduled(self, controller):
        """
        Runs based on the crons configured in wrangler.toml:
        - 19:30 UTC (12:30 AM Asia/Karachi)
        - 07:30 UTC (12:30 PM Asia/Karachi)
        """

        try:
            cache_payload = await refresh_weather_cache(self.env)
            print(f"Weather cache updated at {cache_payload['last_updated']}")
        except Exception as exc:
            print(f"Weather cache update failed: {exc}")
