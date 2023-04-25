import urllib
from datetime import timedelta

import pandas as pd
import requests
from prefect import flow, get_run_logger, task, variables
from prefect.blocks.system import JSON
from prefect.tasks import task_input_hash

API = "https://api.open-meteo.com/v1/"
AIR_QUALITY_API = "https://air-quality-api.open-meteo.com/v1/"
LAT = variables.get("latitude")
LONG = variables.get("longitude")
team = JSON.load("team")


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(minutes=1), retries=3, retry_delay_seconds=60)
def get_temperature(logger, latitude=None, longitude=None):
    forecast_params = urllib.parse.urlencode(
        {"latitude": latitude, "longitude": longitude, "forecast_days": 1, "timezone": "GMT", "current_weather": True}
    )
    url = urllib.parse.urljoin(API, f"forecast?{forecast_params}")
    logger.info(url)
    res = requests.get(url)
    return res.json()["current_weather"]["temperature"]


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(minutes=1), retries=3, retry_delay_seconds=60)
def get_air_quality(logger, latitude=None, longitude=None):
    forecast_params = urllib.parse.urlencode(
        {"latitude": latitude, "longitude": longitude, "hourly": "us_aqi", "timezone": "GMT"}
    )
    url = urllib.parse.urljoin(AIR_QUALITY_API, f"air-quality?{forecast_params}")
    logger.info(url)
    res = requests.get(url)
    return res.json()["hourly"]["us_aqi"][0]


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(minutes=1), retries=3, retry_delay_seconds=60)
def get_elevation(logger, latitude=None, longitude=None):
    forecast_params = urllib.parse.urlencode({"latitude": latitude, "longitude": longitude})
    url = urllib.parse.urljoin(API, f"elevation?{forecast_params}")
    logger.info(url)
    res = requests.get(url)
    return res.json()["elevation"][0]


@flow()
def get_weather_data(logger, latitude, longitude):
    temperature = get_temperature(logger, latitude, longitude)
    air_quality = get_air_quality(logger, latitude, longitude)
    elevation = get_elevation(logger, latitude, longitude)
    logger.info(f"{temperature}\n{air_quality}\n{elevation}")
    return temperature, air_quality, elevation


@flow()
def superflow(latitude, longitude):
    logger = get_run_logger()
    temperature, air_quality, elevation = get_weather_data(logger, latitude, longitude)
    df = pd.DataFrame.from_dict({"elevation": [elevation], "temperature": [temperature], "air_quality": air_quality})
    logger.info(df.head())
    logger.info(team)
    return team


if __name__ == "__main__":
    df = superflow(latitude=None, longitude=None)
