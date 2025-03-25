TAXI_ZONES_FILE_PATH = "data/raw/taxi_zones.csv"
TAXI_TRIPS_TEMPLATE_FILE_PATH = "data/raw/taxi_trips_{}.parquet"

TRIPS_BY_AIRPORT_FILE_PATH = "data/outputs/trips_by_airport.csv"
TRIPS_BY_WEEK_FILE_PATH = "data/outputs/trips_by_week.csv"
MANHATTAN_STATS_FILE_PATH = "data/staging/manhattan_stats.geojson"
MANHATTAN_MAP_FILE_PATH = "data/outputs/manhattan_map.png"

REQUEST_DESTINATION_TEMPLATE_FILE_PATH = "data/outputs/{}.png"

DATE_FORMAT = "%Y-%m-%d"

START_DATE = "2023-01-01"
END_DATE = "2023-04-01"

import requests
import dagster as dg

@dg.asset
def taxi_trips_file() -> None:
    """
      The raw parquet files for the taxi trips dataset. Sourced from the NYC Open Data portal.
    """
    month_to_fetch = '2023-03'
    raw_trips = requests.get(
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month_to_fetch}.parquet"
    )

    with open(constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch), "wb") as output_file:
        output_file.write(raw_trips.content)
