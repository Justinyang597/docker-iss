import requests
import datetime
import logging
import os
import sys
from pymongo import MongoClient

"""
    This script fetches the current ISS location and writes it to MongoDB.
    Updates made:
      - HTTP status checking
      - Renamed `long`/`lat` to `longitude`/`latitude`
      - Improved error handling and exit codes
      - DB name updated to UVA computing ID: pxr6gr
      - Optional MONGO_DB_NAME env var support
"""

# Configuration
API_URL = "http://api.open-notify.org/iss-now.json"
DB_NAME = os.getenv("MONGO_DB_NAME", "pxr6gr")  # Set your UVA computing ID here or via env var

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def get_iss_location():
    """Fetch ISS location from the public API and return formatted data."""
    try:
        response = requests.get(API_URL, timeout=10)
        response.raise_for_status()
        data = response.json()

        # Extract and format
        timestamp = data.get("timestamp")
        dt_obj = datetime.datetime.fromtimestamp(timestamp)
        human_time = dt_obj.strftime("%Y-%m-%d %H:%M:%S")

        longitude = data["iss_position"]["longitude"]
        latitude = data["iss_position"]["latitude"]

        logger.info(f"Timestamp: {human_time}")
        logger.info(f"Longitude: {longitude}")
        logger.info(f"Latitude: {latitude}")

        return human_time, longitude, latitude

    except requests.exceptions.RequestException as e:
        logger.error(f"HTTP error when fetching ISS location: {e}")
        sys.exit(1)
    except (KeyError, ValueError) as e:
        logger.error(f"Error parsing response: {e}")
        sys.exit(1)


def write_to_mongo(timestamp, longitude, latitude):
    """Write the ISS data into MongoDB collection 'locations'."""
    db_pass = os.getenv("MONGOPASS")
    if not db_pass:
        logger.error("MONGOPASS environment variable is not set.")
        sys.exit(1)

    uri = (
        f"mongodb+srv://docker:{db_pass}@cluster0.m3fek.mongodb.net/"
        f"{DB_NAME}?retryWrites=true&w=majority"
    )

    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.server_info()  # trigger connection exception if cannot connect

        db = client[DB_NAME]
        collection = db["locations"]
        collection.insert_one({
            "fetched_at": timestamp,
            "longitude": longitude,
            "latitude": latitude,
        })
        logger.info("Output written to MongoDB")

    except Exception as e:
        logger.error(f"MongoDB write failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    ts, lon, lat = get_iss_location()
    write_to_mongo(ts, lon, lat)
