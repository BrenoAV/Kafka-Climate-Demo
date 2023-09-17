import time
from datetime import date
import requests
from producer_climates import publish_message, connect_kafka_producer

kafka_producer = connect_kafka_producer()

API_URL = "https://climate-api.open-meteo.com/v1/climate"


def main():
    """main function"""
    today_date = date.today()
    var_month = date(year=today_date.year, month=today_date.month, day=1)
    for i in range(1, today_date.day + 1):
        var_month = var_month.replace(day=i)
        query = {
            "latitude": -23.5,
            "longitude": -46.59999,
            "start_date": var_month.strftime("%Y-%m-%d"),
            "end_date": var_month.strftime("%Y-%m-%d"),
            "models": "EC_Earth3P_HR",
            "daily": "temperature_2m_max",
        }
        response = requests.get(url=API_URL, params=query, timeout=20)
        key = "climate_raw"
        value = response.json()
        if kafka_producer:
            publish_message(
                producer_instance=kafka_producer,
                topic_name="climate-events",
                key=key,
                value=value,
            )
            time.sleep(1)  # Wait for 4 seconds mocking passing one day


if __name__ == "__main__":
    main()
