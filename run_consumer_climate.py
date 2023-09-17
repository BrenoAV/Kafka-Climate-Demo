#!/usr/bin/env python

import json
from kafka import KafkaConsumer

THRESHOLD = 25  # 25 degrees


def main():
    """main function"""

    consumer = KafkaConsumer(
        "climate-events",
        auto_offset_reset="earliest",
        bootstrap_servers=["localhost:9092"],
    )
    for message in consumer:
        record = json.loads(message.value.decode("utf-8"))
        date = str(record["daily"]["time"][0])
        temp = int(record["daily"]["temperature_2m_max"][0])
        if temp > THRESHOLD:
            print(
                f"\U000026A0 Alert \U000026A0 - Today's temperature ({date}) reached {temp}°C which is above the threshold of {THRESHOLD}°C \U0001F525"
            )


if __name__ == "__main__":
    main()
