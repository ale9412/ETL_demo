from kafka import KafkaProducer
from datetime import datetime
import cryptocompare as crypto
import time
import json
import random


producer = KafkaProducer(bootstrap_servers=["localhost:9092"])
startTime = time.time()
waitSeconds = 1.0
crypto_name = 'ADA'
KEY = "d9a06ce2007b97cb540345df3096e4ab47367b46b31c847d2d3ddb3b585b0efd"
crypto.cryptocompare._set_api_key_parameter(KEY)

while True:
    # Transaction time
    time_value = str(datetime.utcnow())

    # Cryptocurrency exchange
    exchange = crypto.get_price(crypto_name, currency=['EUR','USD'], full=False)
    EUR = exchange[crypto_name]['EUR']
    USD = exchange[crypto_name]['USD']

    # Format message
    msg = {"time": time_value, "crypto_name": crypto_name, "ADA/EUR": EUR, 'ADA/USD': USD}
    print("Sending JSON to Kafka", msg)
    producer.send("crypto_exchange", json.dumps(msg).encode())

    # Wait a number of second until next message
    time.sleep(waitSeconds - ((time.time() - startTime) % waitSeconds))