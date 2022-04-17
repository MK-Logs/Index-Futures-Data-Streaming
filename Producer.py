import time
import json
import random
from datetime import datetime
from kafka import KafkaProducer
import pandas as pd

liveDataSimulation = pd.read_csv('StockLiveDataSimulation.csv')

def serializer(message):
    return json.dumps(message).encode('utf-8')

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=serializer
)

def generateMessage():

    stockData = liveDataSimulation.sample()

    streamID = stockData['id'].to_string(index=False)
    lastPrice = stockData['last_price'].to_string(index=False)
    midPrice = stockData['mid'].to_string(index=False)
    transactedQTY = stockData['transacted_qty'].to_string(index=False)
    bid = stockData['bid1'].to_string(index=False)
    ask = stockData['ask1'].to_string(index=False)
    bidVolume = stockData['bid1vol'].to_string(index=False)
    askVolume = stockData['ask1vol'].to_string(index=False)

    return {
        'Stream_ID': streamID,
        'Last_Price': lastPrice,
        'Mid_Price': midPrice,
        'Transacted_QTY': transactedQTY,
        'Bid': bid,
        'Ask': ask,
        'Bid_Volume': bidVolume,
        'Ask_Volume': askVolume,
    }

if __name__ == '__main__':

    while True:
        stock_message = generateMessage()

        print(f'{datetime.now()} | Stream = {str(stock_message)}')
        producer.send('messages', stock_message)

        time_to_sleep = random.randint(1, 3)
        time.sleep(time_to_sleep)
