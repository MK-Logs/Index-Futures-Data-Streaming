import json
from kafka import KafkaConsumer
import psycopg2

if __name__ == '__main__':
    consumer = KafkaConsumer(
        'messages',
        bootstrap_servers='localhost:9092',
        #auto_offset_reset='earliest'
    )

    connectionToDB = psycopg2.connect(
        database='',
        user='',
        password='',
        host='',
        port=''
    )

    cursor = connectionToDB.cursor()

    for message in consumer:

        message_string = str(json.loads(message.value))
        streamID = str(json.loads(message.value)['Stream_ID'])
        lastPrice = str(json.loads(message.value)['Last_Price'])
        midPrice = str(json.loads(message.value)['Mid_Price'])
        transactedQTY = str(json.loads(message.value)['Transacted_QTY'])
        bid = str(json.loads(message.value)['Bid'])
        ask = str(json.loads(message.value)['Ask'])
        bidVolume = str(json.loads(message.value)['Bid_Volume'])
        askVolume = str(json.loads(message.value)['Ask_Volume'])

        queryStr = "INSERT INTO stocksdatastream(stream_id,last_price,mid_price,transacted_qty,bid,ask,bid_volume,ask_volume) " \
                   "VALUES('" + streamID + "','" + lastPrice + "','" + midPrice + "','" + \
                   transactedQTY + "','" + bid + "','" + ask + \
                   "','" + bidVolume + "','" + askVolume + "');"
        cursor.execute(queryStr)
        connectionToDB.commit()

        print(message_string)

    connectionToDB.close()
