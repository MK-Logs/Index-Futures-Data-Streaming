# Index Futures Data Streaming

An example producer-consumer Kafka System utilizing the publisher/subscriber messaging architecture. The producer entity streams data records as messages. The broker entity receives the messages from the producer and stores them. Then, the consumer entity reads messages from the broker and processes the data. The topic consists of the stream of data containing individual records. Each record contains a sequence id that determines its place in the stream. The producer appends data to the topic partition and the consumer subscribes to the changes. The Kafka server acts as the message broker and the partitions can be placed across multiple brokers. The dataset used for this system is the high frequency data of index futures. The data will be saved as a CSV file and will be used to create the stream simulation. We will be simulating a live stream by using the local data file. ML analytics can be carried out on the consmed data to determine trading decisions.

## Producer Log

## Consumer Log
