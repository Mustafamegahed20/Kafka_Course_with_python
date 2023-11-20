from confluent_kafka import Consumer, KafkaError

def main():
    print("I am a Kafka Consumer!")

    # Consumer configuration
    consumer_conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'my-second-application',
        'auto.offset.reset': 'earliest', # read from the begin of topic /Latest #read from Now
    }

    # Create Kafka consumer
    consumer = Consumer(consumer_conf)

    # Subscribe to the topic
    topic = 'demo_python'  # Adjust to the topic you want to subscribe to
    consumer.subscribe([topic])

    # Poll for data
    try:
        while True:
            print("Polling")
            msg = consumer.poll(timeout=1000) #if you don't get any record from kafka in a range of 1000 milisec stop consumer

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break

            print('Key: {}, Value: {}, Partition: {}, Offset: {}'.format(msg.key(), msg.value(), msg.partition(), msg.offset()))

    except KeyboardInterrupt:
        pass
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

if __name__ == "__main__":
    main()
