from confluent_kafka import Producer, KafkaError
from time import sleep

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Key: {} | Partition: {}'.format(msg.key(), msg.partition()))
        print('Offset: {}'.format(msg.offset()))
        print('Timestamp: {}'.format(msg.timestamp()))

def main():
    print("I am a Kafka Producer!")

    # Create producer configuration
    producer_conf = {
        'bootstrap.servers': 'localhost:9092',
        'acks': 'all',
    }

    # Create producer instance
    producer = Producer(producer_conf)

    for i in range(10):
        topic = 'demo_python'
        key = 'id_' + str(i+10)
        value = 'hello world ' + str(i)

        # Create a Producer Record
        producer.produce(topic, key=key, value=value, callback=delivery_report)
        # sleep(2)


    # Wait for any outstanding messages to be delivered
    producer.flush()

if __name__ == "__main__":
    main()
