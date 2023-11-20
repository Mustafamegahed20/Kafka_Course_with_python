from time import sleep
from confluent_kafka import Producer, KafkaError

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
        print('Offset: {}'.format(msg.offset()))
        print('Timestamp: {}'.format(msg.timestamp()))


def main():
    print("I am a Kafka Producer!")

    # Configure the Kafka producer
    producer_conf = {
        'bootstrap.servers': 'localhost:9092',
        'client.id': 'python-producer',
        'acks': 'all',
        # 'key.serializer': 'org.apache.kafka.common.serialization.StringSerializer',
        # 'value.serializer': 'org.apache.kafka.common.serialization.StringSerializer'
    }

    # Create producer instance
    producer = Producer(producer_conf)

    for j in range(10):
        
            # Produce a message
        producer.produce('demo_python', value=f'hello world {j}', callback=delivery_report)

        sleep(1)

    # Wait for any outstanding messages to be delivered
    producer.flush()

if __name__ == "__main__":
    main()
